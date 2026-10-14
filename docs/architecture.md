# Архитектура PayRelay v1

## 1. Основание и границы

Исходное ТЗ: асинхронный платёжный сервис, API POST/GET, очередь `payments.new`,
один consumer, Outbox, 3 попытки с exponential backoff, DLQ, webhook, API key.
Цель — продемонстрировать надёжную **at-least-once** доставку и идемпотентную обработку,
не обещая недостижимый exactly-once между PostgreSQL, RabbitMQ и HTTP.

Проектные расширения (не обязательные пункты текста ТЗ): отдельная роль
`outbox-publisher`, независимый webhook delivery status, `request_hash`, lease в Outbox,
изолированный test broker, SSRF защита webhook.

## 2. Контейнеры

```text
Client --HTTP--> api (FastAPI) --- PostgreSQL (payments + outbox)
                                 ^                     |
                                 |                     | fetch/claim pending
                            consumer             outbox-publisher
                        (FastStream + HTTPX)             |
                                 ^                       v
                                 +----- RabbitMQ <---------+
                                         payments.new
                                         payments.dlq
                                 |
                                 +-----> merchant webhook URL
```

Compose: `postgres`, `rabbitmq`, `api`, `consumer`, `outbox-publisher`,
`docker-socket-proxy`, `dozzle` (единый `infra/compose.yaml`).
Тестовый profile `test`: **отдельные** `postgres-test`, `rabbitmq-test` и
`tests` (test-runner с доступом по Docker-сети) с отдельными секретами и volume.
RabbitMQ Management UI (образ `*-management`) публикуется только на
`127.0.0.1:${RABBITMQ_MANAGEMENT_PORT:-15672}` — локальная разработка, не prod.
Dozzle + docker-socket-proxy (read-only) для логов `api` / `consumer` /
`outbox-publisher` без монтирования `docker.sock` в UI и без админ-действий;
UI на `127.0.0.1:${DOZZLE_PORT:-9999}`.

## 3. Модули и запуск

```text
src/payrelay/
  api/{app.py,routers/payments.py,dependencies.py,schemas.py,
       exception_handlers.py,errors.py,messages.py,constants.py}
  application/{payments.py,process_payment.py,outbox_dispatcher.py}
  domain/{enums.py,contracts.py,errors.py}
  infrastructure/{db/{models.py,session.py,repositories.py},
                  messaging/{broker.py,topology.py,consumer.py,publisher.py},
                  gateway/simulator.py,webhooks/http_client.py}
  core/{config.py,logging.py}
  entrypoints/{api.py,consumer.py,outbox_publisher.py}
alembic/{env.py,versions/}
tests/{unit,integration,e2e,conftest.py}
infra/compose.yaml
```

Process-level E2E (`tests/e2e`): поднимает реальные entrypoints subprocess'ами внутри
Compose service `tests` и локальный HTTP webhook receiver; не добавляет постоянный
production-сервис.

Важнее отсутствие циклических импортов и понятные границы, чем формальное число слоёв.
**Один handler** в consumer оркестрирует и бизнес-операцию, и доставку webhook.

## 4. Таблицы

### payments

- `id UUID PK`.
- `amount NUMERIC(18,2) NOT NULL`, `currency`, `description`, `metadata JSONB`.
- `status` (`pending`, `succeeded`, `failed`).
- `idempotency_key VARCHAR UNIQUE NOT NULL`; `request_hash VARCHAR NOT NULL`.
- `webhook_url TEXT NOT NULL`; `webhook_delivery_status` (`pending`, `delivered`, `exhausted`).
- `webhook_attempts SMALLINT NOT NULL DEFAULT 0`, `webhook_delivered_at TIMESTAMPTZ NULL`.
- `processing_lease_until TIMESTAMPTZ NULL` для конкурентной обработки и восстановления после падения.
- `created_at TIMESTAMPTZ`, `processed_at TIMESTAMPTZ`.

### outbox

- `id UUID PK` (`event_id`), `aggregate_id UUID`, `event_type VARCHAR`, `payload JSONB`.
- `available_at TIMESTAMPTZ`, `published_at TIMESTAMPTZ NULL`.
- `claimed_until TIMESTAMPTZ NULL`, `attempts INTEGER DEFAULT 0`, `created_at TIMESTAMPTZ`.
- Индекс `(available_at, created_at) WHERE published_at IS NULL`;
  индекс `claimed_until` в зависимости от query plan.
- Для retry — уникальная пара `(aggregate_id, event_type, attempt)` или эквивалентный
  детерминированный ключ в БД, чтобы дубль события не планировал retry повторно.
  Конкретную форму хранить в схеме (например, отдельная колонка `delivery_attempt`).

Запрошены ровно две таблицы: `payments`, `outbox`; новых таблиц по умолчанию не вводим.

## 5. Путь создания платежа

1. Проверить X-API-Key, Idempotency-Key, payload.
2. Вычислить канонический хэш полей запроса (включая `metadata`, webhook URL).
3. В одной транзакции INSERT `payments` + INSERT `outbox` (`payment.created`, attempt=1).
4. Конкурирующий INSERT с тем же key разрешить через уникальный constraint:
   взять уже созданную запись и сравнить хэш.
5. Вернуть 202; API никогда не ждёт шлюз/вебхук/брокер.
6. Publisher периодически берёт Outbox, публикует persistent `payments.new`
   с broker confirm и **только затем** отмечает published_at.

## 6. Обработка и идемпотентность

- Данные сообщения: `event_id`, `payment_id`, `attempt`, `event_type`, `schema_version`.
  `event_id` — идентификатор конкретного сообщения; отдельный `webhook_event_id`
  стабилен для уведомления одного платежа на всех попытках.
- Consumer читает БД и не повторяет шлюз для терминального платежа.
- Перед эмуляцией атомарно захватывает `processing_lease_until`; пока lease активен,
  конкурентный consumer не делает второй вызов. После аварии истёкший lease позволяет
  восстановление. Для настоящего шлюза обязательно нужна downstream идемпотентность.
- Эмулятор 2–5с, 90/10; для восстановления после падения результат должен быть
  повторяемым для данного `payment_id` (или должен использоваться gateway idempotency key).
  Предпочтение: детерминированное outcome по hash(UUID), не нестабильная `random()`.
- Терминальный статус платежа фиксируется в БД **до** HTTP webhook.
- Если webhook уже доставлен — ack без второй отправки.
- Webhook с одинаковым `event_id` может прийти повторно при краше между HTTP 2xx и
  коммитом `delivered`: клиент должен использовать X-Webhook-Event-ID для дедупликации.
- Первые две неудачи webhook: обновить `webhook_attempts` и атомарно создать новую
  Outbox запись `payment.webhook_retry` с `available_at = now + 2s/4s`.
  Затем ack. Таким образом backoff не блокирует consumer и retry не теряется.
- Третья неудача: сохранить exhausted и выполнить broker `reject/nack(requeue=False)`;
  RabbitMQ routing через DLX в `payments.dlq`.
- Для инфраструктурных ошибок (БД недоступна, обработчик падает до коммита) использовать
  **отдельный** счётчик `system_attempt` в заголовке RabbitMQ (начиная с 1),
  не смешивая его с `webhook_attempt`. Если 1/2-я системная попытка неудачна,
  publisher-confirmed republish в `payments.retry.2` / `payments.retry.3`
  с увеличенным заголовком и только затем ack исходного сообщения.
  Третья — `nack(requeue=False)` через DLX в `payments.dlq`.
  Если публикация retry не подтверждена, **не ack**, сообщение должно вернуться
  после восстановления соединения. Защититься от tight loop, мониторить DLQ.
  Дубли из crash window (publish retry подтверждён, но ack оригинала потерян)
  допустимы и нейтрализуются идемпотентной обработкой.

## 7. RabbitMQ topology

- Durable direct exchange `payments.exchange`, routing key `payments.new`.
- Durable `payments.new`, с `x-dead-letter-exchange: payments.dlx` и routing key `payments.dlq`.
- Durable `payments.retry.2` с TTL 2s и `payments.retry.3` с TTL 4s;
  обе после истечения TTL через DLX возвращают сообщение в `payments.new`.
- Durable exchange `payments.dlx`, durable queue `payments.dlq`.
- Счётчики retry независимы: `webhook_attempt` в payload и `system_attempt`
  в RabbitMQ header; новое outbox webhook retry начинает system_attempt с 1.
- Persistent messages, publisher confirms, ack после сохранения результата.
- `prefetch_count` ограничить (например 10) из-за 2–5 сек симуляции.
- Параметры declare/ack и publisher confirms проверить по текущему FastStream API.

## 8. Безопасность

- `X-API-Key` сверять constant-time; секреты через env, не логировать.
- HTTPX timeout/limit concurrency, redirects выключены.
- В production запрещены loopback, RFC1918, link-local, cloud metadata адреса;
  DNS precheck **не равен** полной защите от DNS rebinding — для production нужен
  network-level egress / pinning назначения.
- В test/E2E profile можно разрешить private URL (`WEBHOOK_ALLOW_PRIVATE_URLS=true`)
  для localhost webhook receiver; production policy этим не ослабляется.
- Ошибки и логи не должны содержать секреты или чувствительные metadata целиком.

## 9. Наблюдаемость и критерии

- JSON structured logs с `payment_id`, `event_id`, `attempt` и duration.
- Health/readiness API защищены `X-API-Key`, как и прочие эндпоинты; Docker healthcheck
  передаёт ключ из окружения без печати в логах; Compose healthchecks и graceful shutdown.
- Swagger `/docs` только в dev-режиме и под согласованным механизмом защиты,
  в production — отключить, чтобы `X-API-Key` требовался на всех публичных маршрутах.
- README с curl, тестовыми командами и логами.
- Оцениваем: Outbox crash windows, уникальная идемпотентность под конкуренцией,
  3 retries + DLQ, интеграционные тесты на реальных инфраструктурных компонентах.

## 10. Отложенные задачи (не для тестового задания)

Нет необходимости в Kubernetes, Redis, Kafka, Prometheus, отдельном webhook-сервисе,
сложной DDD-библиотеке, оркестраторе Saga, real payment provider и панели администратора.
