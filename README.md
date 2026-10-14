# PayRelay

Асинхронный сервис обработки платежей (тестовое задание).

Один репозиторий, три исполняемые роли одного Python-пакета:

```text
Client --HTTP--> api --- PostgreSQL (payments + outbox)
                           ^                     |
                      consumer             outbox-publisher
                  (шлюз + webhook)               |
                           ^                     v
                           +----- RabbitMQ <-----+
                                   payments.new / retry / dlq
                                   |
                                   +-----> merchant webhook
```

Гарантия доставки: **at-least-once** + идемпотентные обработчики (не exactly-once).

## Требования

- Python 3.12, [uv](https://docs.astral.sh/uv/), Docker Compose.
- FastAPI, Pydantic v2; SQLAlchemy 2.0 Async, PostgreSQL, Alembic.
- RabbitMQ + FastStream; HTTPX для webhook; `Idempotency-Key`.

## Документация

- [Архитектура](docs/architecture.md)
- [API контракт](docs/api-contract.md)
- [Матрица тестирования](docs/test-matrix.md)

## Быстрый старт (чистый clone)

```bash
cp .env.example .env   # подставьте свои значения; файл `.env` не коммитится
docker compose -f infra/compose.yaml up -d --build
docker compose -f infra/compose.yaml run --rm api uv run alembic upgrade head
```

Alembic **не** выполняется приложением при старте — схему готовит отдельная команда выше.

Роли Compose:

- `api` — `POST`/`GET` платежей и `/health/live` (не ждёт RabbitMQ на каждый запрос).
- `outbox-publisher` — claim Outbox → publish confirm → `published_at`.
- `consumer` — эмуляция шлюза (≈90/10, задержка 2–5 с), terminal status, webhook HTTP.
- Webhook: до **3** логических попыток; после HTTP-сбоя #1/#2 — Outbox
  `payment.webhook_retry` с `available_at = now()+2s/4s` (без sleep в handler).
  После 3-й неудачи — `webhook_delivery_status=exhausted` и `reject` → `payments.dlq`.
- Инфраструктурные сбои: заголовок `system_attempt` и TTL-очереди
  `payments.retry.2` / `payments.retry.3` (confirm-before-ACK); на 3-й — DLQ.

### Health и примеры API

```bash
curl -fsS -H "X-API-Key: $API_KEY" http://127.0.0.1:8000/health/live
```

Создание платежа (ответ **202**; `amount` в ответе — **строка**):

```bash
curl -fsS -X POST http://127.0.0.1:8000/api/v1/payments \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -H "Idempotency-Key: demo-1" \
  -d '{
    "amount": "1500.50",
    "currency": "RUB",
    "description": "Заказ #123",
    "metadata": {"order_id": "123"},
    "webhook_url": "https://merchant.example.com/payments/callback"
  }'
```

Получение платежа (**200**):

```bash
curl -fsS -H "X-API-Key: $API_KEY" \
  http://127.0.0.1:8000/api/v1/payments/<payment_id>
```

Типичные ошибки: **401** (нет/неверный `X-API-Key`), **404** (неизвестный UUID),
**409** (тот же `Idempotency-Key`, другое тело), **422** (валидация / нет ключа идемпотентности).

Swagger `/docs` доступен только при `ENABLE_DOCS=true` и `APP_ENV!=production`
(по умолчанию в `.env.example` включён для локальной разработки). Все публичные
маршруты, включая health, требуют `X-API-Key`.

## Поведение симулятора и webhook

- Шлюз: детерминированный outcome по `sha256(payment_id)` (~90% `succeeded` / ~10% `failed`),
  задержка 2–5 с. Business `failed` — итог бизнеса, **не** причина DLQ.
- Webhook: до 3 логических HTTP-попыток; backoff Outbox +2 с / +4 с.
- Заголовок `X-Webhook-Event-ID` стабилен на все попытки одного платежа — клиент
  должен дедуплицировать.
- Физических HTTP может быть больше трёх (crash между 2xx и commit `delivered`).
- `system_attempt` — не глобальный durable counter; legacy recovery может начать
  retry сразу. DLQ — диагностика, не автоматический business replay.

## RabbitMQ (локальный просмотр)

Management UI (только dev-брокер): http://127.0.0.1:15672  
Учётка из `.env` (`RABBITMQ_DEFAULT_USER` / `RABBITMQ_DEFAULT_PASS`) — не публикуйте
реальные секреты.

Topology после старта `outbox-publisher`:

- exchange `payments.exchange` (durable direct)
- queue `payments.new` (+ DLX `payments.dlx` / `payments.dlq`)
- retry queues `payments.retry.2` / `payments.retry.3` (TTL 2s/4s)
- DLQ `payments.dlq`

Безопасный просмотр: Queues → Get messages (без global purge). В DLQ ожидайте
`x-death` с `reason=rejected` (или `expired` для TTL-маршрутов).

## Проверки качества

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest -m unit
```

### Integration / E2E (отдельные postgres-test / rabbitmq-test)

Тесты работают **только** с `payrelay_test` / `rabbitmq-test` (фикстуры отказывают
в обычных DSN). Profile `test` не публикует порты наружу и не пишет в основную БД.

```bash
docker compose -f infra/compose.yaml --profile test config

docker compose -f infra/compose.yaml --profile test run --rm tests \
  uv run pytest -m integration

# Process-level E2E: реальные entrypoints api/publisher/consumer + localhost webhook receiver
docker compose -f infra/compose.yaml --profile test run --rm tests \
  uv run pytest -m e2e
```

Миграции на **test** DB:

```bash
docker compose -f infra/compose.yaml --profile test run --rm tests \
  uv run alembic upgrade head
```

Очистка только тестовых volume (не трогает основную БД):

```bash
docker compose -f infra/compose.yaml --profile test down
# удаление test volumes — только осознанно:
# docker volume rm payrelay_postgres_test_data payrelay_rabbitmq_test_data
```

## Ограничения безопасности (честно)

- Production: `WEBHOOK_ALLOW_PRIVATE_URLS=false`; запрет loopback/RFC1918/link-local/metadata,
  DNS precheck, запрет redirect, timeout. **DNS precheck ≠ полная защита от DNS rebinding**;
  для production нужен network-level egress / pinning назначения.
- В test profile private URL (localhost receiver) разрешены явно флагом окружения.
- Секреты только в env; `.env` / `.ai` / `.cursor` / `.serena` / `.repomix` / `AGENTS.md`
  / `SKILLS.md` не отслеживаются Git.
