# Матрица проверки PayRelay

## Unit (без Docker)

| № | Сценарий | Ожидается |
|---|---|---|
| U01 | amount 0, отрицательная, NaN, >2 знаков | 422 |
| U02 | currency не из RUB/USD/EUR | 422 |
| U03 | Отсутствующий Idempotency-Key | 422 |
| U04 | Некорректная схема JSON / URL webhook | 422 |
| U05 | Повтор POST с тем же key/body | тот же payment_id |
| U06 | Повтор key с отличающимся body | 409 |
| U07 | Неверный / отсутствующий X-API-Key | 401 |
| U08 | GET UUID не найден | 404 |
| U09 | Успех/неуспех эмулятора при управляемом outcome | 90/10 по заданным фикстурам |
| U10 | Транзит pending -> succeeded/failed; запрет обратного | корректно |
| U11 | Терминальный payment + повторный event | шлюз не вызывается заново |
| U12 | Webhook success 2xx | delivered |
| U13 | Webhook timeout / 5xx | Outbox retry +2s/+4s; #3 → exhausted без retry4 |
| U14 | Дубликат AMQP после commit retry / после успеха | нет нового Outbox/HTTP; gateway не вызывается |
| U15 | system_attempt 1→2→3; poison payload | TTL retry confirm-before-ACK; #3/poison → DLQ |
| U16 | Запрещённый private/loopback webhook URL | отклонён production policy |
| U17 | Публичные 401/404/409/422/500 — русские detail | коды HTTP и формат detail сохранены |
| U18 | OpenAPI: security X-API-Key, Idempotency-Key, русские примеры | schema содержит описания |
| U19 | Нет module-level docstring в src/tests/alembic | AST-проверка; function/class docstring есть |

## Integration (реальные test PostgreSQL и RabbitMQ)

| № | Сценарий | Ожидается |
|---|---|---|
| I01 | Alembic upgrade head на чистой тестовой БД | таблицы payments/outbox |
| I02 | Успешный POST | две записи атомарно, ответ 202 |
| I03 | Конкурентно 20 одинаковых POST | одна payment и один исходный outbox |
| I04 | Broker остановлен на момент POST | 202, запись Outbox не теряется |
| I05 | Broker восстановлен | publisher доставляет событие, затем published_at |
| I06 | Повторная публикация того же event | нет второй бизнес-операции |
| I07 | End-to-end успешный платёж и webhook | succeeded + delivered |
| I08 | Эмулированный business failure | failed + уведомление, без ненужного retry |
| I09 | Webhook всегда 500 | 3 HTTP, backoff 2/4s, exhausted, reject → payments.dlq (`x-death`) |
| I10 | 500×2→200; 500→200; failed+webhook OK | delivered; DLQ пуст для успешного ID |
| I11 | Crash после broker confirm, до mark-published | безопасная повторная публикация |
| I12 | Consumer crash после commit payment, до webhook | payment не обрабатывается повторно |
| I13 | Crash после webhook 2xx, до отметки delivered | допустим дубль с тем же webhook-event-id |
| I14 | Consumer не может обработать инфраструктурную ошибку 3 раза | TTL retry.2/retry.3, system_attempt=3 → DLQ; gateway не на terminal |
| I14b | Legacy pending (attempts=1/2) без retry Outbox | recovery в outbox-publisher; идемпотентно; lease/attempts=0 не трогает |
| I14c | Crash после commit retry Outbox до ACK | нет второго HTTP; detail=retry_already_scheduled |
| I15 | Прод окружение не доступно тестам | test DSN/volume/vhost независимы |

### Нумерация I09/I10 (фактические тесты)

| Код | Файл / тест | Смысл |
|---|---|---|
| I09 | `test_i09_three_webhook_failures_exhausted_and_dlq` | `500×3` → exhausted + DLQ |
| I10 | `test_i10_webhook_recovers_on_third_and_second_attempt` | `500×2→200`, `500→200`, failed+webhook OK |

## Process-level E2E (`pytest -m e2e`)

Реальные entrypoints `api` / `outbox-publisher` / `consumer` как subprocess +
изолированный localhost webhook receiver. Только `postgres-test` / `rabbitmq-test`.

| № | Сценарий | Ожидается |
|---|---|---|
| E2E-01 | POST → outbox → publish → terminal → webhook 200 → GET | `delivered`, стабильный `X-Webhook-Event-ID` |
| E2E-02 | receiver `500,500,200` | ровно 3 HTTP, интервалы порядка 2/4 с, без DLQ |
| E2E-03 | receiver `500×3` | `exhausted`, сообщение в `payments.dlq` + `x-death` |
| E2E-04 | API без publisher, затем старт publisher/consumer | 202 + Outbox pending → после recovery `delivered` |
| E2E-05 | идемпотентность / 401 / 409; повторный AMQP | тот же `payment_id`; gateway/webhook не повторяются после `delivered` |

## Приёмка

1. `uv run ruff check .` — PASS.
2. `uv run ruff format --check .` — PASS.
3. `uv run mypy src` — PASS (по настроенному строгому профилю).
4. `uv run pytest -m unit` — PASS (ожидаемый baseline: 95).
5. `docker compose -f infra/compose.yaml --profile test run --rm tests uv run pytest -m integration` — PASS
   (ожидаемый baseline: 31; тестовый runner → отдельные DB/RabbitMQ).
6. `docker compose ... run --rm tests uv run pytest -m e2e` — PASS (5 process-level).
7. `docker compose -f infra/compose.yaml up -d --build` поднимает сервис и healthchecks.
8. Проверены POST 202, GET 200, webhook и DLQ по инструкции README.
9. Проверено, что приватные конфиги не трекаются `git ls-files`.

Статус прогонов фиксируется в отчёте этапа 006 по фактическим exit code.
