# PayRelay

Асинхронный сервис обработки платежей (тестовое задание).

## Требования

- Python 3.12, uv.
- FastAPI, Pydantic v2; SQLAlchemy 2.0 Async, PostgreSQL, Alembic.
- RabbitMQ/FastStream, транзакционный Outbox, `Idempotency-Key`.
- Docker Compose, unit/integration тесты.

## Документация

- [Архитектура](docs/architecture.md)
- [API контракт](docs/api-contract.md)
- [Матрица тестирования](docs/test-matrix.md)

## Локальный запуск

```bash
cp .env.example .env   # подставьте свои значения; файл не коммитится
docker compose -f infra/compose.yaml up -d --build
docker compose -f infra/compose.yaml run --rm api uv run alembic upgrade head
```

Роли:

- `api` — POST/GET платежей, health (не ждёт RabbitMQ на каждый запрос).
- `outbox-publisher` — claim Outbox → publish confirm → `published_at`.
- `consumer` — FastStream: эмуляция шлюза, terminal status, webhook HTTP.
  При HTTP-ошибке webhook остаётся `pending` (attempts=1) и сообщение ACK'ается;
  полноценный retry/backoff/DLQ — **этап 005** (пока не production-ready для webhook recovery).

Проверка liveness (ключ из окружения контейнера / `.env`):

```bash
curl -fsS -H "X-API-Key: $API_KEY" http://127.0.0.1:8000/health/live
```

Создание платежа:

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

## RabbitMQ (локальный просмотр)

Management UI (только dev-брокер, не обязателен для runtime):

- URL: http://127.0.0.1:15672
- Учётка из `.env` / `RABBITMQ_DEFAULT_USER` / `RABBITMQ_DEFAULT_PASS`

Ожидаемая topology после старта `outbox-publisher`:

- exchange `payments.exchange` (durable direct)
- queue `payments.new` (+ DLX `payments.dlx` / `payments.dlq`)
- retry queues `payments.retry.2` / `payments.retry.3` (TTL 2s/4s)
- DLQ `payments.dlq`

Безопасный просмотр сообщений в UI: Queues → `payments.new` → Get messages
(не делайте global purge/reset). `event_id` в теле и `message_id` должны совпадать.

## Проверки качества

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest -m unit
```

## Integration (отдельные postgres-test / rabbitmq-test)

Миграции и тесты работают **только** с `payrelay_test` / `rabbitmq-test`
(фикстуры отказываются от обычных DSN/AMQP URL).

```bash
docker compose -f infra/compose.yaml --profile test config

docker compose -f infra/compose.yaml --profile test run --rm tests \
  uv run pytest -m integration
```

Явный шаг миграций:

```bash
docker compose -f infra/compose.yaml --profile test run --rm tests \
  uv run alembic upgrade head
```

Локальные инструкции для Cursor (`AGENTS.md`, `SKILLS.md`, `.cursor`, `.ai`) исключены из Git.
