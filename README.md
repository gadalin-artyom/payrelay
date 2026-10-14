# PayRelay

Асинхронный сервис обработки платежей (тестовое задание).

**Статус:** подготовлен архитектурный проект и пошаговый план. Приложение реализуется
по этапам; команды `docker compose up` будут работоспособны после соответствующего этапа.

## Требования

- FastAPI, Pydantic v2; SQLAlchemy 2.0 Async, PostgreSQL, Alembic.
- RabbitMQ/FastStream: `payments.new`, повторные попытки и DLQ.
- Статический API ключ `X-API-Key`.
- Транзакционный Outbox, `Idempotency-Key`, webhook.
- Docker Compose, unit/integration/e2e тесты.

## Документация

- [Архитектура](docs/architecture.md)
- [API контракт](docs/api-contract.md)
- [Матрица тестирования](docs/test-matrix.md)

Локальные инструкции для Cursor (`AGENTS.md`, `SKILLS.md`, `.cursor`, `.ai`) исключены из Git.
Реальные команды запуска и примеры curl будут добавлены в README в финальном этапе реализации.
