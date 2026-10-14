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
| U13 | Webhook timeout / 5xx | delayed retry 2s, 4s |
| U14 | Webhook повтор после подтверждённого успеха | нет повторного POST |
| U15 | После 3-й неудачи доставки | exhausted + routing в DLQ |
| U16 | Запрещённый private/loopback webhook URL | отклонён production policy |

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
| I09 | Webhook возвращает 500 дважды, потом 200 | ровно 3 попытки, backoff, delivered |
| I10 | Webhook всё время 500 | exhausted и сообщение в payments.dlq |
| I11 | Crash после broker confirm, до mark-published | безопасная повторная публикация |
| I12 | Consumer crash после commit payment, до webhook | payment не обрабатывается повторно |
| I13 | Crash после webhook 2xx, до отметки delivered | допустим дубль с тем же webhook-event-id |
| I14 | Consumer не может обработать инфраструктурную ошибку 3 раза | system_attempt=3, DLQ, нет бесконечного requeue |
| I15 | Прод окружение не доступно тестам | test DSN/volume/vhost независимы |

## Приёмка

1. `uv run ruff check .` — PASS.
2. `uv run ruff format --check .` — PASS.
3. `uv run mypy src` — PASS (по настроенному строгому профилю).
4. `uv run pytest -m unit` — PASS.
5. `docker compose -f infra/compose.yaml --profile test run --rm tests uv run pytest -m integration` — PASS
   (тестовый runner обращается к отдельным тестовым DB и RabbitMQ через Compose).
6. `docker compose -f infra/compose.yaml up -d --build` поднимает сервис и healthchecks.
7. Проверены POST 202, GET 200, webhook и DLQ по инструкции README.
8. Проверено, что приватные конфиги не трекаются `git ls-files`.

Здесь перечислены **целевые**, а не уже пройденные проверки.
