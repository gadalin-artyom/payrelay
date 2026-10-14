# API Contract — PayRelay v1

> Основа: исходное ТЗ. Детали по валидации/повторным запросам — проектное уточнение.

## POST /api/v1/payments

Headers: `X-API-Key: <key>`, `Idempotency-Key: <opaque-string>`.

```json
{
  "amount": "1500.50",
  "currency": "RUB",
  "description": "Заказ #123",
  "metadata": {"order_id": "123"},
  "webhook_url": "https://merchant.example.com/payments/callback"
}
```

Возврат 202:

```json
{
  "payment_id": "11111111-1111-4111-8111-111111111111",
  "status": "pending",
  "created_at": "2026-10-14T10:00:00Z"
}
```

Уточнения:

- Сумму принимать как Decimal с 2 десятичными знаками; строка в примере исключает потерю точности.
  Входной JSON number тоже поддерживать лишь при безопасной конвертации/валидации.
  В ответах `amount` сериализуется **строкой** (Pydantic serializer).
- `amount > 0`, валюта только RUB/USD/EUR; description — непустая строка с лимитом длины.
- Один и тот же `Idempotency-Key` + эквивалентное тело — **тот же payment_id**, HTTP 202.
- Тот же ключ + другое тело — HTTP 409; уникальность обеспечивается индексом БД.
- Ключ отсутствует — HTTP 422 (валидация заголовка).
- API ключ отсутствует/неверен — HTTP 401, не раскрывать значение.
- `webhook_url` — только http/https; обязательная защита от SSRF в production.
- Публичные сообщения об ошибках — **на русском**; имена полей/заголовков и коды HTTP
  не меняются. Формат `detail`: строка для 401/404/409/500; список
  `{loc, msg, type}` для 422.

Примеры ошибок:

| HTTP | detail (фрагмент) |
|---:|---|
| 401 | `"Неверный или отсутствующий API-ключ."` |
| 404 | `"Платёж не найден."` |
| 409 | `"Этот Idempotency-Key уже использован для другого платежа."` |
| 422 | `[{"loc":["header","Idempotency-Key"],"msg":"Обязательный заголовок Idempotency-Key не указан.","type":"missing"}]` |
| 500 | `"Внутренняя ошибка сервера."` (без деталей инфраструктуры) |

### Swagger / OpenAPI

При `ENABLE_DOCS=true` и `APP_ENV!=production`:

- UI: `http://127.0.0.1:8000/docs`, схема: `/openapi.json`.
- Кнопка **Authorize** — ввести значение `X-API-Key` из `.env` (не коммитьте секреты).
- Для `POST /api/v1/payments` дополнительно указать заголовок `Idempotency-Key`
  (клиент генерирует сам; UUID v4 рекомендуется, но не обязателен).
- Интерфейс Swagger UI целиком не переводится: локализованы описания API и примеры ошибок.

## GET /api/v1/payments/{payment_id}

Headers: `X-API-Key: <key>`.

Возврат 200: payment_id, amount, currency, description, metadata, status,
webhook_url, created_at, processed_at. Можно добавить `webhook_delivery_status`
как дополнительное поле, не меняя обязательные поля исходного ТЗ.

Неизвестный UUID — 404; синтаксически неверный UUID — 422.

## POST webhook на URL мерчанта

Проектный контракт (не детализирован в ТЗ):

```json
{
  "webhook_event_id": "22222222-2222-4222-8222-222222222222",
  "event_type": "payment.processed",
  "payment_id": "11111111-1111-4111-8111-111111111111",
  "status": "succeeded",
  "amount": "1500.50",
  "currency": "RUB",
  "processed_at": "2026-10-14T10:00:03Z"
}
```

Отправлять `X-Webhook-Event-ID` со **стабильным** UUID webhook-события для дедупликации у
получателя (один ID на все логические попытки одного платежа; не путать с
`event_id` Outbox/AMQP). HTTP 2xx — успех; timeout/сетевые ошибки/не-2xx —
неуспех и до 2 отложенных retry (2s/4s через Outbox); после 3-й неудачи —
`exhausted` и сообщение в DLQ. Без дополнительных подписей: исходное ТЗ их
не требует; безопасность URL — отдельно.

## Состояния

- Бизнес: `pending -> succeeded | failed`. Вторая бизнес-обработка запрещена.
- Доставка webhook: `pending -> delivered | exhausted`, независимо от бизнес-статуса.
- `failed` по вероятности 10% — **итог бизнеса**, а не сигнал retry/DLQ.
