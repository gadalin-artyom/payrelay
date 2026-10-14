from __future__ import annotations

from typing import Final

from payrelay.api.constants import IDEMPOTENCY_KEY_HEADER_NAME

API_KEY_INVALID: Final[str] = "Неверный или отсутствующий API-ключ."
PAYMENT_NOT_FOUND: Final[str] = "Платёж не найден."
IDEMPOTENCY_CONFLICT: Final[str] = (
    "Этот Idempotency-Key уже использован для другого платежа."
)
INTERNAL_SERVER_ERROR: Final[str] = "Внутренняя ошибка сервера."
FALLBACK_VALIDATION: Final[str] = "Некорректное значение поля."
AMOUNT_INVALID: Final[str] = (
    "Сумма должна быть положительным числом с точностью до двух знаков после запятой."
)
CURRENCY_ALLOWED: Final[str] = "Допустимые валюты: RUB, USD, EUR."
IDEMPOTENCY_KEY_MISSING: Final[str] = (
    f"Обязательный заголовок {IDEMPOTENCY_KEY_HEADER_NAME} не указан."
)
UUID_INVALID: Final[str] = "Некорректный формат UUID."

VALIDATION_TYPE_MESSAGES: Final[dict[str, str]] = {
    "missing": "Обязательное поле отсутствует.",
    "string_type": "Значение должно быть строкой.",
    "string_too_short": "Строка слишком короткая.",
    "string_too_long": "Строка слишком длинная.",
    "uuid_parsing": UUID_INVALID,
    "uuid_type": UUID_INVALID,
    "literal_error": "Значение не входит в список допустимых.",
    "json_invalid": "Тело запроса содержит некорректный JSON.",
    "json_type": "Ожидался JSON-объект.",
    "dict_type": "Значение должно быть JSON-объектом.",
    "decimal_parsing": AMOUNT_INVALID,
    "decimal_type": AMOUNT_INVALID,
    "float_parsing": AMOUNT_INVALID,
    "int_parsing": "Значение должно быть целым числом.",
    "bool_parsing": "Значение должно быть логическим.",
    "greater_than": "Значение должно быть больше нуля.",
    "greater_than_equal": "Значение должно быть не меньше нижней границы.",
    "less_than": "Значение превышает допустимый максимум.",
    "less_than_equal": "Значение превышает допустимый максимум.",
    "finite_number": "Значение должно быть конечным числом.",
    "model_attributes_type": "Ожидался JSON-объект с полями запроса.",
    "extra_forbidden": "Передано неизвестное поле.",
}
