from __future__ import annotations

import math
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Literal
from urllib.parse import urlparse
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
)

from payrelay.api.constants import DESCRIPTION_MAX_LENGTH, IDEMPOTENCY_KEY_MAX_LENGTH
from payrelay.api.messages import AMOUNT_INVALID

_AMOUNT_QUANT = Decimal("0.01")
_AMOUNT_MAX = Decimal("9999999999999999.99")
CurrencyLiteral = Literal["RUB", "USD", "EUR"]


def validate_idempotency_key_value(value: str) -> str:
    """Проверить заголовок Idempotency-Key.

    Args:
        value: Сырое значение заголовка.

    Returns:
        То же значение, если оно валидно.

    Raises:
        ValueError: при пустом, слишком длинном ключе, пробелах по краям
            или управляющих символах.
    """
    if not value:
        msg = "Некорректный заголовок Idempotency-Key: значение не может быть пустым."
        raise ValueError(msg)
    if len(value) > IDEMPOTENCY_KEY_MAX_LENGTH:
        msg = (
            "Некорректный заголовок Idempotency-Key: "
            f"длина не должна превышать {IDEMPOTENCY_KEY_MAX_LENGTH} символов."
        )
        raise ValueError(msg)
    if value != value.strip():
        msg = "Некорректный заголовок Idempotency-Key: не допускаются пробелы по краям."
        raise ValueError(msg)
    if any(ord(char) < 32 for char in value):
        msg = (
            "Некорректный заголовок Idempotency-Key: "
            "не допускаются управляющие символы."
        )
        raise ValueError(msg)
    return value


def validate_webhook_url_value(value: str) -> str:
    """Проверить, что URL webhook — http(s) без userinfo.

    Args:
        value: Строка URL.

    Returns:
        Исходный URL при успехе.

    Raises:
        ValueError: если схема/хост некорректны или есть userinfo.
    """
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"}:
        msg = "Укажите корректный URL webhook с протоколом HTTP или HTTPS."
        raise ValueError(msg)
    if not parsed.netloc:
        msg = "Укажите корректный URL webhook с протоколом HTTP или HTTPS."
        raise ValueError(msg)
    if parsed.username is not None or parsed.password is not None:
        msg = "Укажите корректный URL webhook с протоколом HTTP или HTTPS."
        raise ValueError(msg)
    return value


class ApiErrorResponse(BaseModel):
    """Ответ с строковым detail (401/404/409/500)."""

    detail: str = Field(..., description="Человекочитаемое описание ошибки.")


class ValidationErrorItem(BaseModel):
    """Элемент списка ошибок валидации FastAPI (422)."""

    loc: list[str | int] = Field(..., description="Путь к полю или заголовку.")
    msg: str = Field(..., description="Сообщение об ошибке на русском.")
    type: str = Field(..., description="Стабильный тип ошибки Pydantic/FastAPI.")


class ValidationErrorResponse(BaseModel):
    """Ответ 422 с списком ошибок валидации."""

    detail: list[ValidationErrorItem]


class CreatePaymentRequest(BaseModel):
    """Тело POST /api/v1/payments."""

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "amount": "1500.50",
                    "currency": "RUB",
                    "description": "Заказ #123",
                    "metadata": {"order_id": "123"},
                    "webhook_url": "https://merchant.example.com/payments/callback",
                }
            ]
        },
    )

    amount: Decimal = Field(
        ...,
        title="Сумма",
        description=(
            "Положительная сумма с точностью до двух знаков. "
            'Рекомендуется передавать строкой (например, "1500.50").'
        ),
        examples=["1500.50"],
    )
    currency: CurrencyLiteral = Field(
        ...,
        title="Валюта",
        description="Допустимые значения: RUB, USD, EUR.",
        examples=["RUB"],
    )
    description: str = Field(
        ...,
        min_length=1,
        max_length=DESCRIPTION_MAX_LENGTH,
        title="Описание",
        description=f"Непустая строка до {DESCRIPTION_MAX_LENGTH} символов.",
        examples=["Заказ #123"],
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        title="Метаданные",
        description=(
            "Произвольный JSON-объект. Порядок ключей не влияет на идемпотентность."
        ),
        examples=[{"order_id": "123"}],
    )
    webhook_url: str = Field(
        ...,
        title="URL webhook",
        description="URL мерчанта для callback (только HTTP или HTTPS, без userinfo).",
        examples=["https://merchant.example.com/payments/callback"],
    )

    @field_validator("amount", mode="before")
    @classmethod
    def _parse_amount(cls, value: Any) -> Any:
        """Преобразовать вход к Decimal без float-артефактов где возможно.

        Args:
            value: Сырое значение amount.

        Returns:
            Значение, пригодное для дальнейшей валидации Decimal.
        """
        if value is None:
            return value
        if isinstance(value, bool):
            raise ValueError(AMOUNT_INVALID)
        if isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError(AMOUNT_INVALID)
            return str(value)
        if isinstance(value, str):
            return value.strip()
        return value

    @field_validator("amount")
    @classmethod
    def _validate_amount(cls, value: Decimal) -> Decimal:
        """Проверить положительность, конечность, масштаб и диапазон NUMERIC(18,2).

        Args:
            value: Сумма.

        Returns:
            Квантованная сумма с двумя знаками.

        Raises:
            ValueError: при нарушении ограничений.
        """
        try:
            if not value.is_finite():
                raise ValueError(AMOUNT_INVALID)
        except InvalidOperation as exc:
            raise ValueError(AMOUNT_INVALID) from exc

        if value <= 0:
            raise ValueError(AMOUNT_INVALID)
        if value > _AMOUNT_MAX:
            raise ValueError(AMOUNT_INVALID)
        exponent = value.as_tuple().exponent
        if isinstance(exponent, int) and exponent < -2:
            raise ValueError(AMOUNT_INVALID)
        return value.quantize(_AMOUNT_QUANT)

    @field_validator("description")
    @classmethod
    def _normalize_description(cls, value: str) -> str:
        """Обрезать пробелы и проверить непустоту.

        Args:
            value: Описание.

        Returns:
            Нормализованная строка 1..500 символов.
        """
        normalized = value.strip()
        if not normalized:
            msg = "Описание не может быть пустым."
            raise ValueError(msg)
        if len(normalized) > DESCRIPTION_MAX_LENGTH:
            msg = f"Описание не должно превышать {DESCRIPTION_MAX_LENGTH} символов."
            raise ValueError(msg)
        return normalized

    @field_validator("metadata", mode="before")
    @classmethod
    def _validate_metadata(cls, value: Any) -> Any:
        """Разрешить только JSON-объект.

        Args:
            value: Сырое metadata.

        Returns:
            Объект-словарь или значение по умолчанию.

        Raises:
            ValueError: если передан не объект.
        """
        if value is None:
            return {}
        if not isinstance(value, dict):
            msg = "metadata должен быть JSON-объектом."
            raise ValueError(msg)
        return value

    @field_validator("webhook_url")
    @classmethod
    def _validate_webhook_url(cls, value: str) -> str:
        """Делегировать проверку URL.

        Args:
            value: URL webhook.

        Returns:
            Валидный URL.
        """
        return validate_webhook_url_value(value)


class CreatePaymentResponse(BaseModel):
    """Ответ 202 на создание платежа."""

    payment_id: UUID = Field(..., description="Идентификатор созданного платежа.")
    status: str = Field(..., description="Бизнес-статус платежа (обычно pending).")
    created_at: datetime = Field(..., description="Время создания платежа (UTC).")


class PaymentResponse(BaseModel):
    """Ответ 200 GET /api/v1/payments/{payment_id}."""

    payment_id: UUID
    amount: Decimal = Field(..., description="Сумма строкой с двумя знаками в JSON.")
    currency: str
    description: str
    metadata: dict[str, Any]
    status: str
    webhook_url: str
    webhook_delivery_status: str = Field(
        ...,
        description="Статус доставки webhook: pending, delivered или exhausted.",
    )
    created_at: datetime
    processed_at: datetime | None

    @field_serializer("amount")
    def _serialize_amount(self, value: Decimal) -> str:
        """Сериализовать сумму строкой с двумя знаками.

        Args:
            value: Decimal-сумма.

        Returns:
            Строка вида ``"1500.50"``.
        """
        return f"{value.quantize(_AMOUNT_QUANT):.2f}"
