"""Pydantic-схемы HTTP API платежей."""

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
        msg = "Idempotency-Key must be a non-empty string"
        raise ValueError(msg)
    if len(value) > 255:
        msg = "Idempotency-Key must be at most 255 characters"
        raise ValueError(msg)
    if value != value.strip():
        msg = "Idempotency-Key must not have leading or trailing whitespace"
        raise ValueError(msg)
    if any(ord(char) < 32 for char in value):
        msg = "Idempotency-Key must not contain control characters"
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
        msg = "webhook_url must use http or https scheme"
        raise ValueError(msg)
    if not parsed.netloc:
        msg = "webhook_url must include a host"
        raise ValueError(msg)
    if parsed.username is not None or parsed.password is not None:
        msg = "webhook_url must not contain userinfo"
        raise ValueError(msg)
    return value


class CreatePaymentRequest(BaseModel):
    """Тело POST /api/v1/payments."""

    model_config = ConfigDict(extra="forbid")

    amount: Decimal
    currency: CurrencyLiteral
    description: str = Field(..., min_length=1, max_length=500)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: str

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
            msg = "amount must be a decimal number"
            raise ValueError(msg)
        if isinstance(value, float):
            if not math.isfinite(value):
                msg = "amount must be a finite number"
                raise ValueError(msg)
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
                msg = "amount must be a finite number"
                raise ValueError(msg)
        except InvalidOperation as exc:
            msg = "amount must be a valid decimal"
            raise ValueError(msg) from exc

        if value <= 0:
            msg = "amount must be greater than zero"
            raise ValueError(msg)
        if value > _AMOUNT_MAX:
            msg = "amount exceeds NUMERIC(18,2) range"
            raise ValueError(msg)
        exponent = value.as_tuple().exponent
        if isinstance(exponent, int) and exponent < -2:
            msg = "amount must have at most 2 decimal places"
            raise ValueError(msg)
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
            msg = "description must be a non-empty string"
            raise ValueError(msg)
        if len(normalized) > 500:
            msg = "description must be at most 500 characters"
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
            msg = "metadata must be a JSON object"
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

    payment_id: UUID
    status: str
    created_at: datetime


class PaymentResponse(BaseModel):
    """Ответ 200 GET /api/v1/payments/{payment_id}."""

    payment_id: UUID
    amount: Decimal
    currency: str
    description: str
    metadata: dict[str, Any]
    status: str
    webhook_url: str
    webhook_delivery_status: str
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
