from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from payrelay.api.schemas import (
    CreatePaymentRequest,
    validate_idempotency_key_value,
)


def _valid_payload(**overrides: object) -> dict[str, object]:
    """Собрать минимально валидное тело запроса.

    Args:
        **overrides: Поля для переопределения.

    Returns:
        Словарь тела POST.
    """
    payload: dict[str, object] = {
        "amount": "1500.50",
        "currency": "RUB",
        "description": "Заказ #123",
        "metadata": {"order_id": "123"},
        "webhook_url": "https://merchant.example.com/payments/callback",
    }
    payload.update(overrides)
    return payload


@pytest.mark.unit
@pytest.mark.parametrize(
    "amount",
    [
        "0",
        "0.00",
        "-1",
        "-0.01",
        "NaN",
        "Infinity",
        "-Infinity",
        "1.001",
        "100000000000000000.00",
    ],
)
def test_u01_invalid_amounts_rejected(amount: str) -> None:
    """U01: нулевые, отрицательные, NaN/Inf, >2 знаков и переполнение → ошибка."""
    with pytest.raises(ValidationError):
        CreatePaymentRequest.model_validate(_valid_payload(amount=amount))


@pytest.mark.unit
def test_u01_float_nan_rejected() -> None:
    """U01: float NaN отклоняется."""
    with pytest.raises(ValidationError):
        CreatePaymentRequest.model_validate(_valid_payload(amount=float("nan")))


@pytest.mark.unit
def test_u01_valid_amount_quantized() -> None:
    """U01 позитив: корректная сумма квантуется до 2 знаков."""
    model = CreatePaymentRequest.model_validate(_valid_payload(amount="10.5"))
    assert model.amount == Decimal("10.50")


@pytest.mark.unit
@pytest.mark.parametrize("currency", ["GBP", "rub", "USDT", "", "RUBB"])
def test_u02_invalid_currency_rejected(currency: str) -> None:
    """U02: валюта вне RUB/USD/EUR → ошибка."""
    with pytest.raises(ValidationError):
        CreatePaymentRequest.model_validate(_valid_payload(currency=currency))


@pytest.mark.unit
@pytest.mark.parametrize(
    "key",
    [
        "",
        " " + "x" * 10,
        "x" * 10 + " ",
        "a" * 256,
        "bad\x00key",
        "line\nbreak",
    ],
)
def test_u03_invalid_idempotency_key_rejected(key: str) -> None:
    """U03: некорректный Idempotency-Key отклоняется."""
    with pytest.raises(ValueError):
        validate_idempotency_key_value(key)


@pytest.mark.unit
def test_u03_valid_idempotency_key_accepted() -> None:
    """U03 позитив: обычный непробельный ключ принимается."""
    assert validate_idempotency_key_value("order-123") == "order-123"


@pytest.mark.unit
def test_u04_metadata_must_be_object() -> None:
    """U04: metadata не объект → ошибка."""
    with pytest.raises(ValidationError):
        CreatePaymentRequest.model_validate(_valid_payload(metadata=["x"]))


@pytest.mark.unit
@pytest.mark.parametrize(
    "webhook_url",
    [
        "ftp://example.com/hook",
        "not-a-url",
        "https://user:pass@example.com/hook",
        "http:///no-host",
        "",
    ],
)
def test_u04_invalid_webhook_url_rejected(webhook_url: str) -> None:
    """U04: неверный webhook URL → ошибка."""
    with pytest.raises(ValidationError):
        CreatePaymentRequest.model_validate(_valid_payload(webhook_url=webhook_url))


@pytest.mark.unit
def test_u04_description_blank_after_trim_rejected() -> None:
    """U04: описание из одних пробелов отклоняется."""
    with pytest.raises(ValidationError):
        CreatePaymentRequest.model_validate(_valid_payload(description="   "))
