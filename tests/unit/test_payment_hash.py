from __future__ import annotations

from decimal import Decimal

import pytest

from payrelay.application.request_hash import compute_request_hash, format_amount


@pytest.mark.unit
def test_u05_metadata_key_order_does_not_change_hash() -> None:
    """U05: порядок ключей metadata не меняет хэш."""
    left = compute_request_hash(
        amount=Decimal("1500.50"),
        currency="RUB",
        description="Заказ #123",
        metadata={"a": 1, "b": 2},
        webhook_url="https://merchant.example.com/hook",
    )
    right = compute_request_hash(
        amount=Decimal("1500.50"),
        currency="RUB",
        description="Заказ #123",
        metadata={"b": 2, "a": 1},
        webhook_url="https://merchant.example.com/hook",
    )
    assert left == right
    assert len(left) == 64


@pytest.mark.unit
def test_u05_amount_format_normalized() -> None:
    """U05: 10.5 и 10.50 дают одинаковый хэш."""
    left = compute_request_hash(
        amount=Decimal("10.5"),
        currency="USD",
        description="x",
        metadata={},
        webhook_url="https://example.com/h",
    )
    right = compute_request_hash(
        amount=Decimal("10.50"),
        currency="USD",
        description="x",
        metadata={},
        webhook_url="https://example.com/h",
    )
    assert left == right
    assert format_amount(Decimal("10.5")) == "10.50"


@pytest.mark.unit
def test_u06_meaningful_field_change_changes_hash() -> None:
    """U06: изменение значимого поля меняет хэш."""
    base = {
        "amount": Decimal("100.00"),
        "currency": "EUR",
        "description": "same",
        "metadata": {"order_id": "1"},
        "webhook_url": "https://example.com/h",
    }
    original = compute_request_hash(**base)
    changed_amount = compute_request_hash(**{**base, "amount": Decimal("100.01")})
    changed_meta = compute_request_hash(
        **{**base, "metadata": {"order_id": "2"}},
    )
    changed_url = compute_request_hash(
        **{**base, "webhook_url": "https://example.com/other"},
    )
    assert original != changed_amount
    assert original != changed_meta
    assert original != changed_url


@pytest.mark.unit
def test_nested_metadata_key_order_stable() -> None:
    """Вложенные объекты metadata тоже канонизируются sort_keys."""
    left = compute_request_hash(
        amount=Decimal("1.00"),
        currency="RUB",
        description="d",
        metadata={"outer": {"z": 1, "a": 2}},
        webhook_url="https://example.com/h",
    )
    right = compute_request_hash(
        amount=Decimal("1.00"),
        currency="RUB",
        description="d",
        metadata={"outer": {"a": 2, "z": 1}},
        webhook_url="https://example.com/h",
    )
    assert left == right
