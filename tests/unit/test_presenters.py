from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest

from payrelay.api.presenters import to_create_payment_response, to_payment_response
from payrelay.application.payments import PaymentView


def _sample_view(*, processed_at: datetime | None) -> PaymentView:
    """Собрать тестовый PaymentView.

    Args:
        processed_at: Время обработки или None.

    Returns:
        Заполненное представление платежа.
    """
    return PaymentView(
        payment_id=UUID("11111111-1111-4111-8111-111111111111"),
        amount=Decimal("1500.50"),
        currency="RUB",
        description="Заказ #123",
        metadata={"order_id": "123"},
        status="pending",
        webhook_url="https://merchant.example.com/payments/callback",
        webhook_delivery_status="pending",
        created_at=datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC),
        processed_at=processed_at,
    )


@pytest.mark.unit
def test_to_create_payment_response_maps_subset() -> None:
    """Presenter создания возвращает только payment_id/status/created_at."""
    view = _sample_view(processed_at=None)
    response = to_create_payment_response(view)

    assert response.payment_id == view.payment_id
    assert response.status == "pending"
    assert response.created_at == view.created_at
    payload = response.model_dump(mode="json")
    assert set(payload) == {"payment_id", "status", "created_at"}
    assert payload["payment_id"] == "11111111-1111-4111-8111-111111111111"


@pytest.mark.unit
def test_to_payment_response_preserves_decimal_and_none_processed_at() -> None:
    """Presenter GET сохраняет Decimal как строку и None в processed_at."""
    view = _sample_view(processed_at=None)
    response = to_payment_response(view)

    assert response.amount == Decimal("1500.50")
    assert response.processed_at is None
    assert response.payment_id == view.payment_id
    assert response.status == "pending"
    assert response.created_at == view.created_at

    payload = response.model_dump(mode="json")
    assert payload["amount"] == "1500.50"
    assert isinstance(payload["amount"], str)
    assert payload["processed_at"] is None
    assert payload["payment_id"] == "11111111-1111-4111-8111-111111111111"
    assert payload["metadata"] == {"order_id": "123"}


@pytest.mark.unit
def test_to_payment_response_maps_processed_at() -> None:
    """Presenter GET прокидывает processed_at без потери timezone."""
    processed_at = datetime(2026, 1, 15, 12, 5, 0, tzinfo=UTC)
    view = _sample_view(processed_at=processed_at)
    response = to_payment_response(view)

    assert response.processed_at == processed_at
    payload = response.model_dump(mode="json")
    assert payload["processed_at"] == "2026-01-15T12:05:00Z"
