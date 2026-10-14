from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from fastapi import status
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from payrelay.api.app import create_app
from payrelay.api.dependencies import get_db_session
from payrelay.api.messages import (
    API_KEY_INVALID,
    IDEMPOTENCY_CONFLICT,
    PAYMENT_NOT_FOUND,
)
from payrelay.application.payments import PaymentView
from payrelay.core.config import Settings
from payrelay.domain.errors import IdempotencyConflictError, PaymentNotFoundError


def _payload(**overrides: object) -> dict[str, object]:
    """Валидное тело POST /api/v1/payments.

    Args:
        **overrides: Переопределения полей.

    Returns:
        JSON-тело запроса.
    """
    body: dict[str, object] = {
        "amount": "1500.50",
        "currency": "RUB",
        "description": "Заказ #123",
        "metadata": {"order_id": "123"},
        "webhook_url": "https://merchant.example.com/payments/callback",
    }
    body.update(overrides)
    return body


@pytest.fixture
async def api_client(
    test_settings: Settings,
) -> AsyncIterator[tuple[AsyncClient, str, Any]]:
    """ASGI-клиент с подменой сессии БД.

    Args:
        test_settings: Тестовые настройки.

    Yields:
        Кортеж (client, api_key, app).
    """
    app = create_app(test_settings)
    fake_session = MagicMock(spec=AsyncSession)

    async def _override_session() -> AsyncIterator[AsyncSession]:
        yield fake_session

    app.dependency_overrides[get_db_session] = _override_session
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, test_settings.api_key.get_secret_value(), app
    app.dependency_overrides.clear()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_framework_404_unknown_route(
    api_client: tuple[AsyncClient, str, Any],
) -> None:
    """Неизвестный маршрут → framework 404, не доменный detail платежа."""
    client, api_key, _app = api_client
    response = await client.get(
        "/api/v1/unknown-route",
        headers={"X-API-Key": api_key},
    )
    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json()["detail"] != PAYMENT_NOT_FOUND


@pytest.mark.unit
@pytest.mark.asyncio
async def test_framework_405_preserves_allow_header(
    api_client: tuple[AsyncClient, str, Any],
) -> None:
    """Неверный метод → 405 и заголовок Allow."""
    client, api_key, _app = api_client
    response = await client.request(
        "PUT",
        "/health/live",
        headers={"X-API-Key": api_key},
    )
    assert response.status_code == status.HTTP_405_METHOD_NOT_ALLOWED
    allow = response.headers.get("allow", "")
    assert "GET" in allow.upper()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_domain_errors_mapped_by_handlers(
    api_client: tuple[AsyncClient, str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Доменные исключения маппятся handlers без try/except в роутере."""
    client, api_key, _app = api_client
    payment_id = UUID("11111111-1111-4111-8111-111111111111")

    async def _get_missing(_self: Any, _payment_id: UUID) -> PaymentView:
        raise PaymentNotFoundError(str(payment_id))

    monkeypatch.setattr(
        "payrelay.api.routers.payments.PaymentService.get_payment",
        _get_missing,
    )
    not_found = await client.get(
        f"/api/v1/payments/{payment_id}",
        headers={"X-API-Key": api_key},
    )
    assert not_found.status_code == status.HTTP_404_NOT_FOUND
    assert not_found.json()["detail"] == PAYMENT_NOT_FOUND

    async def _create_conflict(_self: Any, _command: Any) -> PaymentView:
        raise IdempotencyConflictError()

    monkeypatch.setattr(
        "payrelay.api.routers.payments.PaymentService.create_payment",
        _create_conflict,
    )
    conflict = await client.post(
        "/api/v1/payments",
        headers={"X-API-Key": api_key, "Idempotency-Key": "k-conflict-handler"},
        json=_payload(),
    )
    assert conflict.status_code == status.HTTP_409_CONFLICT
    assert conflict.json()["detail"] == IDEMPOTENCY_CONFLICT


@pytest.mark.unit
@pytest.mark.asyncio
async def test_invalid_api_key_uses_typed_error(
    api_client: tuple[AsyncClient, str, Any],
) -> None:
    """Неверный API-ключ → 401 через typed InvalidAPIKeyError handler."""
    client, _api_key, _app = api_client
    response = await client.get("/health/live")
    assert response.status_code == status.HTTP_401_UNAUTHORIZED
    assert response.json()["detail"] == API_KEY_INVALID
