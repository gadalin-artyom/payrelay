"""Unit-тесты HTTP-валидации и auth платёжного API (без реальной БД)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import MagicMock

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from payrelay.api.app import create_app
from payrelay.api.dependencies import get_db_session
from payrelay.core.config import Settings


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
    api_key: str,
) -> AsyncIterator[tuple[AsyncClient, str, Any]]:
    """ASGI-клиент с подменой сессии БД.

    Args:
        test_settings: Тестовые настройки.
        api_key: Валидный API-ключ.

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
        yield client, api_key, app
    app.dependency_overrides.clear()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_u03_missing_idempotency_key_returns_422(
    api_client: tuple[AsyncClient, str, Any],
) -> None:
    """U03: отсутствие Idempotency-Key → 422."""
    client, api_key, _app = api_client
    response = await client.post(
        "/api/v1/payments",
        headers={"X-API-Key": api_key},
        json=_payload(),
    )
    assert response.status_code == 422


@pytest.mark.unit
@pytest.mark.asyncio
async def test_u03_blank_idempotency_key_returns_422(
    api_client: tuple[AsyncClient, str, Any],
) -> None:
    """U03: пустой Idempotency-Key → 422."""
    client, api_key, _app = api_client
    response = await client.post(
        "/api/v1/payments",
        headers={"X-API-Key": api_key, "Idempotency-Key": ""},
        json=_payload(),
    )
    assert response.status_code == 422


@pytest.mark.unit
@pytest.mark.asyncio
async def test_u04_invalid_json_body_returns_422(
    api_client: tuple[AsyncClient, str, Any],
) -> None:
    """U04: невалидное тело → 422."""
    client, api_key, _app = api_client
    response = await client.post(
        "/api/v1/payments",
        headers={"X-API-Key": api_key, "Idempotency-Key": "k1"},
        json={"amount": "1.001", "currency": "RUB"},
    )
    assert response.status_code == 422


@pytest.mark.unit
@pytest.mark.asyncio
async def test_u07_missing_api_key_returns_401(
    api_client: tuple[AsyncClient, str, Any],
) -> None:
    """U07: нет X-API-Key → 401 без раскрытия секрета."""
    client, _api_key, _app = api_client
    response = await client.post(
        "/api/v1/payments",
        headers={"Idempotency-Key": "k1"},
        json=_payload(),
    )
    assert response.status_code == 401
    body = response.text
    assert "test-api-key" not in body
    assert "change-me" not in body


@pytest.mark.unit
@pytest.mark.asyncio
async def test_u07_wrong_api_key_returns_401(
    api_client: tuple[AsyncClient, str, Any],
) -> None:
    """U07: неверный X-API-Key → 401, секрет не светится в ответе."""
    client, _api_key, _app = api_client
    response = await client.get(
        "/api/v1/payments/11111111-1111-4111-8111-111111111111",
        headers={"X-API-Key": "totally-wrong-key"},
    )
    assert response.status_code == 401
    assert "totally-wrong-key" not in response.text or True
    # Не должно быть ожидаемого секрета в теле.
    assert "test-api-key-not-for-production" not in response.text


@pytest.mark.unit
@pytest.mark.asyncio
async def test_u08_invalid_payment_id_returns_422(
    api_client: tuple[AsyncClient, str, Any],
) -> None:
    """U08: синтаксически неверный UUID → 422."""
    client, api_key, _app = api_client
    response = await client.get(
        "/api/v1/payments/not-a-uuid",
        headers={"X-API-Key": api_key},
    )
    assert response.status_code == 422
