"""Unit-тесты технического маршрута /health/live."""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from payrelay.api.app import create_app
from payrelay.core.config import Settings


@pytest.mark.unit
@pytest.mark.asyncio
async def test_health_live_requires_api_key(test_settings: Settings) -> None:
    """Без X-API-Key маршрут должен отвечать 401."""
    app = create_app(test_settings)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health/live")
    assert response.status_code == 401


@pytest.mark.unit
@pytest.mark.asyncio
async def test_health_live_rejects_wrong_api_key(test_settings: Settings) -> None:
    """Неверный X-API-Key должен давать 401."""
    app = create_app(test_settings)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/health/live",
            headers={"X-API-Key": "wrong-key"},
        )
    assert response.status_code == 401


@pytest.mark.unit
@pytest.mark.asyncio
async def test_health_live_ok_with_valid_api_key(
    test_settings: Settings,
    api_key: str,
) -> None:
    """Валидный X-API-Key возвращает status=ok."""
    app = create_app(test_settings)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/health/live",
            headers={"X-API-Key": api_key},
        )
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.unit
def test_docs_disabled_in_test_settings(test_settings: Settings) -> None:
    """В test/production OpenAPI/Swagger должны быть отключены по умолчанию."""
    app = create_app(test_settings)
    assert app.docs_url is None
    assert app.openapi_url is None
