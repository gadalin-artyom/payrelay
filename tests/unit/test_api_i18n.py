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
    INTERNAL_SERVER_ERROR,
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
async def test_401_russian_detail(
    api_client: tuple[AsyncClient, str, Any],
) -> None:
    """401 возвращает русское сообщение без секрета."""
    client, _api_key, _app = api_client
    response = await client.get("/health/live")
    assert response.status_code == status.HTTP_401_UNAUTHORIZED
    assert response.json()["detail"] == API_KEY_INVALID
    assert "test-api-key" not in response.text


@pytest.mark.unit
@pytest.mark.asyncio
async def test_422_missing_idempotency_key_russian(
    api_client: tuple[AsyncClient, str, Any],
) -> None:
    """422 без Idempotency-Key — русский msg, формат списка detail."""
    client, api_key, _app = api_client
    response = await client.post(
        "/api/v1/payments",
        headers={"X-API-Key": api_key},
        json=_payload(),
    )
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    detail = response.json()["detail"]
    assert isinstance(detail, list)
    assert detail[0]["loc"] == ["header", "Idempotency-Key"]
    assert detail[0]["type"] == "missing"
    assert detail[0]["msg"] == "Обязательный заголовок Idempotency-Key не указан."


@pytest.mark.unit
@pytest.mark.asyncio
async def test_422_invalid_idempotency_key_russian(
    api_client: tuple[AsyncClient, str, Any],
) -> None:
    """422 при некорректном Idempotency-Key — русское сообщение."""
    client, api_key, _app = api_client
    response = await client.post(
        "/api/v1/payments",
        headers={"X-API-Key": api_key, "Idempotency-Key": " bad "},
        json=_payload(),
    )
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    detail = response.json()["detail"]
    assert detail[0]["type"] == "value_error"
    assert "Некорректный заголовок Idempotency-Key" in detail[0]["msg"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_422_amount_and_currency_russian(
    api_client: tuple[AsyncClient, str, Any],
) -> None:
    """422 для суммы и валюты содержит русские сообщения."""
    client, api_key, _app = api_client
    amount_resp = await client.post(
        "/api/v1/payments",
        headers={"X-API-Key": api_key, "Idempotency-Key": "k-amount"},
        json=_payload(amount="1.001"),
    )
    assert amount_resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    amount_msgs = " ".join(item["msg"] for item in amount_resp.json()["detail"])
    assert "Сумма должна быть положительным числом" in amount_msgs

    currency_resp = await client.post(
        "/api/v1/payments",
        headers={"X-API-Key": api_key, "Idempotency-Key": "k-currency"},
        json=_payload(currency="GBP"),
    )
    assert currency_resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    currency_msgs = " ".join(item["msg"] for item in currency_resp.json()["detail"])
    assert "Допустимые валюты: RUB, USD, EUR." in currency_msgs


@pytest.mark.unit
@pytest.mark.asyncio
async def test_422_webhook_and_uuid_russian(
    api_client: tuple[AsyncClient, str, Any],
) -> None:
    """422 для webhook_url и невалидного UUID — на русском."""
    client, api_key, _app = api_client
    webhook_resp = await client.post(
        "/api/v1/payments",
        headers={"X-API-Key": api_key, "Idempotency-Key": "k-hook"},
        json=_payload(webhook_url="ftp://example.com/x"),
    )
    assert webhook_resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    webhook_msgs = " ".join(item["msg"] for item in webhook_resp.json()["detail"])
    assert "HTTP или HTTPS" in webhook_msgs

    uuid_resp = await client.get(
        "/api/v1/payments/not-a-uuid",
        headers={"X-API-Key": api_key},
    )
    assert uuid_resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    uuid_detail = uuid_resp.json()["detail"]
    assert any(item["type"] == "uuid_parsing" for item in uuid_detail)
    assert any("UUID" in item["msg"] for item in uuid_detail)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_404_and_409_russian(
    api_client: tuple[AsyncClient, str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """404 и 409 возвращают русские detail-строки."""
    client, api_key, app = api_client
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
        headers={"X-API-Key": api_key, "Idempotency-Key": "k-conflict"},
        json=_payload(),
    )
    assert conflict.status_code == status.HTTP_409_CONFLICT
    assert conflict.json()["detail"] == IDEMPOTENCY_CONFLICT
    assert app is not None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_500_russian_safe_detail(
    test_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """500 не раскрывает инфраструктурные детали.

    ServerErrorMiddleware после ответа снова поднимает исходное исключение
    (для логирования сервером); ASGITransport с raise_app_exceptions=False
    позволяет проверить именно HTTP-тело.
    """
    app = create_app(test_settings)
    fake_session = MagicMock(spec=AsyncSession)

    async def _override_session() -> AsyncIterator[AsyncSession]:
        yield fake_session

    app.dependency_overrides[get_db_session] = _override_session

    async def _boom(_self: Any, _payment_id: UUID) -> PaymentView:
        raise RuntimeError("secret-db-dsn postgresql://hidden")

    monkeypatch.setattr(
        "payrelay.api.routers.payments.PaymentService.get_payment",
        _boom,
    )
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    api_key = test_settings.api_key.get_secret_value()
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/api/v1/payments/11111111-1111-4111-8111-111111111111",
            headers={"X-API-Key": api_key},
        )
    app.dependency_overrides.clear()
    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert response.json()["detail"] == INTERNAL_SERVER_ERROR
    assert "postgresql://" not in response.text
    assert "hidden" not in response.text
