from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, patch
from uuid import UUID, uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payrelay.api.app import create_app
from payrelay.core.config import Settings
from payrelay.infrastructure.db.models import OutboxModel, PaymentModel
from payrelay.infrastructure.db.repositories import OutboxRepository
from tests.integration.conftest import assert_safe_test_database_url


def _payload(**overrides: object) -> dict[str, object]:
    """Валидное тело создания платежа.

    Args:
        **overrides: Переопределения полей.

    Returns:
        JSON-тело.
    """
    body: dict[str, object] = {
        "amount": "1500.50",
        "currency": "RUB",
        "description": "Заказ #123",
        "metadata": {"order_id": "123", "nested": {"b": 2, "a": 1}},
        "webhook_url": "https://merchant.example.com/payments/callback",
    }
    body.update(overrides)
    return body


@pytest.mark.integration
@pytest.mark.asyncio
async def test_i02_post_creates_payment_and_outbox_atomically(
    api_client: tuple[AsyncClient, str],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """I02: POST 202, GET 200; одна payment + одна outbox с нужным payload."""
    client, api_key = api_client
    response = await client.post(
        "/api/v1/payments",
        headers={"X-API-Key": api_key, "Idempotency-Key": "idem-i02"},
        json=_payload(),
    )
    assert response.status_code == 202
    data = response.json()
    payment_id = UUID(data["payment_id"])
    assert data["status"] == "pending"
    assert data["created_at"].endswith("Z") or "+" in data["created_at"]

    get_resp = await client.get(
        f"/api/v1/payments/{payment_id}",
        headers={"X-API-Key": api_key},
    )
    assert get_resp.status_code == 200
    body = get_resp.json()
    assert body["payment_id"] == str(payment_id)
    assert body["amount"] == "1500.50"
    assert body["currency"] == "RUB"
    assert body["status"] == "pending"
    assert body["webhook_delivery_status"] == "pending"
    assert body["processed_at"] is None

    async with session_factory() as session:
        payments_count = await session.scalar(
            select(func.count()).select_from(PaymentModel)
        )
        outbox_count = await session.scalar(
            select(func.count()).select_from(OutboxModel)
        )
        payment = await session.scalar(
            select(PaymentModel).where(PaymentModel.id == payment_id)
        )
        outbox = await session.scalar(
            select(OutboxModel).where(OutboxModel.aggregate_id == payment_id)
        )

    assert payments_count == 1
    assert outbox_count == 1
    assert payment is not None
    assert payment.created_at.tzinfo is not None
    assert outbox is not None
    assert outbox.published_at is None
    assert outbox.attempts == 0
    assert outbox.delivery_attempt == 1
    assert outbox.event_type == "payment.created"
    assert outbox.payload == {
        "schema_version": 1,
        "event_id": str(outbox.id),
        "payment_id": str(payment_id),
        "event_type": "payment.created",
        "attempt": 1,
    }


@pytest.mark.integration
@pytest.mark.asyncio
async def test_i03_concurrent_idempotent_posts(
    api_client: tuple[AsyncClient, str],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """I03: 20 конкурентных POST → один payment_id и одна outbox; иной body → 409."""
    client, api_key = api_client
    headers = {"X-API-Key": api_key, "Idempotency-Key": "idem-concurrent"}
    payload = _payload()

    async def _post() -> Any:
        return await client.post(
            "/api/v1/payments",
            headers=headers,
            json=payload,
        )

    responses = await asyncio.gather(*[_post() for _ in range(20)])
    assert all(resp.status_code == 202 for resp in responses)
    payment_ids = {resp.json()["payment_id"] for resp in responses}
    assert len(payment_ids) == 1

    async with session_factory() as session:
        payments_count = await session.scalar(
            select(func.count()).select_from(PaymentModel)
        )
        outbox_count = await session.scalar(
            select(func.count()).select_from(OutboxModel)
        )

    assert payments_count == 1
    assert outbox_count == 1

    conflict = await client.post(
        "/api/v1/payments",
        headers=headers,
        json=_payload(amount="1500.51"),
    )
    assert conflict.status_code == 409

    async with session_factory() as session:
        payments_count_after = await session.scalar(
            select(func.count()).select_from(PaymentModel)
        )
        outbox_count_after = await session.scalar(
            select(func.count()).select_from(OutboxModel)
        )

    assert payments_count_after == 1
    assert outbox_count_after == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_outbox_insert_failure_rolls_back_payment(
    integration_settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    clean_tables: None,
    integration_database_url: str,
) -> None:
    """При ошибке insert outbox транзакция откатывает и payment."""
    assert_safe_test_database_url(integration_database_url)
    app = create_app(integration_settings)
    api_key = integration_settings.api_key.get_secret_value()

    with patch.object(
        OutboxRepository,
        "insert_payment_created",
        new_callable=AsyncMock,
        side_effect=RuntimeError("simulated outbox failure"),
    ):
        async with app.router.lifespan_context(app):
            transport = ASGITransport(
                app=app,
                raise_app_exceptions=False,
            )
            async with AsyncClient(
                transport=transport,
                base_url="http://test",
            ) as client:
                response = await client.post(
                    "/api/v1/payments",
                    headers={
                        "X-API-Key": api_key,
                        "Idempotency-Key": "idem-rollback",
                    },
                    json=_payload(),
                )

    assert response.status_code == 500

    async with session_factory() as session:
        payments_count = await session.scalar(
            select(func.count()).select_from(PaymentModel)
        )
        outbox_count = await session.scalar(
            select(func.count()).select_from(OutboxModel)
        )

    assert payments_count == 0
    assert outbox_count == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_unknown_uuid_returns_404(
    api_client: tuple[AsyncClient, str],
) -> None:
    """Неизвестный UUID → 404."""
    client, api_key = api_client
    response = await client.get(
        f"/api/v1/payments/{uuid4()}",
        headers={"X-API-Key": api_key},
    )
    assert response.status_code == 404


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_requires_api_key(api_client: tuple[AsyncClient, str]) -> None:
    """GET без X-API-Key → 401."""
    client, _api_key = api_client
    response = await client.get(f"/api/v1/payments/{uuid4()}")
    assert response.status_code == 401


@pytest.mark.integration
@pytest.mark.asyncio
async def test_idempotent_replay_with_equivalent_metadata_order(
    api_client: tuple[AsyncClient, str],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Повтор с семантически тем же metadata (другой порядок ключей) → 202 тот же id."""
    client, api_key = api_client
    headers = {"X-API-Key": api_key, "Idempotency-Key": "idem-meta-order"}
    first = await client.post(
        "/api/v1/payments",
        headers=headers,
        json=_payload(metadata={"b": 2, "a": 1}),
    )
    second = await client.post(
        "/api/v1/payments",
        headers=headers,
        json=_payload(metadata={"a": 1, "b": 2}),
    )
    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()["payment_id"] == second.json()["payment_id"]
    assert first.json()["created_at"] == second.json()["created_at"]

    async with session_factory() as session:
        payments_count = await session.scalar(
            select(func.count()).select_from(PaymentModel)
        )
        outbox_count = await session.scalar(
            select(func.count()).select_from(OutboxModel)
        )

    assert payments_count == 1
    assert outbox_count == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_idempotent_replay_returns_current_status(
    api_client: tuple[AsyncClient, str],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Повтор POST всегда 202 и отдаёт актуальный status сохранённого платежа."""
    client, api_key = api_client
    headers = {"X-API-Key": api_key, "Idempotency-Key": "idem-status"}
    created = await client.post(
        "/api/v1/payments",
        headers=headers,
        json=_payload(),
    )
    assert created.status_code == 202
    payment_id = UUID(created.json()["payment_id"])

    async with session_factory() as session:
        async with session.begin():
            await session.execute(
                text("UPDATE payments SET status = 'succeeded' WHERE id = :id"),
                {"id": payment_id},
            )

    replay = await client.post(
        "/api/v1/payments",
        headers=headers,
        json=_payload(),
    )
    assert replay.status_code == 202
    assert replay.json()["payment_id"] == str(payment_id)
    assert replay.json()["status"] == "succeeded"
    assert replay.json()["created_at"] == created.json()["created_at"]
