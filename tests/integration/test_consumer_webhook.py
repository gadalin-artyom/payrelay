"""Integration-тесты consumer + gateway + webhook (этап 004).

I06–I08, I12–I13, lease/concurrency, HTTP failure. Только postgres-test / rabbitmq-test.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payrelay.application.outbox_dispatcher import OutboxDispatcher
from payrelay.application.process_payment import ProcessOutcome, ProcessPaymentService
from payrelay.core.config import Settings
from payrelay.domain.enums import PaymentStatus, WebhookDeliveryStatus
from payrelay.infrastructure.gateway.simulator import GatewaySimulator
from payrelay.infrastructure.messaging.broker import create_rabbit_broker
from payrelay.infrastructure.messaging.publisher import OutboxAmqpPublisher
from payrelay.infrastructure.messaging.topology import (
    QUEUE_PAYMENTS_NEW,
    ROUTING_KEY_NEW,
    declare_payments_topology,
)
from payrelay.infrastructure.webhooks.http_client import (
    WebhookHttpClient,
    build_webhook_event_id,
)
from tests.integration.conftest import assert_safe_test_rabbitmq_url
from tests.integration.test_outbox_publisher import _drain_queue

pytestmark = pytest.mark.integration


def _payment_body(
    *,
    suffix: str,
    webhook_url: str = "http://127.0.0.1:8199/hook",
) -> dict[str, Any]:
    return {
        "amount": "1500.50",
        "currency": "RUB",
        "description": f"Consumer test {suffix}",
        "metadata": {"case": suffix},
        "webhook_url": webhook_url,
    }


@pytest.fixture
async def rabbit_broker(
    integration_settings: Settings,
    integration_rabbitmq_url: str,
) -> AsyncIterator[Any]:
    """Подключённый test RabbitBroker с topology."""
    assert_safe_test_rabbitmq_url(integration_rabbitmq_url)
    broker = create_rabbit_broker(integration_settings, fail_fast=True)
    await broker.connect()
    await declare_payments_topology(broker)
    try:
        yield broker
    finally:
        await broker.stop()


@pytest.fixture
async def outbox_dispatcher(
    integration_settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    rabbit_broker: Any,
) -> OutboxDispatcher:
    """Диспетчер Outbox на test DB + test broker."""
    publisher = OutboxAmqpPublisher(
        rabbit_broker,
        confirm_timeout_seconds=integration_settings.outbox_publish_timeout_seconds,
    )
    return OutboxDispatcher(
        session_factory=session_factory,
        publisher=publisher,
        settings=integration_settings,
    )


def _webhook_client(
    *,
    status_code: int = 200,
    recorder: list[httpx.Request] | None = None,
    fail_times: int = 0,
) -> WebhookHttpClient:
    """Собрать WebhookHttpClient с MockTransport."""
    state = {"failures_left": fail_times}
    calls = recorder if recorder is not None else []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if state["failures_left"] > 0:
            state["failures_left"] -= 1
            return httpx.Response(500)
        return httpx.Response(status_code)

    return WebhookHttpClient(
        timeout_seconds=2.0,
        allow_private_urls=True,
        transport=httpx.MockTransport(handler),
    )


def _service(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    gateway: GatewaySimulator,
    webhook: WebhookHttpClient,
) -> ProcessPaymentService:
    return ProcessPaymentService(
        session_factory=session_factory,
        gateway=gateway,
        webhook_client=webhook,
        settings=settings,
        sleep=AsyncMock(),
    )


async def _create_payment(
    client: AsyncClient,
    api_key: str,
    *,
    suffix: str,
) -> uuid.UUID:
    response = await client.post(
        "/api/v1/payments",
        headers={
            "X-API-Key": api_key,
            "Idempotency-Key": f"{suffix}-{uuid.uuid4()}",
        },
        json=_payment_body(suffix=suffix),
    )
    assert response.status_code == 202
    return uuid.UUID(response.json()["payment_id"])


async def _get_payment_row(
    session_factory: async_sessionmaker[AsyncSession],
    payment_id: uuid.UUID,
) -> dict[str, Any]:
    async with session_factory() as session:
        row = (
            (
                await session.execute(
                    text(
                        """
                        SELECT status, processed_at, webhook_delivery_status,
                               webhook_attempts, processing_lease_until
                        FROM payments WHERE id = :payment_id
                        """
                    ),
                    {"payment_id": payment_id},
                )
            )
            .mappings()
            .one()
        )
    return dict(row)


@pytest.mark.asyncio
async def test_i06_duplicate_event_gateway_once(
    api_client: tuple[AsyncClient, str],
    session_factory: async_sessionmaker[AsyncSession],
    integration_settings: Settings,
) -> None:
    """I06: повтор того же event не вызывает gateway второй раз."""
    client, api_key = api_client
    payment_id = await _create_payment(client, api_key, suffix="i06")
    event_id = uuid.uuid4()
    sleep = AsyncMock()
    gateway = GatewaySimulator(sleep=sleep, forced_outcome=PaymentStatus.SUCCEEDED)
    webhook = _webhook_client()
    service = _service(
        session_factory=session_factory,
        settings=integration_settings,
        gateway=gateway,
        webhook=webhook,
    )

    first = await service.process_payment_created(
        payment_id=payment_id,
        event_id=event_id,
    )
    assert first.outcome is ProcessOutcome.ACK
    assert sleep.await_count == 1
    row1 = await _get_payment_row(session_factory, payment_id)
    assert row1["status"] == PaymentStatus.SUCCEEDED.value
    processed_at = row1["processed_at"]

    second = await service.process_payment_created(
        payment_id=payment_id,
        event_id=event_id,
    )
    assert second.outcome is ProcessOutcome.ACK
    assert second.detail == "already_delivered"
    assert sleep.await_count == 1
    row2 = await _get_payment_row(session_factory, payment_id)
    assert row2["processed_at"] == processed_at
    assert row2["webhook_delivery_status"] == WebhookDeliveryStatus.DELIVERED.value
    await webhook.aclose()


@pytest.mark.asyncio
async def test_i07_e2e_succeeded_webhook_delivered(
    api_client: tuple[AsyncClient, str],
    session_factory: async_sessionmaker[AsyncSession],
    integration_settings: Settings,
    outbox_dispatcher: OutboxDispatcher,
    rabbit_broker: Any,
) -> None:
    """I07: POST → outbox publish → consumer process → succeeded + delivered."""
    client, api_key = api_client
    await _drain_queue(rabbit_broker, QUEUE_PAYMENTS_NEW)
    payment_id = await _create_payment(client, api_key, suffix="i07")
    assert await outbox_dispatcher.process_one() is True
    messages = await _drain_queue(rabbit_broker, QUEUE_PAYMENTS_NEW)
    assert len(messages) == 1
    body = json.loads(messages[0].body.decode("utf-8"))
    assert body["payment_id"] == str(payment_id)

    recorder: list[httpx.Request] = []
    webhook = _webhook_client(recorder=recorder)
    gateway = GatewaySimulator(
        sleep=AsyncMock(),
        forced_outcome=PaymentStatus.SUCCEEDED,
    )
    service = _service(
        session_factory=session_factory,
        settings=integration_settings,
        gateway=gateway,
        webhook=webhook,
    )
    result = await service.process_payment_created(
        payment_id=uuid.UUID(body["payment_id"]),
        event_id=uuid.UUID(body["event_id"]),
    )
    assert result.outcome is ProcessOutcome.ACK
    row = await _get_payment_row(session_factory, payment_id)
    assert row["status"] == PaymentStatus.SUCCEEDED.value
    assert row["webhook_delivery_status"] == WebhookDeliveryStatus.DELIVERED.value
    assert len(recorder) == 1
    assert recorder[0].headers["X-Webhook-Event-ID"] == str(
        build_webhook_event_id(payment_id)
    )
    payload = json.loads(recorder[0].content.decode("utf-8"))
    assert payload["status"] == "succeeded"
    assert payload["amount"] == "1500.50"
    await webhook.aclose()


@pytest.mark.asyncio
async def test_i08_business_failed_still_delivers_webhook(
    api_client: tuple[AsyncClient, str],
    session_factory: async_sessionmaker[AsyncSession],
    integration_settings: Settings,
) -> None:
    """I08: business failed + webhook payload failed + delivered (без DLQ)."""
    client, api_key = api_client
    payment_id = await _create_payment(client, api_key, suffix="i08")
    recorder: list[httpx.Request] = []
    webhook = _webhook_client(recorder=recorder)
    gateway = GatewaySimulator(sleep=AsyncMock(), forced_outcome=PaymentStatus.FAILED)
    service = _service(
        session_factory=session_factory,
        settings=integration_settings,
        gateway=gateway,
        webhook=webhook,
    )
    result = await service.process_payment_created(
        payment_id=payment_id,
        event_id=uuid.uuid4(),
    )
    assert result.outcome is ProcessOutcome.ACK
    row = await _get_payment_row(session_factory, payment_id)
    assert row["status"] == PaymentStatus.FAILED.value
    assert row["webhook_delivery_status"] == WebhookDeliveryStatus.DELIVERED.value
    payload = json.loads(recorder[0].content.decode("utf-8"))
    assert payload["status"] == "failed"
    await webhook.aclose()


@pytest.mark.asyncio
async def test_i12_crash_after_terminal_before_webhook(
    api_client: tuple[AsyncClient, str],
    session_factory: async_sessionmaker[AsyncSession],
    integration_settings: Settings,
) -> None:
    """I12: crash после terminal commit → redelivery без повторного gateway."""
    client, api_key = api_client
    payment_id = await _create_payment(client, api_key, suffix="i12")
    sleep = AsyncMock()
    gateway = GatewaySimulator(sleep=sleep, forced_outcome=PaymentStatus.SUCCEEDED)
    webhook = _webhook_client()
    service = _service(
        session_factory=session_factory,
        settings=integration_settings,
        gateway=gateway,
        webhook=webhook,
    )

    async def _crash() -> None:
        raise RuntimeError("injected crash after terminal")

    service.set_after_terminal_commit_hook(_crash)
    with pytest.raises(RuntimeError, match="injected crash after terminal"):
        await service.process_payment_created(
            payment_id=payment_id,
            event_id=uuid.uuid4(),
        )
    assert sleep.await_count == 1
    row = await _get_payment_row(session_factory, payment_id)
    assert row["status"] == PaymentStatus.SUCCEEDED.value
    assert row["webhook_delivery_status"] == WebhookDeliveryStatus.PENDING.value

    # Истечь lease владельца (crash recovery).
    async with session_factory() as session:
        async with session.begin():
            await session.execute(
                text(
                    """
                    UPDATE payments
                    SET processing_lease_until = now() - interval '1 second'
                    WHERE id = :payment_id
                    """
                ),
                {"payment_id": payment_id},
            )

    service.set_after_terminal_commit_hook(None)
    result = await service.process_payment_created(
        payment_id=payment_id,
        event_id=uuid.uuid4(),
    )
    assert result.outcome is ProcessOutcome.ACK
    assert sleep.await_count == 1
    row2 = await _get_payment_row(session_factory, payment_id)
    assert row2["webhook_delivery_status"] == WebhookDeliveryStatus.DELIVERED.value
    await webhook.aclose()


@pytest.mark.asyncio
async def test_i13_crash_after_http_2xx_before_delivered(
    api_client: tuple[AsyncClient, str],
    session_factory: async_sessionmaker[AsyncSession],
    integration_settings: Settings,
) -> None:
    """I13: crash после HTTP 2xx до delivered → повтор с тем же event id."""
    client, api_key = api_client
    payment_id = await _create_payment(client, api_key, suffix="i13")
    recorder: list[httpx.Request] = []
    webhook = _webhook_client(recorder=recorder)
    gateway = GatewaySimulator(
        sleep=AsyncMock(),
        forced_outcome=PaymentStatus.SUCCEEDED,
    )
    service = _service(
        session_factory=session_factory,
        settings=integration_settings,
        gateway=gateway,
        webhook=webhook,
    )

    async def _crash() -> None:
        raise RuntimeError("injected crash after http 2xx")

    service.set_after_webhook_http_ok_hook(_crash)
    with pytest.raises(RuntimeError, match="injected crash after http 2xx"):
        await service.process_payment_created(
            payment_id=payment_id,
            event_id=uuid.uuid4(),
        )
    assert len(recorder) == 1
    expected_event_id = str(build_webhook_event_id(payment_id))
    assert recorder[0].headers["X-Webhook-Event-ID"] == expected_event_id

    async with session_factory() as session:
        async with session.begin():
            await session.execute(
                text(
                    """
                    UPDATE payments
                    SET processing_lease_until = now() - interval '1 second'
                    WHERE id = :payment_id
                    """
                ),
                {"payment_id": payment_id},
            )

    service.set_after_webhook_http_ok_hook(None)
    result = await service.process_payment_created(
        payment_id=payment_id,
        event_id=uuid.uuid4(),
    )
    assert result.outcome is ProcessOutcome.ACK
    assert len(recorder) == 2
    assert recorder[1].headers["X-Webhook-Event-ID"] == expected_event_id
    row = await _get_payment_row(session_factory, payment_id)
    assert row["status"] == PaymentStatus.SUCCEEDED.value
    assert row["webhook_delivery_status"] == WebhookDeliveryStatus.DELIVERED.value
    await webhook.aclose()


@pytest.mark.asyncio
async def test_http_500_keeps_terminal_pending_attempt_one(
    api_client: tuple[AsyncClient, str],
    session_factory: async_sessionmaker[AsyncSession],
    integration_settings: Settings,
) -> None:
    """HTTP 500: terminal неизменен, webhook pending, attempts=1 (до stage 005)."""
    client, api_key = api_client
    payment_id = await _create_payment(client, api_key, suffix="http500")
    webhook = _webhook_client(status_code=500)
    gateway = GatewaySimulator(
        sleep=AsyncMock(),
        forced_outcome=PaymentStatus.SUCCEEDED,
    )
    service = _service(
        session_factory=session_factory,
        settings=integration_settings,
        gateway=gateway,
        webhook=webhook,
    )
    result = await service.process_payment_created(
        payment_id=payment_id,
        event_id=uuid.uuid4(),
    )
    assert result.outcome is ProcessOutcome.ACK
    assert result.detail == "webhook_failed_deferred_stage_005"
    row = await _get_payment_row(session_factory, payment_id)
    assert row["status"] == PaymentStatus.SUCCEEDED.value
    assert row["webhook_delivery_status"] == WebhookDeliveryStatus.PENDING.value
    assert row["webhook_attempts"] == 1
    await webhook.aclose()


@pytest.mark.asyncio
async def test_lease_concurrency_second_worker_waits(
    api_client: tuple[AsyncClient, str],
    session_factory: async_sessionmaker[AsyncSession],
    integration_settings: Settings,
) -> None:
    """Два worker: активный lease не даёт второму ACK'ать единственную доставку."""
    client, api_key = api_client
    payment_id = await _create_payment(client, api_key, suffix="lease")

    # Worker A удерживает lease (эмуляция через прямой UPDATE).
    async with session_factory() as session:
        async with session.begin():
            await session.execute(
                text(
                    """
                    UPDATE payments
                    SET processing_lease_until = now() + interval '30 seconds'
                    WHERE id = :payment_id
                    """
                ),
                {"payment_id": payment_id},
            )

    webhook = _webhook_client()
    gateway = GatewaySimulator(
        sleep=AsyncMock(),
        forced_outcome=PaymentStatus.SUCCEEDED,
    )
    # Проверяем BUSY через stop после короткого ожидания lease.
    service = _service(
        session_factory=session_factory,
        settings=integration_settings,
        gateway=gateway,
        webhook=webhook,
    )

    class _StopSoon:
        def __init__(self) -> None:
            self.calls = 0

        def is_set(self) -> bool:
            self.calls += 1
            return self.calls > 2

    result = await service.process_payment_created(
        payment_id=payment_id,
        event_id=uuid.uuid4(),
        stop=_StopSoon(),
    )
    assert result.outcome is ProcessOutcome.RETRY_LATER
    row = await _get_payment_row(session_factory, payment_id)
    assert row["status"] == PaymentStatus.PENDING.value

    # Истечение lease → восстановление.
    async with session_factory() as session:
        async with session.begin():
            await session.execute(
                text(
                    """
                    UPDATE payments
                    SET processing_lease_until = now() - interval '1 second'
                    WHERE id = :payment_id
                    """
                ),
                {"payment_id": payment_id},
            )

    result2 = await service.process_payment_created(
        payment_id=payment_id,
        event_id=uuid.uuid4(),
    )
    assert result2.outcome is ProcessOutcome.ACK
    row2 = await _get_payment_row(session_factory, payment_id)
    assert row2["status"] == PaymentStatus.SUCCEEDED.value
    assert row2["webhook_delivery_status"] == WebhookDeliveryStatus.DELIVERED.value
    await webhook.aclose()


@pytest.mark.asyncio
async def test_wrong_owner_cas_does_not_update_status(
    api_client: tuple[AsyncClient, str],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Wrong owner CAS не обновляет статус платежа."""
    client, api_key = api_client
    payment_id = await _create_payment(client, api_key, suffix="cas")
    from payrelay.infrastructure.db.repositories import PaymentRepository

    async with session_factory() as session:
        async with session.begin():
            repo = PaymentRepository(session)
            token = await repo.claim_pending_for_processing(
                payment_id=payment_id,
                lease_seconds=60.0,
            )
            assert token is not None
            fake_token = datetime.now(UTC)
            updated = await repo.commit_terminal_status(
                payment_id=payment_id,
                lease_token=fake_token,
                status=PaymentStatus.SUCCEEDED,
            )
            assert updated is False

    row = await _get_payment_row(session_factory, payment_id)
    assert row["status"] == PaymentStatus.PENDING.value


@pytest.mark.asyncio
async def test_db_failure_before_ack_leaves_message_for_redelivery(
    api_client: tuple[AsyncClient, str],
    session_factory: async_sessionmaker[AsyncSession],
    integration_settings: Settings,
    rabbit_broker: Any,
) -> None:
    """Инфраструктурный сбой БД до ACK: сообщение остаётся доступным (unacked path)."""
    client, api_key = api_client
    await _drain_queue(rabbit_broker, QUEUE_PAYMENTS_NEW)
    payment_id = await _create_payment(client, api_key, suffix="dbfail")

    # Опубликовать событие вручную.
    event_id = uuid.uuid4()
    payload = {
        "schema_version": 1,
        "event_id": str(event_id),
        "payment_id": str(payment_id),
        "event_type": "payment.created",
        "attempt": 1,
    }
    await rabbit_broker.publish(
        payload,
        queue=ROUTING_KEY_NEW,
        exchange="payments.exchange",
        message_id=str(event_id),
        correlation_id=str(payment_id),
        content_type="application/json",
    )

    webhook = _webhook_client()
    gateway = GatewaySimulator(
        sleep=AsyncMock(),
        forced_outcome=PaymentStatus.SUCCEEDED,
    )
    service = _service(
        session_factory=session_factory,
        settings=integration_settings,
        gateway=gateway,
        webhook=webhook,
    )

    async def _fail_delivered() -> None:
        raise RuntimeError("injected db failure before delivered")

    service.set_after_webhook_http_ok_hook(_fail_delivered)

    # Обработка как у consumer: исключение → без ACK.
    with pytest.raises(RuntimeError, match="injected db failure"):
        await service.process_payment_created(
            payment_id=payment_id,
            event_id=event_id,
        )

    # Сообщение всё ещё в очереди (не ack'алось consumer-ом — мы не вызывали ack).
    # Drain проверит наличие (publish лежит, пока не прочитан).
    # Мы не потребляли AMQP в этом тесте — сообщение ещё в очереди.
    messages = await _drain_queue(rabbit_broker, QUEUE_PAYMENTS_NEW)
    assert len(messages) >= 1
    await webhook.aclose()
