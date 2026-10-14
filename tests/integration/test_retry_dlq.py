from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest
from aio_pika import IncomingMessage
from faststream.rabbit import RabbitBroker, RabbitQueue
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payrelay.application.outbox_dispatcher import OutboxDispatcher
from payrelay.application.process_payment import ProcessOutcome, ProcessPaymentService
from payrelay.core.config import Settings
from payrelay.domain.enums import OutboxEventType, PaymentStatus, WebhookDeliveryStatus
from payrelay.domain.webhook_retry_rules import SYSTEM_ATTEMPT_HEADER
from payrelay.infrastructure.gateway.simulator import GatewaySimulator
from payrelay.infrastructure.messaging.broker import create_rabbit_broker
from payrelay.infrastructure.messaging.publisher import (
    OutboxAmqpPublisher,
    OutboxPublishError,
)
from payrelay.infrastructure.messaging.system_retry import publish_system_retry
from payrelay.infrastructure.messaging.topology import (
    QUEUE_DLQ,
    QUEUE_PAYMENTS_NEW,
    QUEUE_RETRY_2,
    QUEUE_RETRY_3,
    declare_payments_topology,
)
from payrelay.infrastructure.webhooks.http_client import WebhookHttpClient
from tests.integration.conftest import assert_safe_test_rabbitmq_url
from tests.integration.test_outbox_publisher import _drain_queue

pytestmark = pytest.mark.integration


def _payment_body(*, suffix: str) -> dict[str, Any]:
    return {
        "amount": "1500.50",
        "currency": "RUB",
        "description": f"Retry test {suffix}",
        "metadata": {"case": suffix},
        "webhook_url": "http://127.0.0.1:8199/hook",
    }


@pytest.fixture
async def rabbit_broker(
    integration_settings: Settings,
    integration_rabbitmq_url: str,
) -> AsyncIterator[RabbitBroker]:
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
    rabbit_broker: RabbitBroker,
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


async def _get_retry_outbox(
    session_factory: async_sessionmaker[AsyncSession],
    payment_id: uuid.UUID,
    delivery_attempt: int,
) -> dict[str, Any] | None:
    async with session_factory() as session:
        row = (
            (
                await session.execute(
                    text(
                        """
                        SELECT id, delivery_attempt, published_at,
                               EXTRACT(EPOCH FROM (available_at - created_at))
                                 AS delay_from_created
                        FROM outbox
                        WHERE aggregate_id = :payment_id
                          AND event_type = :event_type
                          AND delivery_attempt = :delivery_attempt
                        """
                    ),
                    {
                        "payment_id": payment_id,
                        "event_type": OutboxEventType.PAYMENT_WEBHOOK_RETRY.value,
                        "delivery_attempt": delivery_attempt,
                    },
                )
            )
            .mappings()
            .first()
        )
    return dict(row) if row is not None else None


async def _get_one(
    broker: RabbitBroker,
    queue_name: str,
    *,
    wait_seconds: float = 2.0,
) -> IncomingMessage | None:
    """Прочитать одно сообщение без ack.

    Args:
        broker: Подключённый брокер.
        queue_name: Имя очереди.
        wait_seconds: Таймаут ожидания сообщения.

    Returns:
        Сообщение либо ``None``.
    """
    queue = await broker.declare_queue(
        RabbitQueue(queue_name, durable=True, declare=False)
    )
    return await queue.get(fail=False, timeout=wait_seconds)


@pytest.mark.asyncio
async def test_i09_three_webhook_failures_exhausted_and_dlq(
    api_client: tuple[AsyncClient, str],
    session_factory: async_sessionmaker[AsyncSession],
    integration_settings: Settings,
    rabbit_broker: RabbitBroker,
    outbox_dispatcher: OutboxDispatcher,
) -> None:
    """I09: три HTTP 500 → exhausted; финальный reject попадает в payments.dlq."""
    client, api_key = api_client
    payment_id = await _create_payment(client, api_key, suffix="i09")
    await _drain_queue(rabbit_broker, QUEUE_PAYMENTS_NEW)
    await _drain_queue(rabbit_broker, QUEUE_DLQ)
    # Опубликовать и снять исходный payment.created, чтобы в очереди
    # оставались только webhook_retry.
    await outbox_dispatcher.run_tick()
    await _drain_queue(rabbit_broker, QUEUE_PAYMENTS_NEW)

    recorder: list[httpx.Request] = []
    webhook = _webhook_client(status_code=500, recorder=recorder)
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

    # attempt 1
    r1 = await service.process_payment_created(
        payment_id=payment_id,
        event_id=uuid.uuid4(),
    )
    assert r1.outcome is ProcessOutcome.ACK
    retry2 = await _get_retry_outbox(session_factory, payment_id, 2)
    assert retry2 is not None
    assert 1.5 <= float(retry2["delay_from_created"]) <= 2.5

    # дождаться available_at и опубликовать retry2
    await asyncio.sleep(2.2)
    await outbox_dispatcher.run_tick()
    msg2 = await _get_one(rabbit_broker, QUEUE_PAYMENTS_NEW, wait_seconds=3.0)
    assert msg2 is not None
    body2 = json.loads(msg2.body)
    assert body2["attempt"] == 2
    assert body2["event_type"] == OutboxEventType.PAYMENT_WEBHOOK_RETRY.value
    r2 = await service.process_payment_event(
        payment_id=payment_id,
        event_id=uuid.UUID(body2["event_id"]),
        event_type=body2["event_type"],
        attempt=body2["attempt"],
    )
    assert r2.outcome is ProcessOutcome.ACK
    await msg2.ack()
    retry3 = await _get_retry_outbox(session_factory, payment_id, 3)
    assert retry3 is not None
    assert 3.5 <= float(retry3["delay_from_created"]) <= 4.5

    await asyncio.sleep(4.2)
    await outbox_dispatcher.run_tick()
    msg3 = await _get_one(rabbit_broker, QUEUE_PAYMENTS_NEW, wait_seconds=3.0)
    assert msg3 is not None
    body3 = json.loads(msg3.body)
    assert body3["attempt"] == 3
    assert body3["event_type"] == OutboxEventType.PAYMENT_WEBHOOK_RETRY.value
    r3 = await service.process_payment_event(
        payment_id=payment_id,
        event_id=uuid.UUID(body3["event_id"]),
        event_type=body3["event_type"],
        attempt=body3["attempt"],
    )
    assert r3.outcome is ProcessOutcome.REJECT
    assert r3.detail == "webhook_exhausted"
    # reject → DLQ через DLX очереди payments.new
    await msg3.reject(requeue=False)

    row = await _get_payment_row(session_factory, payment_id)
    assert row["status"] == PaymentStatus.SUCCEEDED.value
    assert row["webhook_delivery_status"] == WebhookDeliveryStatus.EXHAUSTED.value
    assert row["webhook_attempts"] == 3
    assert len(recorder) == 3

    dlq_msgs = await _drain_queue(rabbit_broker, QUEUE_DLQ, limit=10)
    assert any(
        json.loads(m.body).get("payment_id") == str(payment_id) for m in dlq_msgs
    )
    matched = next(
        m for m in dlq_msgs if json.loads(m.body).get("payment_id") == str(payment_id)
    )
    death = matched.headers.get("x-death") if matched.headers else None
    assert death is not None
    reasons: list[Any] = []
    for item in death:
        if isinstance(item, dict):
            reasons.append(item.get("reason") or item.get(b"reason"))
        else:
            reasons.append(item)
    assert any(r in ("rejected", b"rejected") for r in reasons)
    await webhook.aclose()


@pytest.mark.asyncio
async def test_i10_webhook_recovers_on_third_and_second_attempt(
    api_client: tuple[AsyncClient, str],
    session_factory: async_sessionmaker[AsyncSession],
    integration_settings: Settings,
) -> None:
    """I10: 500,500,200 → delivered; 500,200 за две попытки; failed+webhook OK."""
    client, api_key = api_client

    # 500, 500, 200
    payment_id = await _create_payment(client, api_key, suffix="i10a")
    recorder: list[httpx.Request] = []
    webhook = _webhook_client(fail_times=2, status_code=200, recorder=recorder)
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
    r1 = await service.process_payment_created(
        payment_id=payment_id, event_id=uuid.uuid4()
    )
    assert r1.detail == "webhook_failed_retry_scheduled"
    r2 = await service.process_payment_event(
        payment_id=payment_id,
        event_id=uuid.uuid4(),
        event_type=OutboxEventType.PAYMENT_WEBHOOK_RETRY.value,
        attempt=2,
    )
    assert r2.detail == "webhook_failed_retry_scheduled"
    r3 = await service.process_payment_event(
        payment_id=payment_id,
        event_id=uuid.uuid4(),
        event_type=OutboxEventType.PAYMENT_WEBHOOK_RETRY.value,
        attempt=3,
    )
    assert r3.outcome is ProcessOutcome.ACK
    assert r3.detail == "webhook_delivered"
    row = await _get_payment_row(session_factory, payment_id)
    assert row["webhook_delivery_status"] == WebhookDeliveryStatus.DELIVERED.value
    assert len(recorder) == 3
    await webhook.aclose()

    # 500 → 200 за две попытки
    payment_id2 = await _create_payment(client, api_key, suffix="i10b")
    recorder2: list[httpx.Request] = []
    webhook2 = _webhook_client(fail_times=1, status_code=200, recorder=recorder2)
    service2 = _service(
        session_factory=session_factory,
        settings=integration_settings,
        gateway=GatewaySimulator(
            sleep=AsyncMock(), forced_outcome=PaymentStatus.SUCCEEDED
        ),
        webhook=webhook2,
    )
    await service2.process_payment_created(
        payment_id=payment_id2, event_id=uuid.uuid4()
    )
    r = await service2.process_payment_event(
        payment_id=payment_id2,
        event_id=uuid.uuid4(),
        event_type=OutboxEventType.PAYMENT_WEBHOOK_RETRY.value,
        attempt=2,
    )
    assert r.detail == "webhook_delivered"
    assert len(recorder2) == 2
    await webhook2.aclose()

    # business failed + webhook success
    payment_id3 = await _create_payment(client, api_key, suffix="i10c")
    webhook3 = _webhook_client(status_code=200)
    service3 = _service(
        session_factory=session_factory,
        settings=integration_settings,
        gateway=GatewaySimulator(
            sleep=AsyncMock(), forced_outcome=PaymentStatus.FAILED
        ),
        webhook=webhook3,
    )
    result = await service3.process_payment_created(
        payment_id=payment_id3, event_id=uuid.uuid4()
    )
    assert result.outcome is ProcessOutcome.ACK
    row3 = await _get_payment_row(session_factory, payment_id3)
    assert row3["status"] == PaymentStatus.FAILED.value
    assert row3["webhook_delivery_status"] == WebhookDeliveryStatus.DELIVERED.value
    await webhook3.aclose()


@pytest.mark.asyncio
async def test_i14_system_retry_ttl_then_dlq(
    api_client: tuple[AsyncClient, str],
    session_factory: async_sessionmaker[AsyncSession],
    integration_settings: Settings,
    rabbit_broker: RabbitBroker,
) -> None:
    """I14: system_attempt 1→retry.2→2→retry.3→3→DLQ; без gateway на terminal."""
    client, api_key = api_client
    payment_id = await _create_payment(client, api_key, suffix="i14")
    await _drain_queue(rabbit_broker, QUEUE_PAYMENTS_NEW)
    await _drain_queue(rabbit_broker, QUEUE_RETRY_2)
    await _drain_queue(rabbit_broker, QUEUE_RETRY_3)
    await _drain_queue(rabbit_broker, QUEUE_DLQ)

    # Сначала довести платёж до terminal+delivered, затем проверить system path
    # на отдельном сообщении с тем же payment_id (gateway не должен вызваться).
    webhook = _webhook_client(status_code=200)
    sleep = AsyncMock()
    gateway = GatewaySimulator(sleep=sleep, forced_outcome=PaymentStatus.SUCCEEDED)
    service = _service(
        session_factory=session_factory,
        settings=integration_settings,
        gateway=gateway,
        webhook=webhook,
    )
    done = await service.process_payment_created(
        payment_id=payment_id, event_id=uuid.uuid4()
    )
    assert done.detail == "webhook_delivered"
    gateway_calls = sleep.await_count

    body = {
        "schema_version": 1,
        "event_id": str(uuid.uuid4()),
        "payment_id": str(payment_id),
        "event_type": OutboxEventType.PAYMENT_CREATED.value,
        "attempt": 1,
    }
    # system_attempt=1 → retry.2
    await publish_system_retry(
        rabbit_broker,
        body=body,
        current_system_attempt=1,
        message_id=body["event_id"],
        correlation_id=body["payment_id"],
        confirm_timeout_seconds=integration_settings.outbox_publish_timeout_seconds,
    )
    await asyncio.sleep(2.5)
    back = await _get_one(rabbit_broker, QUEUE_PAYMENTS_NEW, wait_seconds=3.0)
    assert back is not None
    assert back.headers.get(SYSTEM_ATTEMPT_HEADER) in (2, "2")
    await back.ack()

    # system_attempt=2 → retry.3
    await publish_system_retry(
        rabbit_broker,
        body=body,
        current_system_attempt=2,
        message_id=body["event_id"],
        correlation_id=body["payment_id"],
        confirm_timeout_seconds=integration_settings.outbox_publish_timeout_seconds,
    )
    await asyncio.sleep(4.5)
    back3 = await _get_one(rabbit_broker, QUEUE_PAYMENTS_NEW, wait_seconds=3.0)
    assert back3 is not None
    assert back3.headers.get(SYSTEM_ATTEMPT_HEADER) in (3, "3")
    # terminal payment: обработка ACK without gateway
    result = await service.process_payment_created(
        payment_id=payment_id,
        event_id=uuid.UUID(body["event_id"]),
    )
    assert result.outcome is ProcessOutcome.ACK
    assert sleep.await_count == gateway_calls
    await back3.reject(requeue=False)

    # DLX routing в payments.dlq не всегда мгновенный под нагрузкой suite.
    matched_dlq = False
    for _ in range(20):
        dlq = await _drain_queue(rabbit_broker, QUEUE_DLQ, limit=10)
        if any(json.loads(m.body).get("event_id") == body["event_id"] for m in dlq):
            matched_dlq = True
            break
        await asyncio.sleep(0.25)
    assert matched_dlq
    await webhook.aclose()


@pytest.mark.asyncio
async def test_legacy_recovery_pending_attempts_one_and_two(
    api_client: tuple[AsyncClient, str],
    session_factory: async_sessionmaker[AsyncSession],
    integration_settings: Settings,
    outbox_dispatcher: OutboxDispatcher,
) -> None:
    """Legacy: pending+attempts=1/2 без retry Outbox → recovery создаёт ровно один."""
    client, api_key = api_client
    payment_id = await _create_payment(client, api_key, suffix="rec1")

    # Эмулируем состояние этапа 004: terminal + pending + attempts=1, created ACK.
    async with session_factory() as session:
        async with session.begin():
            await session.execute(
                text(
                    """
                    UPDATE payments
                    SET status = :succeeded,
                        processed_at = now(),
                        webhook_delivery_status = :pending,
                        webhook_attempts = 1,
                        processing_lease_until = NULL
                    WHERE id = :payment_id
                    """
                ),
                {
                    "payment_id": payment_id,
                    "succeeded": PaymentStatus.SUCCEEDED.value,
                    "pending": WebhookDeliveryStatus.PENDING.value,
                },
            )

    inserted = await outbox_dispatcher.recover_pending_webhooks()
    assert inserted == 1
    retry2 = await _get_retry_outbox(session_factory, payment_id, 2)
    assert retry2 is not None
    # повторный tick не дублирует
    inserted2 = await outbox_dispatcher.recover_pending_webhooks()
    assert inserted2 == 0
    parallel = await asyncio.gather(
        outbox_dispatcher.recover_pending_webhooks(),
        outbox_dispatcher.recover_pending_webhooks(),
    )
    assert parallel == [0, 0]

    # attempts=2 без retry3
    payment_id2 = await _create_payment(client, api_key, suffix="rec2")
    async with session_factory() as session:
        async with session.begin():
            await session.execute(
                text(
                    """
                    UPDATE payments
                    SET status = :failed,
                        processed_at = now(),
                        webhook_delivery_status = :pending,
                        webhook_attempts = 2,
                        processing_lease_until = NULL
                    WHERE id = :payment_id
                    """
                ),
                {
                    "payment_id": payment_id2,
                    "failed": PaymentStatus.FAILED.value,
                    "pending": WebhookDeliveryStatus.PENDING.value,
                },
            )
    assert await outbox_dispatcher.recover_pending_webhooks() == 1
    assert await _get_retry_outbox(session_factory, payment_id2, 3) is not None
    assert await outbox_dispatcher.recover_pending_webhooks() == 0

    # pending+attempts=0 и активный lease не трогаем
    payment_id3 = await _create_payment(client, api_key, suffix="rec3")
    async with session_factory() as session:
        async with session.begin():
            await session.execute(
                text(
                    """
                    UPDATE payments
                    SET status = :succeeded,
                        processed_at = now(),
                        webhook_delivery_status = :pending,
                        webhook_attempts = 0,
                        processing_lease_until = now() + interval '30 seconds'
                    WHERE id = :payment_id
                    """
                ),
                {
                    "payment_id": payment_id3,
                    "succeeded": PaymentStatus.SUCCEEDED.value,
                    "pending": WebhookDeliveryStatus.PENDING.value,
                },
            )
    before = await _get_retry_outbox(session_factory, payment_id3, 2)
    assert before is None
    assert await outbox_dispatcher.recover_pending_webhooks() == 0
    assert await _get_retry_outbox(session_factory, payment_id3, 2) is None


@pytest.mark.asyncio
async def test_crash_after_retry_commit_no_duplicate_http(
    api_client: tuple[AsyncClient, str],
    session_factory: async_sessionmaker[AsyncSession],
    integration_settings: Settings,
) -> None:
    """Crash после commit retry Outbox до ACK: повтор не шлёт лишний HTTP."""
    client, api_key = api_client
    payment_id = await _create_payment(client, api_key, suffix="crash-retry")
    recorder: list[httpx.Request] = []
    webhook = _webhook_client(status_code=500, recorder=recorder)
    service = _service(
        session_factory=session_factory,
        settings=integration_settings,
        gateway=GatewaySimulator(
            sleep=AsyncMock(), forced_outcome=PaymentStatus.SUCCEEDED
        ),
        webhook=webhook,
    )

    async def _boom() -> None:
        raise RuntimeError("injected crash after retry commit")

    service.set_after_webhook_failure_commit_hook(_boom)
    with pytest.raises(RuntimeError, match="injected crash after retry commit"):
        await service.process_payment_created(
            payment_id=payment_id, event_id=uuid.uuid4()
        )
    assert len(recorder) == 1
    assert await _get_retry_outbox(session_factory, payment_id, 2) is not None

    service.set_after_webhook_failure_commit_hook(None)
    result = await service.process_payment_created(
        payment_id=payment_id, event_id=uuid.uuid4()
    )
    assert result.detail == "retry_already_scheduled"
    assert len(recorder) == 1
    await webhook.aclose()


@pytest.mark.asyncio
async def test_system_retry_publish_confirm_fail_no_ack_semantics() -> None:
    """Confirm fail ⇒ publish_system_retry бросает; ACK не выполняется вызывающим."""
    broker = AsyncMock()
    broker.publish = AsyncMock(side_effect=TimeoutError("confirm timeout"))
    with pytest.raises(OutboxPublishError, match="confirm timeout"):
        await publish_system_retry(
            broker,
            body={
                "schema_version": 1,
                "event_id": str(uuid.uuid4()),
                "payment_id": str(uuid.uuid4()),
                "event_type": "payment.created",
                "attempt": 1,
            },
            current_system_attempt=1,
            message_id="e",
            correlation_id="p",
            confirm_timeout_seconds=0.1,
        )
