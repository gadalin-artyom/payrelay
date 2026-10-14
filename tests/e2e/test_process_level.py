from __future__ import annotations

import asyncio
import json
import time
import uuid
from typing import Any

import httpx
import pytest
from aio_pika import IncomingMessage
from faststream.rabbit import RabbitBroker
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payrelay.core.config import Settings
from payrelay.domain.enums import PaymentStatus, WebhookDeliveryStatus
from payrelay.infrastructure.messaging.broker import create_rabbit_broker
from payrelay.infrastructure.messaging.publisher import OutboxAmqpPublisher
from payrelay.infrastructure.messaging.topology import (
    QUEUE_DLQ,
    declare_payments_topology,
)
from payrelay.infrastructure.webhooks.http_client import build_webhook_event_id
from tests.e2e.process_harness import ProcessHarness
from tests.e2e.webhook_receiver import WebhookReceiver
from tests.integration.test_outbox_publisher import _drain_queue

pytestmark = pytest.mark.e2e

_TERMINAL = {PaymentStatus.SUCCEEDED.value, PaymentStatus.FAILED.value}


def _payload(*, webhook_url: str, suffix: str) -> dict[str, Any]:
    """Собрать тело POST /payments.

    Args:
        webhook_url: URL test receiver.
        suffix: Метка сценария в description/metadata.

    Returns:
        JSON-тело запроса.
    """
    return {
        "amount": "1500.50",
        "currency": "RUB",
        "description": f"E2E {suffix}",
        "metadata": {"case": suffix},
        "webhook_url": webhook_url,
    }


async def _wait_payment(
    session_factory: async_sessionmaker[AsyncSession],
    payment_id: uuid.UUID,
    *,
    predicate: Any,
    timeout_seconds: float,
    interval: float = 0.25,
) -> dict[str, Any]:
    """Ждать состояние платежа по предикату (без безусловного sleep).

    Args:
        session_factory: Фабрика сессий.
        payment_id: UUID платежа.
        predicate: Callable(row) -> bool.
        timeout_seconds: Максимум ожидания.
        interval: Интервал опроса.

    Returns:
        Строка payments.

    Raises:
        TimeoutError: если условие не выполнено.
    """
    deadline = time.monotonic() + timeout_seconds
    last: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        async with session_factory() as session:
            row = (
                (
                    await session.execute(
                        text(
                            """
                            SELECT id, status, webhook_delivery_status,
                                   webhook_attempts, processed_at
                            FROM payments WHERE id = :payment_id
                            """
                        ),
                        {"payment_id": payment_id},
                    )
                )
                .mappings()
                .one()
            )
        last = dict(row)
        if predicate(last):
            return last
        await asyncio.sleep(interval)
    msg = (
        f"payment {payment_id} condition not met within {timeout_seconds}s; last={last}"
    )
    raise TimeoutError(msg)


async def _get_outbox_created(
    session_factory: async_sessionmaker[AsyncSession],
    payment_id: uuid.UUID,
) -> dict[str, Any]:
    """Прочитать outbox payment.created.

    Args:
        session_factory: Фабрика сессий.
        payment_id: UUID платежа.

    Returns:
        Строка outbox.
    """
    async with session_factory() as session:
        row = (
            (
                await session.execute(
                    text(
                        """
                        SELECT id, event_type, published_at, available_at, payload
                        FROM outbox
                        WHERE aggregate_id = :payment_id
                          AND event_type = 'payment.created'
                        ORDER BY created_at
                        LIMIT 1
                        """
                    ),
                    {"payment_id": payment_id},
                )
            )
            .mappings()
            .one()
        )
    return dict(row)


async def _wait_dlq_message(
    broker: RabbitBroker,
    payment_id: uuid.UUID,
    *,
    timeout_seconds: float,
) -> IncomingMessage:
    """Дождаться сообщения в payments.dlq для payment_id.

    Args:
        broker: Подключённый брокер.
        payment_id: UUID платежа.
        timeout_seconds: Таймаут.

    Returns:
        Сообщение DLQ (ack после чтения).

    Raises:
        TimeoutError: если сообщение не найдено.
    """
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        messages = await _drain_queue(broker, QUEUE_DLQ, limit=50)
        for message in messages:
            body = json.loads(message.body.decode("utf-8"))
            if body.get("payment_id") == str(payment_id):
                return message
        await asyncio.sleep(0.5)
    msg = f"DLQ message for {payment_id} not found within {timeout_seconds}s"
    raise TimeoutError(msg)


@pytest.mark.asyncio
async def test_e2e01_post_terminal_webhook_delivered(
    process_harness: ProcessHarness,
    webhook_receiver: WebhookReceiver,
    e2e_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """E2E-01: POST 202 → outbox → publish → terminal → webhook 200 → GET."""
    webhook_receiver.set_status_sequence([200])
    client = httpx.AsyncClient(base_url=process_harness.api_base_url, timeout=10.0)
    try:
        response = await client.post(
            "/api/v1/payments",
            headers={
                "X-API-Key": process_harness.api_key,
                "Idempotency-Key": f"e2e01-{uuid.uuid4()}",
            },
            json=_payload(webhook_url=webhook_receiver.hook_url, suffix="e2e01"),
        )
        assert response.status_code == 202
        body = response.json()
        payment_id = uuid.UUID(body["payment_id"])
        assert body["status"] == PaymentStatus.PENDING.value

        row = await _wait_payment(
            e2e_session_factory,
            payment_id,
            predicate=lambda r: (
                r["status"] in _TERMINAL
                and r["webhook_delivery_status"]
                == WebhookDeliveryStatus.DELIVERED.value
            ),
            timeout_seconds=60.0,
        )
        assert row["webhook_attempts"] >= 1

        get_resp = await client.get(
            f"/api/v1/payments/{payment_id}",
            headers={"X-API-Key": process_harness.api_key},
        )
        assert get_resp.status_code == 200
        got = get_resp.json()
        assert got["status"] == row["status"]
        assert got["amount"] == "1500.50"
        assert got["webhook_delivery_status"] == WebhookDeliveryStatus.DELIVERED.value

        calls = webhook_receiver.snapshot_calls()
        assert len(calls) >= 1
        assert calls[0].payment_id == str(payment_id)
        assert calls[0].webhook_event_id == str(build_webhook_event_id(payment_id))
        assert calls[0].status_code == 200
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_e2e02_webhook_500_500_200_three_attempts(
    process_harness: ProcessHarness,
    webhook_receiver: WebhookReceiver,
    e2e_session_factory: async_sessionmaker[AsyncSession],
    e2e_settings: Settings,
) -> None:
    """E2E-02: receiver 500,500,200 → ровно 3 логические попытки, интервалы ~2/4 с."""
    webhook_receiver.set_status_sequence([500, 500, 200])
    client = httpx.AsyncClient(base_url=process_harness.api_base_url, timeout=10.0)
    broker = create_rabbit_broker(e2e_settings, fail_fast=True)
    await broker.connect()
    await declare_payments_topology(broker)
    try:
        response = await client.post(
            "/api/v1/payments",
            headers={
                "X-API-Key": process_harness.api_key,
                "Idempotency-Key": f"e2e02-{uuid.uuid4()}",
            },
            json=_payload(webhook_url=webhook_receiver.hook_url, suffix="e2e02"),
        )
        assert response.status_code == 202
        payment_id = uuid.UUID(response.json()["payment_id"])
        expected_event_id = str(build_webhook_event_id(payment_id))

        row = await _wait_payment(
            e2e_session_factory,
            payment_id,
            predicate=lambda r: (
                r["status"] in _TERMINAL
                and r["webhook_delivery_status"]
                == WebhookDeliveryStatus.DELIVERED.value
                and r["webhook_attempts"] == 3
            ),
            timeout_seconds=90.0,
        )
        assert row["status"] in _TERMINAL

        calls = [
            c
            for c in webhook_receiver.snapshot_calls()
            if c.payment_id == str(payment_id)
        ]
        assert len(calls) == 3
        assert [c.status_code for c in calls] == [500, 500, 200]
        assert all(c.webhook_event_id == expected_event_id for c in calls)

        gap1 = calls[1].received_at - calls[0].received_at
        gap2 = calls[2].received_at - calls[1].received_at
        # Допуск для RabbitMQ/Outbox/CI: не требуем точных 2.000/4.000.
        assert 1.0 <= gap1 <= 12.0, f"unexpected gap1={gap1}"
        assert 2.0 <= gap2 <= 16.0, f"unexpected gap2={gap2}"
        assert gap2 >= gap1 * 0.8

        dlq = await _drain_queue(broker, QUEUE_DLQ, limit=20)
        assert not any(
            json.loads(m.body.decode("utf-8")).get("payment_id") == str(payment_id)
            for m in dlq
        )
    finally:
        await broker.stop()
        await client.aclose()


@pytest.mark.asyncio
async def test_e2e03_webhook_500x3_exhausted_dlq(
    process_harness: ProcessHarness,
    webhook_receiver: WebhookReceiver,
    e2e_session_factory: async_sessionmaker[AsyncSession],
    e2e_settings: Settings,
) -> None:
    """E2E-03: 500×3 → exhausted, сообщение в payments.dlq, business status terminal."""
    webhook_receiver.set_status_sequence([500, 500, 500])
    client = httpx.AsyncClient(base_url=process_harness.api_base_url, timeout=10.0)
    broker = create_rabbit_broker(e2e_settings, fail_fast=True)
    await broker.connect()
    await declare_payments_topology(broker)
    try:
        response = await client.post(
            "/api/v1/payments",
            headers={
                "X-API-Key": process_harness.api_key,
                "Idempotency-Key": f"e2e03-{uuid.uuid4()}",
            },
            json=_payload(webhook_url=webhook_receiver.hook_url, suffix="e2e03"),
        )
        assert response.status_code == 202
        payment_id = uuid.UUID(response.json()["payment_id"])

        row = await _wait_payment(
            e2e_session_factory,
            payment_id,
            predicate=lambda r: (
                r["status"] in _TERMINAL
                and r["webhook_delivery_status"]
                == WebhookDeliveryStatus.EXHAUSTED.value
                and r["webhook_attempts"] == 3
            ),
            timeout_seconds=90.0,
        )
        assert row["status"] in _TERMINAL

        calls = [
            c
            for c in webhook_receiver.snapshot_calls()
            if c.payment_id == str(payment_id)
        ]
        assert len(calls) == 3
        assert all(c.status_code == 500 for c in calls)

        dlq_msg = await _wait_dlq_message(broker, payment_id, timeout_seconds=30.0)
        headers = dlq_msg.headers or {}
        assert "x-death" in headers or headers.get("x-death") is not None
    finally:
        await broker.stop()
        await client.aclose()


@pytest.mark.asyncio
async def test_e2e04_api_without_publisher_then_recovery(
    api_only_harness: ProcessHarness,
    webhook_receiver: WebhookReceiver,
    e2e_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """E2E-04: API 202 при остановленном publisher; после старта — доставка."""
    webhook_receiver.set_status_sequence([200])
    client = httpx.AsyncClient(base_url=api_only_harness.api_base_url, timeout=10.0)
    try:
        response = await client.post(
            "/api/v1/payments",
            headers={
                "X-API-Key": api_only_harness.api_key,
                "Idempotency-Key": f"e2e04-{uuid.uuid4()}",
            },
            json=_payload(webhook_url=webhook_receiver.hook_url, suffix="e2e04"),
        )
        assert response.status_code == 202
        payment_id = uuid.UUID(response.json()["payment_id"])

        outbox = await _get_outbox_created(e2e_session_factory, payment_id)
        assert outbox["published_at"] is None

        # Короткое ожидание: без publisher запись не публикуется.
        await asyncio.sleep(1.0)
        outbox2 = await _get_outbox_created(e2e_session_factory, payment_id)
        assert outbox2["published_at"] is None

        api_only_harness.start_role("outbox-publisher")
        api_only_harness.start_role("consumer")

        row = await _wait_payment(
            e2e_session_factory,
            payment_id,
            predicate=lambda r: (
                r["status"] in _TERMINAL
                and r["webhook_delivery_status"]
                == WebhookDeliveryStatus.DELIVERED.value
            ),
            timeout_seconds=60.0,
        )
        assert row["status"] in _TERMINAL
        outbox3 = await _get_outbox_created(e2e_session_factory, payment_id)
        assert outbox3["published_at"] is not None
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_e2e05_idempotency_auth_and_no_gateway_replay(
    process_harness: ProcessHarness,
    webhook_receiver: WebhookReceiver,
    e2e_session_factory: async_sessionmaker[AsyncSession],
    e2e_settings: Settings,
) -> None:
    """E2E-05: идемпотентность, 401/409; повторный AMQP не шлёт второй webhook."""
    webhook_receiver.set_status_sequence([200])
    client = httpx.AsyncClient(base_url=process_harness.api_base_url, timeout=10.0)
    idem_key = f"e2e05-{uuid.uuid4()}"
    payload = _payload(webhook_url=webhook_receiver.hook_url, suffix="e2e05")
    broker = create_rabbit_broker(e2e_settings, fail_fast=True)
    await broker.connect()
    await declare_payments_topology(broker)
    try:
        unauthorized = await client.post(
            "/api/v1/payments",
            headers={"Idempotency-Key": idem_key},
            json=payload,
        )
        assert unauthorized.status_code == 401

        bad_key = await client.post(
            "/api/v1/payments",
            headers={
                "X-API-Key": "definitely-wrong-key",
                "Idempotency-Key": idem_key,
            },
            json=payload,
        )
        assert bad_key.status_code == 401

        first = await client.post(
            "/api/v1/payments",
            headers={
                "X-API-Key": process_harness.api_key,
                "Idempotency-Key": idem_key,
            },
            json=payload,
        )
        assert first.status_code == 202
        payment_id = uuid.UUID(first.json()["payment_id"])

        second = await client.post(
            "/api/v1/payments",
            headers={
                "X-API-Key": process_harness.api_key,
                "Idempotency-Key": idem_key,
            },
            json=payload,
        )
        assert second.status_code == 202
        assert uuid.UUID(second.json()["payment_id"]) == payment_id

        conflict = await client.post(
            "/api/v1/payments",
            headers={
                "X-API-Key": process_harness.api_key,
                "Idempotency-Key": idem_key,
            },
            json={**payload, "amount": "1500.51"},
        )
        assert conflict.status_code == 409

        row = await _wait_payment(
            e2e_session_factory,
            payment_id,
            predicate=lambda r: (
                r["status"] in _TERMINAL
                and r["webhook_delivery_status"]
                == WebhookDeliveryStatus.DELIVERED.value
            ),
            timeout_seconds=60.0,
        )
        calls_before = [
            c
            for c in webhook_receiver.snapshot_calls()
            if c.payment_id == str(payment_id)
        ]
        assert len(calls_before) >= 1
        attempts_before = row["webhook_attempts"]

        outbox = await _get_outbox_created(e2e_session_factory, payment_id)
        event_id = outbox["id"]
        payload = outbox["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        publisher = OutboxAmqpPublisher(
            broker,
            confirm_timeout_seconds=e2e_settings.outbox_publish_timeout_seconds,
        )
        # Повторная публикация того же события (имитация redelivery/дубля).
        await publisher.publish_outbox_payload(
            outbox_id=event_id,
            aggregate_id=payment_id,
            payload=dict(payload),
        )

        await asyncio.sleep(3.0)
        row2 = await _wait_payment(
            e2e_session_factory,
            payment_id,
            predicate=lambda r: r["status"] == row["status"],
            timeout_seconds=10.0,
        )
        assert row2["status"] == row["status"]
        assert row2["webhook_delivery_status"] == WebhookDeliveryStatus.DELIVERED.value
        assert row2["webhook_attempts"] == attempts_before
        calls_after = [
            c
            for c in webhook_receiver.snapshot_calls()
            if c.payment_id == str(payment_id)
        ]
        # Допустим физический дубль webhook только при crash-window; после
        # delivered повторный AMQP не должен инициировать новый HTTP.
        assert len(calls_after) == len(calls_before)
    finally:
        await broker.stop()
        await client.aclose()
