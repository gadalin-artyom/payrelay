from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any
from urllib.parse import quote, urlparse

import httpx
import pytest
from aio_pika import DeliveryMode, IncomingMessage
from aio_pika.exceptions import AMQPConnectionError, AMQPError
from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue
from httpx import AsyncClient
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payrelay.application.outbox_dispatcher import OutboxDispatcher
from payrelay.core.config import Settings
from payrelay.infrastructure.messaging.broker import create_rabbit_broker
from payrelay.infrastructure.messaging.publisher import OutboxAmqpPublisher
from payrelay.infrastructure.messaging.topology import (
    PAYMENTS_DLX,
    PAYMENTS_EXCHANGE,
    QUEUE_DLQ,
    QUEUE_PAYMENTS_NEW,
    QUEUE_RETRY_2,
    QUEUE_RETRY_3,
    RETRY_2_TTL_MS,
    RETRY_3_TTL_MS,
    ROUTING_KEY_DLQ,
    ROUTING_KEY_NEW,
    ROUTING_KEY_RETRY_2,
    ROUTING_KEY_RETRY_3,
    declare_payments_topology,
)
from tests.integration.conftest import assert_safe_test_rabbitmq_url

pytestmark = pytest.mark.integration


def _payment_body(*, suffix: str) -> dict[str, Any]:
    return {
        "amount": "1500.50",
        "currency": "RUB",
        "description": f"Outbox test {suffix}",
        "metadata": {"case": suffix},
        "webhook_url": "https://merchant.example.com/payments/callback",
    }


async def _drain_queue(
    broker: RabbitBroker,
    queue_name: str,
    *,
    limit: int = 50,
) -> list[IncomingMessage]:
    """Прочитать до ``limit`` сообщений из очереди с ручным ack (без consumer-роли).

    Args:
        broker: Подключённый брокер.
        queue_name: Имя очереди.
        limit: Максимум сообщений.

    Returns:
        Список входящих сообщений (уже ack).
    """
    queue = await broker.declare_queue(
        RabbitQueue(queue_name, durable=True, declare=False)
    )
    messages: list[IncomingMessage] = []
    for _ in range(limit):
        message = await queue.get(fail=False, timeout=0.2)
        if message is None:
            break
        messages.append(message)
        await message.ack()
    return messages


@pytest.fixture
async def rabbit_broker(
    integration_settings: Settings,
    integration_rabbitmq_url: str,
) -> AsyncIterator[RabbitBroker]:
    """Подключённый test RabbitBroker с объявленной payments topology.

    Args:
        integration_settings: Настройки теста.
        integration_rabbitmq_url: Проверенный URL.

    Yields:
        Брокер FastStream.
    """
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
    """Диспетчер Outbox на test DB + test broker.

    Args:
        integration_settings: Настройки.
        session_factory: Фабрика сессий test DB.
        rabbit_broker: Подключённый брокер.

    Returns:
        Готовый `OutboxDispatcher`.
    """
    publisher = OutboxAmqpPublisher(
        rabbit_broker,
        confirm_timeout_seconds=integration_settings.outbox_publish_timeout_seconds,
    )
    return OutboxDispatcher(
        session_factory=session_factory,
        publisher=publisher,
        settings=integration_settings,
    )


@pytest.mark.asyncio
async def test_i04_api_independent_of_broker(
    api_client: tuple[AsyncClient, str],
    session_factory: async_sessionmaker[AsyncSession],
    integration_settings: Settings,
) -> None:
    """I04: POST 202 при недоступном для publisher broker; outbox unpublished."""
    client, api_key = api_client
    unreachable = Settings(
        database_url=integration_settings.database_url,
        rabbitmq_url=SecretStr("amqp://payrelay_test:payrelay_test@127.0.0.1:1/"),
        api_key=integration_settings.api_key,
        app_env="test",
        outbox_lease_seconds=30,
        outbox_publish_timeout_seconds=1,
    )
    # API не использует RabbitMQ — достаточно создать платёж.
    response = await client.post(
        "/api/v1/payments",
        headers={
            "X-API-Key": api_key,
            "Idempotency-Key": f"i04-{uuid.uuid4()}",
        },
        json=_payment_body(suffix="i04"),
    )
    assert response.status_code == 202
    payment_id = response.json()["payment_id"]

    async with session_factory() as session:
        rows = (
            (
                await session.execute(
                    text(
                        """
                    SELECT id, published_at
                    FROM outbox
                    WHERE aggregate_id = :payment_id
                    """
                    ),
                    {"payment_id": payment_id},
                )
            )
            .mappings()
            .all()
        )
    assert len(rows) == 1
    assert rows[0]["published_at"] is None

    # Publisher с заведомо недоступным endpoint не должен подключиться.
    bad_broker = create_rabbit_broker(unreachable, fail_fast=True)
    with pytest.raises(
        (OSError, TimeoutError, ConnectionError, AMQPError, AMQPConnectionError)
    ):
        await asyncio.wait_for(bad_broker.connect(), timeout=2.0)
    try:
        await bad_broker.stop()
    except Exception:
        pass


@pytest.mark.asyncio
async def test_i05_publish_after_confirm(
    api_client: tuple[AsyncClient, str],
    session_factory: async_sessionmaker[AsyncSession],
    outbox_dispatcher: OutboxDispatcher,
    rabbit_broker: RabbitBroker,
) -> None:
    """I05: после confirm published_at IS NOT NULL; тело/message_id/routing верны."""
    client, api_key = api_client
    # Очистить возможный хвостовой backlog только нашей очереди через drain.
    await _drain_queue(rabbit_broker, QUEUE_PAYMENTS_NEW)

    response = await client.post(
        "/api/v1/payments",
        headers={
            "X-API-Key": api_key,
            "Idempotency-Key": f"i05-{uuid.uuid4()}",
        },
        json=_payment_body(suffix="i05"),
    )
    assert response.status_code == 202
    payment_id = response.json()["payment_id"]

    async with session_factory() as session:
        outbox = (
            (
                await session.execute(
                    text(
                        "SELECT id, payload, published_at FROM outbox "
                        "WHERE aggregate_id = :payment_id"
                    ),
                    {"payment_id": payment_id},
                )
            )
            .mappings()
            .one()
        )
    assert outbox["published_at"] is None
    event_id = str(outbox["id"])

    handled = await outbox_dispatcher.process_one()
    assert handled is True

    async with session_factory() as session:
        published = (
            await session.execute(
                text("SELECT published_at FROM outbox WHERE id = :event_id"),
                {"event_id": event_id},
            )
        ).scalar_one()
    assert published is not None

    messages = await _drain_queue(rabbit_broker, QUEUE_PAYMENTS_NEW)
    assert len(messages) == 1
    message = messages[0]
    body = json.loads(message.body.decode("utf-8"))
    assert body["event_id"] == event_id
    assert body["payment_id"] == payment_id
    assert body["attempt"] == 1
    assert body["schema_version"] == 1
    assert message.message_id == event_id
    assert message.correlation_id == payment_id
    assert message.content_type == "application/json"
    assert message.delivery_mode == DeliveryMode.PERSISTENT
    # routing_key на get() из очереди — исходный key binding.
    assert message.routing_key == ROUTING_KEY_NEW


@pytest.mark.asyncio
async def test_i11_crash_after_confirm_allows_duplicate_same_event_id(
    api_client: tuple[AsyncClient, str],
    session_factory: async_sessionmaker[AsyncSession],
    outbox_dispatcher: OutboxDispatcher,
    rabbit_broker: RabbitBroker,
) -> None:
    """I11: сбой после confirm до mark → повтор с тем же event_id (дубль допустим)."""
    client, api_key = api_client
    await _drain_queue(rabbit_broker, QUEUE_PAYMENTS_NEW)

    response = await client.post(
        "/api/v1/payments",
        headers={
            "X-API-Key": api_key,
            "Idempotency-Key": f"i11-{uuid.uuid4()}",
        },
        json=_payment_body(suffix="i11"),
    )
    assert response.status_code == 202
    payment_id = response.json()["payment_id"]

    async with session_factory() as session:
        event_id = (
            await session.execute(
                text("SELECT id FROM outbox WHERE aggregate_id = :payment_id"),
                {"payment_id": payment_id},
            )
        ).scalar_one()

    async def _fail_before_mark(_outbox_id: uuid.UUID, _token: datetime) -> None:
        raise RuntimeError("injected crash after broker confirm")

    outbox_dispatcher.set_before_mark_published_hook(_fail_before_mark)
    with pytest.raises(RuntimeError, match="injected crash"):
        await outbox_dispatcher.process_one()
    outbox_dispatcher.set_before_mark_published_hook(None)

    async with session_factory() as session:
        row = (
            (
                await session.execute(
                    text(
                        """
                    SELECT published_at, claimed_until, attempts
                    FROM outbox WHERE id = :event_id
                    """
                    ),
                    {"event_id": event_id},
                )
            )
            .mappings()
            .one()
        )
    assert row["published_at"] is None
    assert row["attempts"] >= 1

    # Принудительно истечь lease в test DB (без реального sleep).
    async with session_factory() as session:
        async with session.begin():
            await session.execute(
                text(
                    """
                    UPDATE outbox
                    SET claimed_until = now() - interval '1 second',
                        available_at = now() - interval '1 second'
                    WHERE id = :event_id
                    """
                ),
                {"event_id": event_id},
            )

    assert await outbox_dispatcher.process_one() is True

    async with session_factory() as session:
        published_at = (
            await session.execute(
                text("SELECT published_at FROM outbox WHERE id = :event_id"),
                {"event_id": event_id},
            )
        ).scalar_one()
    assert published_at is not None

    messages = await _drain_queue(rabbit_broker, QUEUE_PAYMENTS_NEW)
    assert len(messages) == 2
    event_ids = {json.loads(msg.body.decode("utf-8"))["event_id"] for msg in messages}
    assert event_ids == {str(event_id)}
    assert {msg.message_id for msg in messages} == {str(event_id)}


@pytest.mark.asyncio
async def test_concurrency_skip_locked_single_publish(
    api_client: tuple[AsyncClient, str],
    session_factory: async_sessionmaker[AsyncSession],
    integration_settings: Settings,
    rabbit_broker: RabbitBroker,
) -> None:
    """Два dispatcher: один claim на строку; в очереди одно событие при живом lease."""
    client, api_key = api_client
    await _drain_queue(rabbit_broker, QUEUE_PAYMENTS_NEW)

    response = await client.post(
        "/api/v1/payments",
        headers={
            "X-API-Key": api_key,
            "Idempotency-Key": f"conc-{uuid.uuid4()}",
        },
        json=_payment_body(suffix="concurrency"),
    )
    assert response.status_code == 202

    publisher = OutboxAmqpPublisher(
        rabbit_broker,
        confirm_timeout_seconds=integration_settings.outbox_publish_timeout_seconds,
    )
    d1 = OutboxDispatcher(
        session_factory=session_factory,
        publisher=publisher,
        settings=integration_settings,
    )
    d2 = OutboxDispatcher(
        session_factory=session_factory,
        publisher=publisher,
        settings=integration_settings,
    )

    results = await asyncio.gather(d1.process_one(), d2.process_one())
    assert sorted(results) == [False, True]

    messages = await _drain_queue(rabbit_broker, QUEUE_PAYMENTS_NEW)
    assert len(messages) == 1


@pytest.mark.asyncio
async def test_unroutable_mandatory_does_not_mark_published(
    api_client: tuple[AsyncClient, str],
    session_factory: async_sessionmaker[AsyncSession],
    integration_settings: Settings,
    rabbit_broker: RabbitBroker,
) -> None:
    """Mandatory return без binding не приводит к mark-published."""
    client, api_key = api_client
    response = await client.post(
        "/api/v1/payments",
        headers={
            "X-API-Key": api_key,
            "Idempotency-Key": f"unr-{uuid.uuid4()}",
        },
        json=_payment_body(suffix="unroutable"),
    )
    assert response.status_code == 202
    payment_id = response.json()["payment_id"]

    orphan_name = f"payrelay.test.orphan.{uuid.uuid4().hex}"
    orphan_exchange = RabbitExchange(
        orphan_name,
        type=ExchangeType.DIRECT,
        durable=False,
        auto_delete=True,
    )
    await rabbit_broker.declare_exchange(orphan_exchange)

    publisher = OutboxAmqpPublisher(
        rabbit_broker,
        confirm_timeout_seconds=integration_settings.outbox_publish_timeout_seconds,
        exchange=orphan_exchange,
        routing_key="no.binding.key",
    )
    dispatcher = OutboxDispatcher(
        session_factory=session_factory,
        publisher=publisher,
        settings=integration_settings,
    )
    assert await dispatcher.process_one() is True

    async with session_factory() as session:
        published_at = (
            await session.execute(
                text(
                    "SELECT published_at FROM outbox WHERE aggregate_id = :payment_id"
                ),
                {"payment_id": payment_id},
            )
        ).scalar_one()
    assert published_at is None

    # Удаляем только временный orphan exchange теста.
    exch = await rabbit_broker.declare_exchange(orphan_exchange)
    await exch.delete(if_unused=False)


@pytest.mark.asyncio
async def test_lease_recovery_after_expiry(
    api_client: tuple[AsyncClient, str],
    session_factory: async_sessionmaker[AsyncSession],
    outbox_dispatcher: OutboxDispatcher,
    rabbit_broker: RabbitBroker,
) -> None:
    """Запись с активным lease недоступна; после истечения — снова claimable."""
    client, api_key = api_client
    await _drain_queue(rabbit_broker, QUEUE_PAYMENTS_NEW)

    response = await client.post(
        "/api/v1/payments",
        headers={
            "X-API-Key": api_key,
            "Idempotency-Key": f"lease-{uuid.uuid4()}",
        },
        json=_payment_body(suffix="lease"),
    )
    assert response.status_code == 202
    payment_id = response.json()["payment_id"]

    async with session_factory() as session:
        async with session.begin():
            await session.execute(
                text(
                    """
                    UPDATE outbox
                    SET claimed_until = now() + interval '10 minutes',
                        attempts = 1
                    WHERE aggregate_id = :payment_id
                    """
                ),
                {"payment_id": payment_id},
            )

    assert await outbox_dispatcher.process_one() is False

    async with session_factory() as session:
        async with session.begin():
            await session.execute(
                text(
                    """
                    UPDATE outbox
                    SET claimed_until = now() - interval '1 second',
                        available_at = now() - interval '1 second'
                    WHERE aggregate_id = :payment_id
                    """
                ),
                {"payment_id": payment_id},
            )

    assert await outbox_dispatcher.process_one() is True
    messages = await _drain_queue(rabbit_broker, QUEUE_PAYMENTS_NEW)
    assert len(messages) == 1


async def _mgmt_queue_arguments(rabbitmq_url: str, queue_name: str) -> dict[str, Any]:
    """Прочитать arguments очереди через RabbitMQ Management HTTP API.

    Args:
        rabbitmq_url: AMQP URL тестового брокера.
        queue_name: Имя очереди.

    Returns:
        Словарь arguments из management API.
    """
    parsed = urlparse(rabbitmq_url)
    user = parsed.username or ""
    password = parsed.password or ""
    host = parsed.hostname or "rabbitmq-test"
    vhost = parsed.path.lstrip("/") or "/"
    vhost_enc = quote(vhost, safe="")
    queue_enc = quote(queue_name, safe="")
    url = f"http://{host}:15672/api/queues/{vhost_enc}/{queue_enc}"
    async with httpx.AsyncClient(timeout=5.0) as client:
        response = await client.get(url, auth=(user, password))
        response.raise_for_status()
        payload = response.json()
    arguments = payload.get("arguments") or {}
    assert isinstance(arguments, dict)
    return arguments


@pytest.mark.asyncio
async def test_topology_durable_bindings_and_redeclare(
    rabbit_broker: RabbitBroker,
    integration_rabbitmq_url: str,
) -> None:
    """Topology: durable queues/exchanges, DLX+TTL; повторный declare безопасен."""
    assert_safe_test_rabbitmq_url(integration_rabbitmq_url)
    # Повторный declare не должен падать и не требует delete.
    await declare_payments_topology(rabbit_broker)
    await declare_payments_topology(rabbit_broker)

    new_args = await _mgmt_queue_arguments(integration_rabbitmq_url, QUEUE_PAYMENTS_NEW)
    assert new_args.get("x-dead-letter-exchange") == PAYMENTS_DLX
    assert new_args.get("x-dead-letter-routing-key") == ROUTING_KEY_DLQ

    retry2_args = await _mgmt_queue_arguments(integration_rabbitmq_url, QUEUE_RETRY_2)
    assert retry2_args.get("x-message-ttl") == RETRY_2_TTL_MS
    assert retry2_args.get("x-dead-letter-exchange") == PAYMENTS_EXCHANGE
    assert retry2_args.get("x-dead-letter-routing-key") == ROUTING_KEY_NEW

    retry3_args = await _mgmt_queue_arguments(integration_rabbitmq_url, QUEUE_RETRY_3)
    assert retry3_args.get("x-message-ttl") == RETRY_3_TTL_MS
    assert retry3_args.get("x-dead-letter-exchange") == PAYMENTS_EXCHANGE
    assert retry3_args.get("x-dead-letter-routing-key") == ROUTING_KEY_NEW

    dlq_args = await _mgmt_queue_arguments(integration_rabbitmq_url, QUEUE_DLQ)
    assert isinstance(dlq_args, dict)

    assert ROUTING_KEY_RETRY_2 == QUEUE_RETRY_2
    assert ROUTING_KEY_RETRY_3 == QUEUE_RETRY_3
    assert ROUTING_KEY_DLQ == QUEUE_DLQ
