"""U15: unit-тесты system retry publish и consumer helpers."""

from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from pamqp.commands import Basic
from pydantic import SecretStr, ValidationError

from payrelay.core.config import Settings
from payrelay.domain.webhook_retry_rules import SYSTEM_ATTEMPT_HEADER
from payrelay.infrastructure.messaging.consumer import (
    PaymentEventMessage,
    handle_system_failure,
)
from payrelay.infrastructure.messaging.publisher import OutboxPublishError
from payrelay.infrastructure.messaging.system_retry import publish_system_retry
from payrelay.infrastructure.messaging.topology import (
    PAYMENTS_EXCHANGE,
    ROUTING_KEY_RETRY_2,
    ROUTING_KEY_RETRY_3,
    build_payments_topology,
)

pytestmark = pytest.mark.unit


def _settings() -> Settings:
    return Settings(
        database_url=SecretStr(
            "postgresql+asyncpg://payrelay:payrelay@localhost:5432/payrelay_test"
        ),
        rabbitmq_url=SecretStr("amqp://payrelay:payrelay@localhost:5672/"),
        api_key=SecretStr("test-api-key-not-for-production"),
        app_env="test",
        outbox_publish_timeout_seconds=1.0,
    )


def _body(*, event_type: str = "payment.created", attempt: int = 1) -> dict[str, Any]:
    event_id = uuid.uuid4()
    payment_id = uuid.uuid4()
    return {
        "schema_version": 1,
        "event_id": str(event_id),
        "payment_id": str(payment_id),
        "event_type": event_type,
        "attempt": attempt,
    }


@pytest.mark.asyncio
async def test_u15_publish_system_retry_routing_and_headers() -> None:
    """U15: publish в retry.2/retry.3 с persist/mandatory/confirm и header."""
    broker = AsyncMock()
    broker.publish = AsyncMock(return_value=Basic.Ack())
    body = _body()

    nxt = await publish_system_retry(
        broker,
        body=body,
        current_system_attempt=1,
        message_id=body["event_id"],
        correlation_id=body["payment_id"],
        confirm_timeout_seconds=1.0,
    )
    assert nxt == 2
    kwargs = broker.publish.await_args.kwargs
    assert kwargs["routing_key"] == ROUTING_KEY_RETRY_2
    assert kwargs["exchange"] == PAYMENTS_EXCHANGE
    assert kwargs["persist"] is True
    assert kwargs["mandatory"] is True
    assert kwargs["headers"][SYSTEM_ATTEMPT_HEADER] == 2
    assert kwargs["message_id"] == body["event_id"]

    broker.publish.reset_mock()
    nxt3 = await publish_system_retry(
        broker,
        body=body,
        current_system_attempt=2,
        message_id=body["event_id"],
        correlation_id=body["payment_id"],
        confirm_timeout_seconds=1.0,
    )
    assert nxt3 == 3
    assert broker.publish.await_args.kwargs["routing_key"] == ROUTING_KEY_RETRY_3


@pytest.mark.asyncio
async def test_u15_no_ack_when_publish_confirm_fails() -> None:
    """U15: при отказе publish исходное сообщение не ACK."""
    broker = AsyncMock()
    broker.publish = AsyncMock(side_effect=OutboxPublishError("broker basic.nack"))
    message = AsyncMock()
    message.headers = {}
    message.raw_message = MagicMock(message_id="e1", correlation_id="p1")
    body = _body()
    event = PaymentEventMessage.model_validate(body)

    await handle_system_failure(
        broker=broker,
        message=message,
        body=body,
        event=event,
        settings=_settings(),
        topology=build_payments_topology(),
    )
    message.ack.assert_not_awaited()
    message.reject.assert_not_awaited()


@pytest.mark.asyncio
async def test_u15_system_attempt_3_rejects_to_dlq() -> None:
    """U15: system_attempt=3 → reject(requeue=False), без publish."""
    broker = AsyncMock()
    message = AsyncMock()
    message.headers = {SYSTEM_ATTEMPT_HEADER: 3}
    message.raw_message = MagicMock(message_id="e1", correlation_id="p1")
    body = _body()
    event = PaymentEventMessage.model_validate(body)

    await handle_system_failure(
        broker=broker,
        message=message,
        body=body,
        event=event,
        settings=_settings(),
        topology=build_payments_topology(),
    )
    message.reject.assert_awaited_once_with(requeue=False)
    broker.publish.assert_not_awaited()


def test_poison_payload_rejected_by_validation() -> None:
    """Poison: неверная schema/type/attempt → ValidationError."""
    with pytest.raises(ValidationError):
        PaymentEventMessage.model_validate(
            {
                "schema_version": 99,
                "event_id": str(uuid.uuid4()),
                "payment_id": str(uuid.uuid4()),
                "event_type": "payment.created",
                "attempt": 1,
            }
        )
    with pytest.raises(ValidationError):
        PaymentEventMessage.model_validate(
            {
                "schema_version": 1,
                "event_id": str(uuid.uuid4()),
                "payment_id": str(uuid.uuid4()),
                "event_type": "payment.created",
                "attempt": 2,
            }
        )
    with pytest.raises(ValidationError):
        PaymentEventMessage.model_validate(
            {
                "schema_version": 1,
                "event_id": str(uuid.uuid4()),
                "payment_id": str(uuid.uuid4()),
                "event_type": "unknown.event",
                "attempt": 1,
            }
        )
