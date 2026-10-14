from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import AsyncMock

import pytest
from pamqp.commands import Basic

from payrelay.infrastructure.messaging.publisher import (
    OutboxAmqpPublisher,
    OutboxPublishError,
    assert_broker_confirmed,
    build_amqp_publish_plan,
)
from payrelay.infrastructure.messaging.topology import (
    PAYMENTS_EXCHANGE,
    ROUTING_KEY_NEW,
)


def _valid_payload(
    *,
    event_id: uuid.UUID,
    payment_id: uuid.UUID,
    attempt: int = 1,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "event_id": str(event_id),
        "payment_id": str(payment_id),
        "event_type": "payment.created",
        "attempt": attempt,
    }


@pytest.mark.unit
def test_amqp_plan_preserves_payload_ids_and_flags() -> None:
    """event_id/payment_id/attempt неизменны; persist+mandatory+timeout заданы."""
    event_id = uuid.uuid4()
    payment_id = uuid.uuid4()
    payload = _valid_payload(event_id=event_id, payment_id=payment_id, attempt=1)

    plan = build_amqp_publish_plan(
        outbox_id=event_id,
        aggregate_id=payment_id,
        payload=payload,
        confirm_timeout_seconds=5.0,
    )

    assert plan.body == payload
    assert plan.body["attempt"] == 1
    assert plan.body["schema_version"] == 1
    assert plan.message_id == str(event_id)
    assert plan.correlation_id == str(payment_id)
    assert plan.exchange_name == PAYMENTS_EXCHANGE
    assert plan.routing_key == ROUTING_KEY_NEW
    assert plan.persist is True
    assert plan.mandatory is True
    assert plan.confirm_timeout_seconds == 5.0
    assert plan.content_type == "application/json"


@pytest.mark.unit
def test_amqp_plan_rejects_mismatched_event_id() -> None:
    """Нельзя публиковать payload с чужим event_id."""
    event_id = uuid.uuid4()
    payment_id = uuid.uuid4()
    payload = _valid_payload(event_id=uuid.uuid4(), payment_id=payment_id)
    with pytest.raises(OutboxPublishError, match="event_id"):
        build_amqp_publish_plan(
            outbox_id=event_id,
            aggregate_id=payment_id,
            payload=payload,
            confirm_timeout_seconds=1.0,
        )


@pytest.mark.unit
def test_confirm_ack_allows_mark_published() -> None:
    """Basic.Ack — единственный успешный confirm."""
    assert_broker_confirmed(Basic.Ack(delivery_tag=1))


@pytest.mark.unit
@pytest.mark.parametrize(
    "confirmation",
    [None, Basic.Nack(delivery_tag=1), Basic.Reject(delivery_tag=1)],
)
def test_confirm_failure_forbids_mark_published(
    confirmation: Basic.Ack | Basic.Nack | Basic.Reject | None,
) -> None:
    """Отсутствие confirm / nack / reject запрещают mark-published."""
    with pytest.raises(OutboxPublishError):
        assert_broker_confirmed(confirmation)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_publisher_does_not_treat_exception_as_success() -> None:
    """Исключение publish не считается подтверждением."""
    broker = AsyncMock()
    broker.publish = AsyncMock(side_effect=TimeoutError("confirm timeout"))
    publisher = OutboxAmqpPublisher(broker, confirm_timeout_seconds=1.0)
    event_id = uuid.uuid4()
    payment_id = uuid.uuid4()
    with pytest.raises(OutboxPublishError):
        await publisher.publish_outbox_payload(
            outbox_id=event_id,
            aggregate_id=payment_id,
            payload=_valid_payload(event_id=event_id, payment_id=payment_id),
        )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_publisher_rejects_none_confirm() -> None:
    """publish без ConfirmationFrame запрещает успех."""
    broker = AsyncMock()
    broker.publish = AsyncMock(return_value=None)
    publisher = OutboxAmqpPublisher(broker, confirm_timeout_seconds=1.0)
    event_id = uuid.uuid4()
    payment_id = uuid.uuid4()
    with pytest.raises(OutboxPublishError, match="confirm missing"):
        await publisher.publish_outbox_payload(
            outbox_id=event_id,
            aggregate_id=payment_id,
            payload=_valid_payload(event_id=event_id, payment_id=payment_id),
        )
