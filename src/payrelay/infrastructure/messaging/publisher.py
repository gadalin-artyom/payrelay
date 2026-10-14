from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from aio_pika.exceptions import DeliveryError
from faststream.rabbit import RabbitBroker, RabbitExchange
from pamqp.commands import Basic

from payrelay.infrastructure.messaging.topology import (
    PAYMENTS_EXCHANGE,
    ROUTING_KEY_NEW,
    PaymentsTopology,
    build_payments_topology,
)


class OutboxPublishError(Exception):
    """Публикация не подтверждена брокером (или результат неизвестен)."""


@dataclass(frozen=True, slots=True)
class AmqpPublishPlan:
    """План публикации AMQP-сообщения из Outbox payload."""

    body: dict[str, Any]
    exchange_name: str
    routing_key: str
    message_id: str
    correlation_id: str
    content_type: str
    persist: bool
    mandatory: bool
    confirm_timeout_seconds: float


def build_amqp_publish_plan(
    *,
    outbox_id: uuid.UUID,
    aggregate_id: uuid.UUID,
    payload: dict[str, Any],
    confirm_timeout_seconds: float,
) -> AmqpPublishPlan:
    """Построить план публикации из payload Outbox без изменения тела.

    Args:
        outbox_id: UUID записи Outbox (`event_id`).
        aggregate_id: UUID платежа.
        payload: JSON payload как в БД.
        confirm_timeout_seconds: Таймаут ожидания broker confirm.

    Returns:
        Неизменяемый план публикации.

    Raises:
        OutboxPublishError: payload невалиден или не согласован с записью.
    """
    if not isinstance(payload, dict):
        raise OutboxPublishError("payload must be an object")

    required = ("schema_version", "event_id", "payment_id", "event_type", "attempt")
    missing = [key for key in required if key not in payload]
    if missing:
        raise OutboxPublishError(f"payload missing fields: {', '.join(missing)}")

    event_id = str(payload["event_id"])
    payment_id = str(payload["payment_id"])
    if event_id != str(outbox_id):
        raise OutboxPublishError("payload.event_id must equal outbox.id")
    if payment_id != str(aggregate_id):
        raise OutboxPublishError("payload.payment_id must equal aggregate_id")
    if not isinstance(payload["schema_version"], int):
        raise OutboxPublishError("payload.schema_version must be int")
    if not isinstance(payload["attempt"], int) or payload["attempt"] < 1:
        raise OutboxPublishError("payload.attempt must be positive int")
    if not isinstance(payload["event_type"], str) or not payload["event_type"]:
        raise OutboxPublishError("payload.event_type must be non-empty str")

    return AmqpPublishPlan(
        body=dict(payload),
        exchange_name=PAYMENTS_EXCHANGE,
        routing_key=ROUTING_KEY_NEW,
        message_id=event_id,
        correlation_id=payment_id,
        content_type="application/json",
        persist=True,
        mandatory=True,
        confirm_timeout_seconds=confirm_timeout_seconds,
    )


type ConfirmFrame = Basic.Ack | Basic.Nack | Basic.Reject


def assert_broker_confirmed(confirmation: ConfirmFrame | None) -> None:
    """Проверить, что брокер явно подтвердил запись сообщения.

    Args:
        confirmation: Результат `RabbitBroker.publish`.

    Raises:
        OutboxPublishError: confirm отсутствует, nack или reject.
    """
    if confirmation is None:
        raise OutboxPublishError("broker confirm missing (publisher_confirms off?)")
    if isinstance(confirmation, Basic.Ack):
        return
    if isinstance(confirmation, Basic.Nack):
        raise OutboxPublishError("broker basic.nack")
    if isinstance(confirmation, Basic.Reject):
        raise OutboxPublishError("broker basic.reject")
    raise OutboxPublishError(f"unexpected confirm type: {type(confirmation)!r}")


class OutboxAmqpPublisher:
    """Публикатор Outbox → payments.exchange / payments.new."""

    def __init__(
        self,
        broker: RabbitBroker,
        *,
        topology: PaymentsTopology | None = None,
        confirm_timeout_seconds: float,
        exchange: RabbitExchange | None = None,
        routing_key: str = ROUTING_KEY_NEW,
    ) -> None:
        """Инициализировать публикатор.

        Args:
            broker: Подключённый брокер с confirms.
            topology: Схемы топологии (по умолчанию стандартная).
            confirm_timeout_seconds: Таймаут ожидания confirm.
            exchange: Целевой exchange (для unroutable-тестов можно подменить).
            routing_key: Routing key публикации.
        """
        self._broker = broker
        self._topology = topology or build_payments_topology()
        self._confirm_timeout_seconds = confirm_timeout_seconds
        self._exchange = exchange or self._topology.payments_exchange
        self._routing_key = routing_key

    async def publish_outbox_payload(
        self,
        *,
        outbox_id: uuid.UUID,
        aggregate_id: uuid.UUID,
        payload: dict[str, Any],
    ) -> AmqpPublishPlan:
        """Опубликовать payload и дождаться доказанного broker confirm.

        Args:
            outbox_id: UUID Outbox.
            aggregate_id: UUID платежа.
            payload: Тело из столбца `outbox.payload`.

        Returns:
            План, по которому выполнена публикация.

        Raises:
            OutboxPublishError: валидация, timeout, return, nack или отсутствие confirm.
            TimeoutError: истечение timeout ожидания confirm.
            DeliveryError: unroutable mandatory return.
        """
        plan = build_amqp_publish_plan(
            outbox_id=outbox_id,
            aggregate_id=aggregate_id,
            payload=payload,
            confirm_timeout_seconds=self._confirm_timeout_seconds,
        )
        try:
            confirmation = await self._broker.publish(
                plan.body,
                exchange=self._exchange,
                routing_key=self._routing_key,
                mandatory=plan.mandatory,
                persist=plan.persist,
                message_id=plan.message_id,
                correlation_id=plan.correlation_id,
                content_type=plan.content_type,
                timeout=plan.confirm_timeout_seconds,
            )
        except DeliveryError as exc:
            raise OutboxPublishError("mandatory message returned (unroutable)") from exc
        except TimeoutError as exc:
            raise OutboxPublishError("broker confirm timeout") from exc

        assert_broker_confirmed(confirmation)
        return plan
