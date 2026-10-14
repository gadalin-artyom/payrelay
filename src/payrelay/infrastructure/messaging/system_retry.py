"""Публикация bounded system retry в TTL-очереди RabbitMQ."""

from __future__ import annotations

import logging
from typing import Any

from aio_pika.exceptions import DeliveryError
from faststream.rabbit import RabbitBroker, RabbitExchange

from payrelay.domain.webhook_retry_rules import (
    SYSTEM_ATTEMPT_HEADER,
    next_system_attempt,
    system_retry_routing_key,
)
from payrelay.infrastructure.messaging.publisher import (
    OutboxPublishError,
    assert_broker_confirmed,
)
from payrelay.infrastructure.messaging.topology import PAYMENTS_EXCHANGE

logger = logging.getLogger(__name__)


async def publish_system_retry(
    broker: RabbitBroker,
    *,
    body: dict[str, Any],
    current_system_attempt: int,
    message_id: str | None,
    correlation_id: str | None,
    confirm_timeout_seconds: float,
    exchange: RabbitExchange | str | None = None,
) -> int:
    """Опубликовать то же логическое сообщение в TTL retry-очередь.

    Args:
        broker: RabbitBroker с publisher confirms.
        body: Неизменённый JSON payload.
        current_system_attempt: Текущий ``system_attempt`` исходного сообщения.
        message_id: Исходный AMQP ``message_id`` (обычно event_id).
        correlation_id: Исходный ``correlation_id`` (payment_id).
        confirm_timeout_seconds: Таймаут ожидания broker confirm.
        exchange: Exchange (по умолчанию ``payments.exchange``).

    Returns:
        Новое значение ``system_attempt`` в заголовке retry-сообщения.

    Raises:
        OutboxPublishError: нет маршрута retry, timeout, return, nack.
        DeliveryError: unroutable mandatory.
    """
    routing_key = system_retry_routing_key(
        current_system_attempt=current_system_attempt
    )
    next_attempt = next_system_attempt(current_system_attempt=current_system_attempt)
    if routing_key is None or next_attempt is None:
        raise OutboxPublishError("system retry exhausted; caller must reject to DLQ")

    target_exchange: RabbitExchange | str = exchange or PAYMENTS_EXCHANGE
    try:
        confirmation = await broker.publish(
            body,
            exchange=target_exchange,
            routing_key=routing_key,
            mandatory=True,
            persist=True,
            message_id=message_id,
            correlation_id=correlation_id,
            content_type="application/json",
            headers={SYSTEM_ATTEMPT_HEADER: next_attempt},
            timeout=confirm_timeout_seconds,
        )
    except DeliveryError as exc:
        raise OutboxPublishError("system retry mandatory return (unroutable)") from exc
    except TimeoutError as exc:
        raise OutboxPublishError("system retry broker confirm timeout") from exc

    assert_broker_confirmed(confirmation)
    logger.info(
        "System retry published",
        extra={
            "routing_key": routing_key,
            "system_attempt": next_attempt,
            "message_id": message_id,
            "correlation_id": correlation_id,
        },
    )
    return next_attempt
