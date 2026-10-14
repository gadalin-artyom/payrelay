from __future__ import annotations

import logging
import uuid
from typing import Any

from aio_pika.exceptions import DeliveryError
from faststream import AckPolicy
from faststream.rabbit import RabbitBroker, RabbitMessage
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from payrelay.application.process_payment import (
    ProcessOutcome,
    ProcessPaymentService,
    ProcessResult,
)
from payrelay.core.config import Settings
from payrelay.domain.enums import OutboxEventType
from payrelay.domain.webhook_retry_rules import (
    MAX_SYSTEM_ATTEMPTS,
    SYSTEM_ATTEMPT_HEADER,
    is_valid_event_attempt,
    parse_system_attempt,
)
from payrelay.infrastructure.messaging.publisher import OutboxPublishError
from payrelay.infrastructure.messaging.system_retry import publish_system_retry
from payrelay.infrastructure.messaging.topology import (
    PaymentsTopology,
    build_payments_topology,
)

logger = logging.getLogger(__name__)

SUPPORTED_SCHEMA_VERSION = 1
_SUPPORTED_EVENT_TYPES = frozenset(
    {
        OutboxEventType.PAYMENT_CREATED.value,
        OutboxEventType.PAYMENT_WEBHOOK_RETRY.value,
    }
)


class PaymentEventMessage(BaseModel):
    """Валидируемое тело AMQP ``payment.created`` / ``payment.webhook_retry``."""

    model_config = ConfigDict(extra="forbid")

    schema_version: int
    event_id: uuid.UUID
    payment_id: uuid.UUID
    event_type: str
    attempt: int = Field(..., ge=1, le=3)

    @field_validator("schema_version")
    @classmethod
    def _schema_version_supported(cls, value: int) -> int:
        """Проверить версию схемы.

        Args:
            value: schema_version.

        Returns:
            То же значение.

        Raises:
            ValueError: при неподдерживаемой версии.
        """
        if value != SUPPORTED_SCHEMA_VERSION:
            msg = f"unsupported schema_version: {value}"
            raise ValueError(msg)
        return value

    @field_validator("event_type")
    @classmethod
    def _event_type_supported(cls, value: str) -> str:
        """Проверить поддерживаемый тип события.

        Args:
            value: event_type.

        Returns:
            То же значение.

        Raises:
            ValueError: при неизвестном типе.
        """
        if value not in _SUPPORTED_EVENT_TYPES:
            msg = f"unsupported event_type: {value}"
            raise ValueError(msg)
        return value

    @model_validator(mode="after")
    def _attempt_matches_event_type(self) -> PaymentEventMessage:
        """Проверить согласованность event_type и attempt.

        Returns:
            Валидированная модель.

        Raises:
            ValueError: при несовместимой паре type/attempt.
        """
        if not is_valid_event_attempt(
            event_type=self.event_type,
            attempt=self.attempt,
        ):
            msg = f"incompatible event_type/attempt: {self.event_type!r}/{self.attempt}"
            raise ValueError(msg)
        return self


# Обратная совместимость имени для существующих импортов/тестов.
PaymentCreatedMessage = PaymentEventMessage


def _log_meta_without_payload(
    *,
    event_type: str | None,
    event_id: str | None,
    payment_id: str | None,
    reason: str,
) -> None:
    """Залогировать метаданные без тела сообщения.

    Args:
        event_type: Тип события или ``None``.
        event_id: Идентификатор события или ``None``.
        payment_id: Идентификатор платежа или ``None``.
        reason: Причина отклонения.
    """
    logger.warning(
        "Rejecting AMQP message",
        extra={
            "event_type": event_type,
            "event_id": event_id,
            "payment_id": payment_id,
            "reason": reason,
        },
    )


def _message_ids(message: RabbitMessage, event: PaymentEventMessage) -> tuple[str, str]:
    """Извлечь message_id / correlation_id для system retry.

    Args:
        message: Входящее AMQP-сообщение.
        event: Валидированный payload.

    Returns:
        Кортеж ``(message_id, correlation_id)``.
    """
    raw = message.raw_message
    message_id = getattr(raw, "message_id", None) or str(event.event_id)
    correlation_id = getattr(raw, "correlation_id", None) or str(event.payment_id)
    return str(message_id), str(correlation_id)


async def apply_process_result(message: RabbitMessage, result: ProcessResult) -> None:
    """Применить AMQP ack/reject по исходу application-слоя.

    Args:
        message: Входящее RabbitMQ-сообщение.
        result: Результат ``ProcessPaymentService``.
    """
    if result.outcome is ProcessOutcome.ACK:
        await message.ack()
        return
    if result.outcome is ProcessOutcome.REJECT:
        await message.reject(requeue=False)
        return
    # RETRY_LATER: не ack и не hot nack(requeue=True) — оставляем unacked
    # до закрытия канала / восстановления; вызывающий может nack(requeue=True)
    # один раз после паузы. Здесь — nack без tight loop (requeue=True один раз).
    await message.nack(requeue=True)


async def handle_system_failure(
    *,
    broker: RabbitBroker,
    message: RabbitMessage,
    body: dict[str, Any],
    event: PaymentEventMessage,
    settings: Settings,
    topology: PaymentsTopology,
) -> None:
    """Обработать инфраструктурный сбой через TTL system retry или DLQ.

    Args:
        broker: RabbitBroker для confirm-publish.
        message: Исходное сообщение (ещё не ACK).
        body: Исходный payload (без изменений).
        event: Валидированное событие.
        settings: Настройки (timeout confirm).
        topology: Топология exchanges/queues.
    """
    system_attempt = parse_system_attempt(message.headers.get(SYSTEM_ATTEMPT_HEADER))
    if system_attempt is None:
        _log_meta_without_payload(
            event_type=event.event_type,
            event_id=str(event.event_id),
            payment_id=str(event.payment_id),
            reason="invalid_system_attempt",
        )
        await message.reject(requeue=False)
        return

    if system_attempt >= MAX_SYSTEM_ATTEMPTS:
        logger.error(
            "System attempts exhausted; reject to DLQ",
            extra={
                "event_id": str(event.event_id),
                "payment_id": str(event.payment_id),
                "system_attempt": system_attempt,
            },
        )
        await message.reject(requeue=False)
        return

    message_id, correlation_id = _message_ids(message, event)
    try:
        await publish_system_retry(
            broker,
            body=body,
            current_system_attempt=system_attempt,
            message_id=message_id,
            correlation_id=correlation_id,
            confirm_timeout_seconds=settings.outbox_publish_timeout_seconds,
            exchange=topology.payments_exchange,
        )
    except (OutboxPublishError, DeliveryError, Exception):
        logger.exception(
            "System retry publish failed; leaving message unacked",
            extra={
                "event_id": str(event.event_id),
                "payment_id": str(event.payment_id),
                "system_attempt": system_attempt,
            },
        )
        # Не ACK: сообщение вернётся после восстановления канала/worker.
        return

    await message.ack()


def register_payments_consumer(
    broker: RabbitBroker,
    service: ProcessPaymentService,
    *,
    settings: Settings | None = None,
    topology: PaymentsTopology | None = None,
) -> None:
    """Зарегистрировать единственный handler на ``payments.new``.

    Args:
        broker: RabbitBroker.
        service: Application-сервис обработки.
        settings: Настройки (для system retry confirm timeout).
        topology: Опционально уже собранная топология.
    """
    topo = topology or build_payments_topology()
    cfg = settings if settings is not None else service.settings

    @broker.subscriber(
        topo.payments_new,
        topo.payments_exchange,
        ack_policy=AckPolicy.MANUAL,
    )
    async def handle_payment_event(
        body: dict[str, Any],
        message: RabbitMessage,
    ) -> None:
        """Обработать одно сообщение очереди ``payments.new``.

        Args:
            body: Декодированный JSON.
            message: Объект сообщения для ручного ACK.
        """
        raw_type = body.get("event_type") if isinstance(body, dict) else None
        if raw_type not in _SUPPORTED_EVENT_TYPES:
            _log_meta_without_payload(
                event_type=str(raw_type) if raw_type is not None else None,
                event_id=str(body.get("event_id")) if isinstance(body, dict) else None,
                payment_id=(
                    str(body.get("payment_id")) if isinstance(body, dict) else None
                ),
                reason="unsupported_event_type",
            )
            await message.reject(requeue=False)
            return

        try:
            event = PaymentEventMessage.model_validate(body)
        except ValidationError:
            _log_meta_without_payload(
                event_type=str(raw_type) if raw_type is not None else None,
                event_id=str(body.get("event_id")) if isinstance(body, dict) else None,
                payment_id=(
                    str(body.get("payment_id")) if isinstance(body, dict) else None
                ),
                reason="invalid_payload",
            )
            await message.reject(requeue=False)
            return

        # Невалидный system_attempt у входящего — сразу DLQ (poison).
        if parse_system_attempt(message.headers.get(SYSTEM_ATTEMPT_HEADER)) is None:
            _log_meta_without_payload(
                event_type=event.event_type,
                event_id=str(event.event_id),
                payment_id=str(event.payment_id),
                reason="invalid_system_attempt",
            )
            await message.reject(requeue=False)
            return

        try:
            result = await service.process_payment_event(
                payment_id=event.payment_id,
                event_id=event.event_id,
                event_type=event.event_type,
                attempt=event.attempt,
            )
        except Exception:
            logger.exception(
                "Infrastructure failure during payment processing",
                extra={
                    "event_id": str(event.event_id),
                    "payment_id": str(event.payment_id),
                    "event_type": event.event_type,
                    "attempt": event.attempt,
                },
            )
            await handle_system_failure(
                broker=broker,
                message=message,
                body=body if isinstance(body, dict) else dict(body),
                event=event,
                settings=cfg,
                topology=topo,
            )
            return

        await apply_process_result(message, result)

    _ = handle_payment_event
