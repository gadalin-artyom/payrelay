"""FastStream consumer: подписка на ``payments.new`` с ручным ACK."""

from __future__ import annotations

import logging
import uuid
from typing import Any

from faststream import AckPolicy
from faststream.rabbit import RabbitBroker, RabbitMessage
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from payrelay.application.process_payment import (
    ProcessOutcome,
    ProcessPaymentService,
    ProcessResult,
)
from payrelay.domain.enums import OutboxEventType
from payrelay.infrastructure.messaging.topology import (
    PaymentsTopology,
    build_payments_topology,
)

logger = logging.getLogger(__name__)

SUPPORTED_SCHEMA_VERSION = 1


class PaymentCreatedMessage(BaseModel):
    """Валидируемое тело AMQP ``payment.created``."""

    model_config = ConfigDict(extra="forbid")

    schema_version: int
    event_id: uuid.UUID
    payment_id: uuid.UUID
    event_type: str
    attempt: int = Field(..., ge=1)

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


def _log_meta_without_payload(
    *,
    event_type: str | None,
    event_id: str | None,
    payment_id: str | None,
    reason: str,
) -> None:
    """Залогировать метаданные без полного payload.

    Args:
        event_type: Тип события или ``None``.
        event_id: Идентификатор события.
        payment_id: Идентификатор платежа.
        reason: Причина отклонения.
    """
    logger.error(
        "AMQP message rejected",
        extra={
            "event_type": event_type,
            "event_id": event_id,
            "payment_id": payment_id,
            "reason": reason,
        },
    )


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


def register_payments_consumer(
    broker: RabbitBroker,
    service: ProcessPaymentService,
    *,
    topology: PaymentsTopology | None = None,
) -> None:
    """Зарегистрировать единственный handler на ``payments.new``.

    Args:
        broker: RabbitBroker.
        service: Application-сервис обработки.
        topology: Опционально уже собранная топология.
    """
    topo = topology or build_payments_topology()

    @broker.subscriber(
        topo.payments_new,
        topo.payments_exchange,
        ack_policy=AckPolicy.MANUAL,
    )
    async def handle_payment_created(
        body: dict[str, Any],
        message: RabbitMessage,
    ) -> None:
        """Обработать одно сообщение очереди ``payments.new``.

        Args:
            body: Декодированный JSON.
            message: Объект сообщения для ручного ACK.
        """
        raw_type = body.get("event_type") if isinstance(body, dict) else None
        if raw_type != OutboxEventType.PAYMENT_CREATED.value:
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
            event = PaymentCreatedMessage.model_validate(body)
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

        try:
            result = await service.process_payment_created(
                payment_id=event.payment_id,
                event_id=event.event_id,
            )
        except Exception:
            logger.exception(
                "Infrastructure failure during payment processing; leaving unacked",
                extra={
                    "event_id": str(event.event_id),
                    "payment_id": str(event.payment_id),
                },
            )
            # MANUAL: без ack сообщение останется для broker redelivery.
            raise

        await apply_process_result(message, result)

    # Явная ссылка, чтобы линтер не считал handler неиспользуемым.
    _ = handle_payment_created
