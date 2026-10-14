from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payrelay.core.config import Settings
from payrelay.domain.enums import OutboxEventType, PaymentStatus, WebhookDeliveryStatus
from payrelay.domain.webhook_retry_rules import (
    MAX_WEBHOOK_ATTEMPTS,
    is_valid_event_attempt,
    next_webhook_attempt,
)
from payrelay.infrastructure.db.models import PaymentModel
from payrelay.infrastructure.db.repositories import OutboxRepository, PaymentRepository
from payrelay.infrastructure.gateway.simulator import PaymentGateway
from payrelay.infrastructure.webhooks.http_client import (
    WebhookHttpClient,
    build_webhook_payload,
)

logger = logging.getLogger(__name__)

SessionFactory = async_sessionmaker[AsyncSession]
SleepFunc = Callable[[float], Awaitable[None]]
AsyncHook = Callable[[], Awaitable[None]]


class ProcessOutcome(StrEnum):
    """Исход обработки сообщения для решения ack/nack/reject."""

    ACK = "ack"
    REJECT = "reject"
    RETRY_LATER = "retry_later"


class _LeaseAcquireKind(StrEnum):
    """Внутренний результат попытки захвата lease."""

    CLAIMED = "claimed"
    ALREADY_DELIVERED = "already_delivered"
    BUSY = "busy"
    NOT_FOUND = "not_found"
    EXHAUSTED = "exhausted"


@dataclass(frozen=True, slots=True)
class ProcessResult:
    """Результат оркестрации одного события."""

    outcome: ProcessOutcome
    payment_id: uuid.UUID | None
    business_status: str | None
    webhook_status: str | None
    detail: str


@dataclass(frozen=True, slots=True)
class _LeaseAcquireResult:
    """Результат ожидания/захвата processing lease."""

    kind: _LeaseAcquireKind
    token: datetime | None = None


class StopSignal(Protocol):
    """Сигнал остановки consumer."""

    def is_set(self) -> bool:
        """Проверить запрос остановки.

        Returns:
            ``True``, если нужно прекратить ожидание lease.
        """
        ...


@dataclass(slots=True)
class ProcessPaymentService:
    """Один handler бизнес-логики: pending→terminal, затем webhook.

    Транзакции PostgreSQL короткие; sleep шлюза и HTTP вне TX.
    """

    session_factory: SessionFactory
    gateway: PaymentGateway
    webhook_client: WebhookHttpClient
    settings: Settings
    sleep: SleepFunc
    _on_after_terminal_commit: AsyncHook | None = field(default=None, repr=False)
    _on_after_webhook_http_ok: AsyncHook | None = field(default=None, repr=False)
    _on_after_webhook_failure_commit: AsyncHook | None = field(default=None, repr=False)

    def set_after_terminal_commit_hook(self, hook: AsyncHook | None) -> None:
        """Hook после commit terminal (для crash-тестов I12).

        Args:
            hook: Async callback или ``None``.
        """
        self._on_after_terminal_commit = hook

    def set_after_webhook_http_ok_hook(self, hook: AsyncHook | None) -> None:
        """Hook после HTTP 2xx до mark delivered (crash-тест I13).

        Args:
            hook: Async callback или ``None``.
        """
        self._on_after_webhook_http_ok = hook

    def set_after_webhook_failure_commit_hook(self, hook: AsyncHook | None) -> None:
        """Hook после commit failure+retry Outbox до ACK (crash-тесты).

        Args:
            hook: Async callback или ``None``.
        """
        self._on_after_webhook_failure_commit = hook

    async def process_payment_created(
        self,
        *,
        payment_id: uuid.UUID,
        event_id: uuid.UUID,
        stop: StopSignal | None = None,
    ) -> ProcessResult:
        """Обработать событие ``payment.created`` (attempt=1).

        Args:
            payment_id: UUID платежа из сообщения.
            event_id: UUID Outbox/event.
            stop: Опциональный сигнал остановки ожидания lease.

        Returns:
            ``ProcessResult`` с рекомендуемым AMQP-исходом.
        """
        return await self.process_payment_event(
            payment_id=payment_id,
            event_id=event_id,
            event_type=OutboxEventType.PAYMENT_CREATED.value,
            attempt=1,
            stop=stop,
        )

    async def process_payment_event(
        self,
        *,
        payment_id: uuid.UUID,
        event_id: uuid.UUID,
        event_type: str,
        attempt: int,
        stop: StopSignal | None = None,
    ) -> ProcessResult:
        """Обработать ``payment.created`` или ``payment.webhook_retry``.

        Args:
            payment_id: UUID платежа из сообщения.
            event_id: UUID Outbox/event.
            event_type: Тип события.
            attempt: Логическая попытка webhook (1..3).
            stop: Опциональный сигнал остановки ожидания lease.

        Returns:
            ``ProcessResult`` с рекомендуемым AMQP-исходом.

        Raises:
            Exception: инфраструктурные сбои БД (вызывающий не должен ACK).
        """
        started = time.perf_counter()
        if not is_valid_event_attempt(event_type=event_type, attempt=attempt):
            logger.error(
                "Invalid event_type/attempt combination",
                extra={
                    "payment_id": str(payment_id),
                    "event_id": str(event_id),
                    "event_type": event_type,
                    "attempt": attempt,
                },
            )
            return ProcessResult(
                outcome=ProcessOutcome.REJECT,
                payment_id=payment_id,
                business_status=None,
                webhook_status=None,
                detail="invalid_event_attempt",
            )

        payment = await self._load_payment(payment_id)
        if payment is None:
            logger.error(
                "Payment not found for event",
                extra={"payment_id": str(payment_id), "event_id": str(event_id)},
            )
            return ProcessResult(
                outcome=ProcessOutcome.REJECT,
                payment_id=payment_id,
                business_status=None,
                webhook_status=None,
                detail="payment_not_found",
            )

        early = await self._evaluate_without_lease(
            payment=payment,
            payment_id=payment_id,
            event_id=event_id,
            event_type=event_type,
            attempt=attempt,
            started=started,
        )
        if early is not None:
            return early

        acquire = await self._acquire_lease(payment_id=payment_id, stop=stop)
        if acquire.kind is _LeaseAcquireKind.NOT_FOUND:
            return ProcessResult(
                outcome=ProcessOutcome.REJECT,
                payment_id=payment_id,
                business_status=None,
                webhook_status=None,
                detail="payment_not_found",
            )
        if acquire.kind is _LeaseAcquireKind.ALREADY_DELIVERED:
            payment = await self._load_payment(payment_id)
            status = payment.status if payment else None
            webhook = payment.webhook_delivery_status if payment else None
            self._log_done(
                event_id=event_id,
                payment_id=payment_id,
                business_status=status,
                webhook_status=webhook,
                started=started,
                detail="already_delivered",
            )
            return ProcessResult(
                outcome=ProcessOutcome.ACK,
                payment_id=payment_id,
                business_status=status,
                webhook_status=webhook,
                detail="already_delivered",
            )
        if acquire.kind is _LeaseAcquireKind.EXHAUSTED:
            payment = await self._load_payment(payment_id)
            status = payment.status if payment else None
            # Повтор последней попытки после exhausted → снова reject в DLQ.
            if attempt == MAX_WEBHOOK_ATTEMPTS:
                return ProcessResult(
                    outcome=ProcessOutcome.REJECT,
                    payment_id=payment_id,
                    business_status=status,
                    webhook_status=WebhookDeliveryStatus.EXHAUSTED.value,
                    detail="webhook_exhausted_redelivery",
                )
            return ProcessResult(
                outcome=ProcessOutcome.ACK,
                payment_id=payment_id,
                business_status=status,
                webhook_status=WebhookDeliveryStatus.EXHAUSTED.value,
                detail="webhook_exhausted_stale_event",
            )
        if acquire.kind is _LeaseAcquireKind.BUSY or acquire.token is None:
            return ProcessResult(
                outcome=ProcessOutcome.RETRY_LATER,
                payment_id=payment_id,
                business_status=None,
                webhook_status=None,
                detail="lease_busy_or_shutdown",
            )

        lease_token = acquire.token
        try:
            return await self._process_under_lease(
                payment_id=payment_id,
                event_id=event_id,
                event_type=event_type,
                attempt=attempt,
                lease_token=lease_token,
                started=started,
            )
        except Exception:
            logger.exception(
                "Payment processing infrastructure failure",
                extra={
                    "payment_id": str(payment_id),
                    "event_id": str(event_id),
                    "attempt": attempt,
                },
            )
            raise

    async def _evaluate_without_lease(
        self,
        *,
        payment: PaymentModel,
        payment_id: uuid.UUID,
        event_id: uuid.UUID,
        event_type: str,
        attempt: int,
        started: float,
    ) -> ProcessResult | None:
        """Быстрые идемпотентные исходы без захвата lease.

        Args:
            payment: Текущая строка платежа.
            payment_id: UUID платежа.
            event_id: UUID события.
            event_type: Тип события.
            attempt: Логическая попытка.
            started: perf_counter старта.

        Returns:
            Готовый ``ProcessResult`` либо ``None``, если нужна дальнейшая обработка.
        """
        if self._is_delivered(payment):
            self._log_done(
                event_id=event_id,
                payment_id=payment_id,
                business_status=payment.status,
                webhook_status=payment.webhook_delivery_status,
                started=started,
                detail="already_delivered",
            )
            return ProcessResult(
                outcome=ProcessOutcome.ACK,
                payment_id=payment_id,
                business_status=payment.status,
                webhook_status=payment.webhook_delivery_status,
                detail="already_delivered",
            )

        if payment.webhook_delivery_status == WebhookDeliveryStatus.EXHAUSTED.value:
            if attempt == MAX_WEBHOOK_ATTEMPTS:
                return ProcessResult(
                    outcome=ProcessOutcome.REJECT,
                    payment_id=payment_id,
                    business_status=payment.status,
                    webhook_status=WebhookDeliveryStatus.EXHAUSTED.value,
                    detail="webhook_exhausted_redelivery",
                )
            return ProcessResult(
                outcome=ProcessOutcome.ACK,
                payment_id=payment_id,
                business_status=payment.status,
                webhook_status=WebhookDeliveryStatus.EXHAUSTED.value,
                detail="webhook_exhausted_stale_event",
            )

        # Устаревшее событие: уже продвинулись дальше этой попытки.
        if payment.webhook_attempts > attempt:
            self._log_done(
                event_id=event_id,
                payment_id=payment_id,
                business_status=payment.status,
                webhook_status=payment.webhook_delivery_status,
                started=started,
                detail="stale_lower_attempt",
            )
            return ProcessResult(
                outcome=ProcessOutcome.ACK,
                payment_id=payment_id,
                business_status=payment.status,
                webhook_status=payment.webhook_delivery_status,
                detail="stale_lower_attempt",
            )

        # После commit failure+retry: redelivery не повторяет HTTP.
        if (
            payment.webhook_attempts == attempt
            and payment.webhook_delivery_status == WebhookDeliveryStatus.PENDING.value
        ):
            nxt = next_webhook_attempt(failed_attempt=attempt)
            if nxt is not None and await self._has_webhook_retry(payment_id, nxt):
                self._log_done(
                    event_id=event_id,
                    payment_id=payment_id,
                    business_status=payment.status,
                    webhook_status=payment.webhook_delivery_status,
                    started=started,
                    detail="retry_already_scheduled",
                )
                return ProcessResult(
                    outcome=ProcessOutcome.ACK,
                    payment_id=payment_id,
                    business_status=payment.status,
                    webhook_status=payment.webhook_delivery_status,
                    detail="retry_already_scheduled",
                )

        # Преждевременный event (attempt слишком большой относительно DB stage).
        if (
            payment.status != PaymentStatus.PENDING.value
            and payment.webhook_attempts < attempt - 1
        ):
            logger.warning(
                "Premature webhook attempt event; ack without HTTP",
                extra={
                    "payment_id": str(payment_id),
                    "event_id": str(event_id),
                    "event_type": event_type,
                    "attempt": attempt,
                    "webhook_attempts": payment.webhook_attempts,
                },
            )
            return ProcessResult(
                outcome=ProcessOutcome.ACK,
                payment_id=payment_id,
                business_status=payment.status,
                webhook_status=payment.webhook_delivery_status,
                detail="premature_attempt_quarantine",
            )

        return None

    async def _process_under_lease(
        self,
        *,
        payment_id: uuid.UUID,
        event_id: uuid.UUID,
        event_type: str,
        attempt: int,
        lease_token: datetime,
        started: float,
    ) -> ProcessResult:
        """Выполнить gateway/webhook под удерживаемым lease.

        Args:
            payment_id: UUID платежа.
            event_id: UUID события.
            event_type: Тип события.
            attempt: Логическая попытка webhook.
            lease_token: Owner token.
            started: perf_counter старта.

        Returns:
            Исход обработки.
        """
        payment = await self._load_payment(payment_id)
        if payment is None:
            await self._release_lease(payment_id, lease_token)
            return ProcessResult(
                outcome=ProcessOutcome.REJECT,
                payment_id=payment_id,
                business_status=None,
                webhook_status=None,
                detail="payment_not_found_after_claim",
            )

        early = await self._evaluate_without_lease(
            payment=payment,
            payment_id=payment_id,
            event_id=event_id,
            event_type=event_type,
            attempt=attempt,
            started=started,
        )
        if early is not None:
            await self._release_lease(payment_id, lease_token)
            return early

        # Gateway только для payment.created / pending.
        if (
            event_type == OutboxEventType.PAYMENT_CREATED.value
            and payment.status == PaymentStatus.PENDING.value
        ):
            try:
                gateway_status = await self.gateway.charge(payment_id)
            except Exception:
                await self._release_lease(payment_id, lease_token)
                raise

            committed = await self._commit_terminal(
                payment_id=payment_id,
                lease_token=lease_token,
                status=gateway_status,
            )
            if not committed:
                payment = await self._load_payment(payment_id)
                if payment is None:
                    return ProcessResult(
                        outcome=ProcessOutcome.REJECT,
                        payment_id=payment_id,
                        business_status=None,
                        webhook_status=None,
                        detail="payment_missing_after_cas",
                    )
                if payment.status == PaymentStatus.PENDING.value:
                    await self._release_lease(payment_id, lease_token)
                    return ProcessResult(
                        outcome=ProcessOutcome.RETRY_LATER,
                        payment_id=payment_id,
                        business_status=payment.status,
                        webhook_status=payment.webhook_delivery_status,
                        detail="terminal_cas_miss",
                    )
            if self._on_after_terminal_commit is not None:
                await self._on_after_terminal_commit()

        payment = await self._load_payment(payment_id)
        if payment is None:
            return ProcessResult(
                outcome=ProcessOutcome.REJECT,
                payment_id=payment_id,
                business_status=None,
                webhook_status=None,
                detail="payment_missing_before_webhook",
            )
        if payment.status == PaymentStatus.PENDING.value:
            # webhook_retry не должен видеть pending; payment.created — ждать.
            await self._release_lease(payment_id, lease_token)
            if event_type == OutboxEventType.PAYMENT_WEBHOOK_RETRY.value:
                return ProcessResult(
                    outcome=ProcessOutcome.REJECT,
                    payment_id=payment_id,
                    business_status=payment.status,
                    webhook_status=payment.webhook_delivery_status,
                    detail="retry_on_non_terminal",
                )
            return ProcessResult(
                outcome=ProcessOutcome.RETRY_LATER,
                payment_id=payment_id,
                business_status=payment.status,
                webhook_status=payment.webhook_delivery_status,
                detail="still_pending",
            )

        early = await self._evaluate_without_lease(
            payment=payment,
            payment_id=payment_id,
            event_id=event_id,
            event_type=event_type,
            attempt=attempt,
            started=started,
        )
        if early is not None:
            await self._release_lease(payment_id, lease_token)
            return early

        await self._mark_attempt_started(payment_id, lease_token, attempt=attempt)

        processed_at = payment.processed_at or datetime.now(UTC)
        payload = build_webhook_payload(
            payment_id=payment_id,
            status=payment.status,
            amount=payment.amount,
            currency=payment.currency,
            processed_at=processed_at,
        )
        delivery = await self.webhook_client.deliver(
            url=payment.webhook_url,
            payload=payload,
        )

        if delivery.ok:
            if self._on_after_webhook_http_ok is not None:
                await self._on_after_webhook_http_ok()
            marked = await self._mark_delivered(payment_id, lease_token)
            if not marked:
                msg = "failed to persist webhook delivered"
                raise RuntimeError(msg)
            self._log_done(
                event_id=event_id,
                payment_id=payment_id,
                business_status=payment.status,
                webhook_status=WebhookDeliveryStatus.DELIVERED.value,
                started=started,
                detail="webhook_delivered",
            )
            return ProcessResult(
                outcome=ProcessOutcome.ACK,
                payment_id=payment_id,
                business_status=payment.status,
                webhook_status=WebhookDeliveryStatus.DELIVERED.value,
                detail="webhook_delivered",
            )

        return await self._handle_webhook_http_failure(
            payment_id=payment_id,
            event_id=event_id,
            attempt=attempt,
            lease_token=lease_token,
            business_status=payment.status,
            started=started,
            error_class=delivery.error_class,
            status_code=delivery.status_code,
        )

    async def _handle_webhook_http_failure(
        self,
        *,
        payment_id: uuid.UUID,
        event_id: uuid.UUID,
        attempt: int,
        lease_token: datetime,
        business_status: str,
        started: float,
        error_class: str | None,
        status_code: int | None,
    ) -> ProcessResult:
        """Зафиксировать HTTP-неуспех: retry Outbox или exhausted+REJECT.

        Args:
            payment_id: UUID платежа.
            event_id: UUID текущего AMQP/Outbox события.
            attempt: Номер завершившейся попытки.
            lease_token: Owner token.
            business_status: Неизменяемый terminal status.
            started: perf_counter старта.
            error_class: Класс ошибки HTTP-клиента.
            status_code: HTTP status code или ``None``.

        Returns:
            ACK после планирования retry либо REJECT после exhausted.

        Raises:
            RuntimeError: если CAS/фиксация результата не удалась.
        """
        delay = self.settings.webhook_retry_delay_for_failed_attempt(attempt)
        next_attempt = next_webhook_attempt(failed_attempt=attempt)

        if next_attempt is not None and delay is not None:
            ok = await self._record_failure_and_retry(
                payment_id=payment_id,
                lease_token=lease_token,
                failed_attempt=attempt,
                delay_seconds=delay,
            )
            if not ok:
                msg = "failed to persist webhook failure and retry outbox"
                raise RuntimeError(msg)
            if self._on_after_webhook_failure_commit is not None:
                await self._on_after_webhook_failure_commit()
            logger.warning(
                "Webhook delivery failed; retry scheduled via outbox",
                extra={
                    "payment_id": str(payment_id),
                    "event_id": str(event_id),
                    "attempt": attempt,
                    "next_attempt": next_attempt,
                    "delay_seconds": delay,
                    "error_class": error_class,
                    "status_code": status_code,
                },
            )
            self._log_done(
                event_id=event_id,
                payment_id=payment_id,
                business_status=business_status,
                webhook_status=WebhookDeliveryStatus.PENDING.value,
                started=started,
                detail="webhook_failed_retry_scheduled",
            )
            return ProcessResult(
                outcome=ProcessOutcome.ACK,
                payment_id=payment_id,
                business_status=business_status,
                webhook_status=WebhookDeliveryStatus.PENDING.value,
                detail="webhook_failed_retry_scheduled",
            )

        exhausted = await self._mark_exhausted(
            payment_id=payment_id,
            lease_token=lease_token,
            failed_attempt=attempt,
        )
        if not exhausted:
            msg = "failed to persist webhook exhausted"
            raise RuntimeError(msg)
        if self._on_after_webhook_failure_commit is not None:
            await self._on_after_webhook_failure_commit()
        logger.warning(
            "Webhook delivery exhausted; reject to DLQ",
            extra={
                "payment_id": str(payment_id),
                "event_id": str(event_id),
                "attempt": attempt,
                "error_class": error_class,
                "status_code": status_code,
            },
        )
        self._log_done(
            event_id=event_id,
            payment_id=payment_id,
            business_status=business_status,
            webhook_status=WebhookDeliveryStatus.EXHAUSTED.value,
            started=started,
            detail="webhook_exhausted",
        )
        return ProcessResult(
            outcome=ProcessOutcome.REJECT,
            payment_id=payment_id,
            business_status=business_status,
            webhook_status=WebhookDeliveryStatus.EXHAUSTED.value,
            detail="webhook_exhausted",
        )

    @staticmethod
    def _is_fully_done(payment: PaymentModel) -> bool:
        """Проверить terminal + webhook delivered (совместимость с тестами).

        Args:
            payment: Строка платежа.

        Returns:
            ``True``, если повторная обработка не нужна.
        """
        return ProcessPaymentService._is_delivered(payment)

    @staticmethod
    def _is_delivered(payment: PaymentModel) -> bool:
        """Проверить terminal + webhook delivered.

        Args:
            payment: Строка платежа.

        Returns:
            ``True``, если webhook уже доставлен.
        """
        return (
            payment.status != PaymentStatus.PENDING.value
            and payment.webhook_delivery_status == WebhookDeliveryStatus.DELIVERED.value
        )

    async def _load_payment(self, payment_id: uuid.UUID) -> PaymentModel | None:
        """Загрузить платёж по id.

        Args:
            payment_id: UUID.

        Returns:
            Модель или ``None``.
        """
        async with self.session_factory() as session:
            repo = PaymentRepository(session)
            return await repo.get_by_id(payment_id)

    async def _has_webhook_retry(
        self,
        payment_id: uuid.UUID,
        delivery_attempt: int,
    ) -> bool:
        """Проверить наличие Outbox ``payment.webhook_retry`` для попытки.

        Args:
            payment_id: UUID платежа.
            delivery_attempt: Номер попытки (2 или 3).

        Returns:
            ``True``, если запись существует.
        """
        async with self.session_factory() as session:
            repo = OutboxRepository(session)
            return await repo.has_delivery_attempt(
                payment_id=payment_id,
                event_type=OutboxEventType.PAYMENT_WEBHOOK_RETRY.value,
                delivery_attempt=delivery_attempt,
            )

    async def _acquire_lease(
        self,
        *,
        payment_id: uuid.UUID,
        stop: StopSignal | None,
    ) -> _LeaseAcquireResult:
        """Захватить lease с пассивным ожиданием чужого активного lease.

        Args:
            payment_id: UUID платежа.
            stop: Сигнал остановки.

        Returns:
            Результат захвата.
        """
        lease_seconds = self.settings.processing_lease_seconds
        wait_cap = lease_seconds + 2.0
        waited = 0.0
        while waited <= wait_cap:
            if stop is not None and stop.is_set():
                return _LeaseAcquireResult(kind=_LeaseAcquireKind.BUSY)

            async with self.session_factory() as session:
                async with session.begin():
                    repo = PaymentRepository(session)
                    payment = await repo.get_by_id(payment_id)
                    if payment is None:
                        return _LeaseAcquireResult(kind=_LeaseAcquireKind.NOT_FOUND)
                    if self._is_delivered(payment):
                        return _LeaseAcquireResult(
                            kind=_LeaseAcquireKind.ALREADY_DELIVERED
                        )
                    if (
                        payment.webhook_delivery_status
                        == WebhookDeliveryStatus.EXHAUSTED.value
                    ):
                        return _LeaseAcquireResult(kind=_LeaseAcquireKind.EXHAUSTED)

                    if payment.status == PaymentStatus.PENDING.value:
                        token = await repo.claim_pending_for_processing(
                            payment_id=payment_id,
                            lease_seconds=lease_seconds,
                        )
                    else:
                        token = await repo.claim_terminal_for_webhook(
                            payment_id=payment_id,
                            lease_seconds=lease_seconds,
                        )
                    if token is not None:
                        return _LeaseAcquireResult(
                            kind=_LeaseAcquireKind.CLAIMED,
                            token=token,
                        )

                    lease_until = payment.processing_lease_until
                    if lease_until is not None and lease_until.tzinfo is None:
                        lease_until = lease_until.replace(tzinfo=UTC)
                    now = datetime.now(UTC)
                    if lease_until is not None and lease_until > now:
                        remaining = (lease_until - now).total_seconds()
                        pause = min(1.0, max(0.05, remaining))
                    else:
                        pause = 0.2

            await self.sleep(pause)
            waited += pause

        return _LeaseAcquireResult(kind=_LeaseAcquireKind.BUSY)

    async def _commit_terminal(
        self,
        *,
        payment_id: uuid.UUID,
        lease_token: datetime,
        status: PaymentStatus,
    ) -> bool:
        """Зафиксировать терминальный статус в короткой TX.

        Args:
            payment_id: UUID.
            lease_token: Owner token.
            status: Бизнес-итог.

        Returns:
            Успех CAS.
        """
        async with self.session_factory() as session:
            async with session.begin():
                repo = PaymentRepository(session)
                return await repo.commit_terminal_status(
                    payment_id=payment_id,
                    lease_token=lease_token,
                    status=status,
                )

    async def _mark_attempt_started(
        self,
        payment_id: uuid.UUID,
        lease_token: datetime,
        *,
        attempt: int,
    ) -> None:
        """Записать ``webhook_attempts=attempt`` перед HTTP.

        Args:
            payment_id: UUID.
            lease_token: Owner token.
            attempt: Номер текущей логической попытки.

        Raises:
            RuntimeError: если CAS владельца не удался.
        """
        async with self.session_factory() as session:
            async with session.begin():
                repo = PaymentRepository(session)
                ok = await repo.mark_webhook_attempt_started(
                    payment_id=payment_id,
                    lease_token=lease_token,
                    attempt=attempt,
                )
        if not ok:
            msg = "webhook attempt CAS failed"
            raise RuntimeError(msg)

    async def _mark_delivered(
        self,
        payment_id: uuid.UUID,
        lease_token: datetime,
    ) -> bool:
        """Пометить webhook delivered.

        Args:
            payment_id: UUID.
            lease_token: Owner token.

        Returns:
            Успех CAS.
        """
        async with self.session_factory() as session:
            async with session.begin():
                repo = PaymentRepository(session)
                return await repo.mark_webhook_delivered(
                    payment_id=payment_id,
                    lease_token=lease_token,
                )

    async def _record_failure_and_retry(
        self,
        *,
        payment_id: uuid.UUID,
        lease_token: datetime,
        failed_attempt: int,
        delay_seconds: float,
    ) -> bool:
        """Атомарно снять lease и вставить retry Outbox.

        Args:
            payment_id: UUID.
            lease_token: Owner token.
            failed_attempt: Номер завершившейся попытки.
            delay_seconds: Задержка available_at.

        Returns:
            Успех CAS.
        """
        async with self.session_factory() as session:
            async with session.begin():
                payment_repo = PaymentRepository(session)
                outbox_repo = OutboxRepository(session)
                return await payment_repo.record_webhook_failure_and_schedule_retry(
                    payment_id=payment_id,
                    lease_token=lease_token,
                    failed_attempt=failed_attempt,
                    delay_seconds=delay_seconds,
                    outbox_repo=outbox_repo,
                )

    async def _mark_exhausted(
        self,
        *,
        payment_id: uuid.UUID,
        lease_token: datetime,
        failed_attempt: int,
    ) -> bool:
        """Пометить webhook exhausted.

        Args:
            payment_id: UUID.
            lease_token: Owner token.
            failed_attempt: Номер попытки (3).

        Returns:
            Успех CAS.
        """
        async with self.session_factory() as session:
            async with session.begin():
                repo = PaymentRepository(session)
                return await repo.mark_webhook_exhausted(
                    payment_id=payment_id,
                    lease_token=lease_token,
                    failed_attempt=failed_attempt,
                )

    async def _release_lease(
        self,
        payment_id: uuid.UUID,
        lease_token: datetime,
    ) -> None:
        """Освободить lease владельца.

        Args:
            payment_id: UUID.
            lease_token: Owner token.
        """
        async with self.session_factory() as session:
            async with session.begin():
                repo = PaymentRepository(session)
                await repo.release_processing_lease(
                    payment_id=payment_id,
                    lease_token=lease_token,
                )

    def _log_done(
        self,
        *,
        event_id: uuid.UUID,
        payment_id: uuid.UUID,
        business_status: str | None,
        webhook_status: str | None,
        started: float,
        detail: str,
    ) -> None:
        """Структурированный лог завершения обработки.

        Args:
            event_id: UUID события.
            payment_id: UUID платежа.
            business_status: Статус платежа.
            webhook_status: Статус webhook.
            started: perf_counter старта.
            detail: Краткая метка исхода.
        """
        duration_ms = int((time.perf_counter() - started) * 1000)
        logger.info(
            "Payment processing finished",
            extra={
                "event_id": str(event_id),
                "payment_id": str(payment_id),
                "business_status": business_status,
                "webhook_status": webhook_status,
                "duration_ms": duration_ms,
                "detail": detail,
            },
        )
