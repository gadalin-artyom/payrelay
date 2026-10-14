"""Диспетчер Outbox: claim → publish confirm → mark published."""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payrelay.core.config import Settings
from payrelay.domain.outbox_rules import publish_backoff_seconds
from payrelay.infrastructure.db.repositories import ClaimedOutboxRow, OutboxRepository
from payrelay.infrastructure.messaging.publisher import OutboxAmqpPublisher

logger = logging.getLogger(__name__)

SessionFactory = async_sessionmaker[AsyncSession]
MarkPublishedHook = Callable[[uuid.UUID, datetime], Awaitable[None]]


class StopSignal(Protocol):
    """Протокол сигнала остановки (например ``asyncio.Event``)."""

    def is_set(self) -> bool:
        """Проверить, запрошена ли остановка.

        Returns:
            ``True``, если цикл должен завершиться.
        """
        ...


@dataclass(slots=True)
class OutboxDispatcher:
    """Поллинг Outbox с одной записью на claim и at-least-once publish.

    Транзакция PostgreSQL никогда не держится открытой во время RabbitMQ publish.
    """

    session_factory: SessionFactory
    publisher: OutboxAmqpPublisher
    settings: Settings
    _stopping: bool = False
    _on_before_mark_published: MarkPublishedHook | None = None

    def request_stop(self) -> None:
        """Прекратить новые claims после завершения текущей операции."""
        self._stopping = True

    @property
    def is_stopping(self) -> bool:
        """Признак запрошенной остановки.

        Returns:
            ``True``, если новые claims запрещены.
        """
        return self._stopping

    def set_before_mark_published_hook(self, hook: MarkPublishedHook | None) -> None:
        """Установить тестовый hook между confirm и mark-published.

        Args:
            hook: Async-функция ``(outbox_id, claim_token)`` или ``None``.
        """
        self._on_before_mark_published = hook

    async def process_one(self) -> bool:
        """Захватить и обработать одну доступную запись.

        Returns:
            ``True``, если запись была захвачена (успех или ошибка publish);
            ``False``, если доступных записей нет или остановка запрошена.
        """
        if self._stopping:
            return False

        claimed = await self._claim_one()
        if claimed is None:
            return False

        started = time.perf_counter()
        event_id = str(claimed.id)
        payment_id = str(claimed.aggregate_id)
        publish_attempt = claimed.attempts

        try:
            await self.publisher.publish_outbox_payload(
                outbox_id=claimed.id,
                aggregate_id=claimed.aggregate_id,
                payload=claimed.payload,
            )
        except Exception as exc:
            duration_ms = int((time.perf_counter() - started) * 1000)
            logger.warning(
                "Outbox publish failed",
                extra={
                    "event_id": event_id,
                    "payment_id": payment_id,
                    "publish_attempt": publish_attempt,
                    "duration_ms": duration_ms,
                    "error_type": type(exc).__name__,
                },
            )
            await self._release_after_failure(claimed)
            return True

        if self._on_before_mark_published is not None:
            await self._on_before_mark_published(claimed.id, claimed.claim_token)

        marked = await self._mark_published(claimed)
        duration_ms = int((time.perf_counter() - started) * 1000)
        if marked:
            logger.info(
                "Outbox published",
                extra={
                    "event_id": event_id,
                    "payment_id": payment_id,
                    "publish_attempt": publish_attempt,
                    "duration_ms": duration_ms,
                },
            )
        else:
            logger.warning(
                "Outbox broker confirmed but mark-published CAS missed",
                extra={
                    "event_id": event_id,
                    "payment_id": payment_id,
                    "publish_attempt": publish_attempt,
                    "duration_ms": duration_ms,
                },
            )
        return True

    async def recover_pending_webhooks(self) -> int:
        """Восстановить legacy pending webhook без Outbox retry (этап 004).

        Создаёт недостающие ``payment.webhook_retry`` для terminal+pending
        с ``webhook_attempts`` 1 или 2 при отсутствии активного lease.
        Идемпотентно за счёт уникального ключа Outbox.

        Returns:
            Число вставленных Outbox-строк.
        """
        if self._stopping:
            return 0
        async with self.session_factory() as session:
            async with session.begin():
                repo = OutboxRepository(session)
                inserted = await repo.recover_pending_webhook_retries(
                    batch_size=self.settings.webhook_recovery_batch_size,
                )
        if inserted:
            logger.info(
                "Recovered pending webhook retries",
                extra={"inserted": inserted},
            )
        return inserted

    async def run_tick(self) -> int:
        """Recovery pending webhook + публикация до ``outbox_batch_size`` событий.

        Returns:
            Число обработанных (захваченных) записей за тик.
        """
        try:
            await self.recover_pending_webhooks()
        except Exception:
            logger.exception("Webhook pending recovery tick failed")

        processed = 0
        limit = self.settings.outbox_batch_size
        while processed < limit and not self._stopping:
            handled = await self.process_one()
            if not handled:
                break
            processed += 1
        return processed

    async def _claim_one(self) -> ClaimedOutboxRow | None:
        """Короткая TX: claim одной строки.

        Returns:
            Захваченная запись или ``None``.
        """
        async with self.session_factory() as session:
            async with session.begin():
                repo = OutboxRepository(session)
                return await repo.claim_next(
                    lease_seconds=self.settings.outbox_lease_seconds,
                )

    async def _mark_published(self, claimed: ClaimedOutboxRow) -> bool:
        """Короткая TX: CAS mark published.

        Args:
            claimed: Ранее захваченная строка.

        Returns:
            ``True``, если CAS успешен.
        """
        async with self.session_factory() as session:
            async with session.begin():
                repo = OutboxRepository(session)
                return await repo.mark_published(
                    outbox_id=claimed.id,
                    claim_token=claimed.claim_token,
                )

    async def _release_after_failure(self, claimed: ClaimedOutboxRow) -> None:
        """Короткая TX: освободить claim и выставить backoff.

        Args:
            claimed: Ранее захваченная строка.
        """
        backoff = publish_backoff_seconds(
            attempts=claimed.attempts,
            max_delay_seconds=self.settings.outbox_retry_max_delay_seconds,
        )
        try:
            async with self.session_factory() as session:
                async with session.begin():
                    repo = OutboxRepository(session)
                    await repo.release_claim_with_backoff(
                        outbox_id=claimed.id,
                        claim_token=claimed.claim_token,
                        backoff_seconds=backoff,
                    )
        except Exception:
            # Lease истечёт сам; не теряем запись.
            logger.exception(
                "Failed to release outbox claim after publish error",
                extra={
                    "event_id": str(claimed.id),
                    "payment_id": str(claimed.aggregate_id),
                    "publish_attempt": claimed.attempts,
                },
            )


async def run_dispatcher_loop(
    dispatcher: OutboxDispatcher,
    *,
    stop_event: StopSignal,
    sleep: Callable[[float], Awaitable[None]],
) -> None:
    """Запустить bounded poll loop до сигнала остановки.

    Args:
        dispatcher: Экземпляр диспетчера.
        stop_event: Объект с методом ``is_set()`` (например ``asyncio.Event``).
        sleep: Async sleep (подменяется в unit-тестах).
    """
    poll = dispatcher.settings.outbox_poll_interval_seconds
    db_delay = dispatcher.settings.outbox_db_retry_delay_seconds

    while not stop_event.is_set() and not dispatcher.is_stopping:
        try:
            processed = await dispatcher.run_tick()
        except Exception:
            logger.exception("Outbox dispatcher tick failed (DB?)")
            await sleep(db_delay)
            continue

        if stop_event.is_set() or dispatcher.is_stopping:
            break
        if processed == 0:
            await sleep(poll)
