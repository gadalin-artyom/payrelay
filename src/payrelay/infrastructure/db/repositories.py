"""Репозитории платежей и Outbox."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from payrelay.domain.enums import (
    OutboxEventType,
    PaymentStatus,
    WebhookDeliveryStatus,
)
from payrelay.infrastructure.db.models import OutboxModel, PaymentModel


@dataclass(frozen=True, slots=True)
class ClaimedOutboxRow:
    """Захваченная запись Outbox с claim token."""

    id: uuid.UUID
    aggregate_id: uuid.UUID
    event_type: str
    payload: dict[str, Any]
    delivery_attempt: int
    available_at: datetime
    published_at: datetime | None
    claimed_until: datetime
    attempts: int
    created_at: datetime

    @property
    def claim_token(self) -> datetime:
        """Точное значение ``claimed_until`` после claim (CAS-токен).

        Returns:
            Timestamp lease, выставленный БД.
        """
        return self.claimed_until


class PaymentRepository:
    """Доступ к таблице payments."""

    def __init__(self, session: AsyncSession) -> None:
        """Инициализировать репозиторий.

        Args:
            session: Текущая async-сессия SQLAlchemy.
        """
        self._session = session

    async def insert_payment_if_absent(
        self,
        *,
        payment_id: uuid.UUID,
        amount: Decimal,
        currency: str,
        description: str,
        metadata: dict[str, Any],
        idempotency_key: str,
        request_hash: str,
        webhook_url: str,
    ) -> uuid.UUID | None:
        """Вставить платёж; при конфликте по idempotency_key вернуть None.

        Использует ``INSERT ... ON CONFLICT (idempotency_key) DO NOTHING
        RETURNING id`` без гонки SELECT-then-INSERT.

        Args:
            payment_id: Новый UUID платежа.
            amount: Сумма.
            currency: Валюта.
            description: Описание.
            metadata: JSON metadata.
            idempotency_key: Ключ идемпотентности.
            request_hash: Канонический хэш тела.
            webhook_url: URL webhook.

        Returns:
            UUID вставленной строки либо ``None``, если ключ уже занят.
        """
        stmt = (
            insert(PaymentModel)
            .values(
                id=payment_id,
                amount=amount,
                currency=currency,
                description=description,
                metadata_=metadata,
                status=PaymentStatus.PENDING.value,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                webhook_url=webhook_url,
                webhook_delivery_status=WebhookDeliveryStatus.PENDING.value,
                webhook_attempts=0,
            )
            .on_conflict_do_nothing(index_elements=["idempotency_key"])
            .returning(PaymentModel.id)
        )
        result = await self._session.execute(stmt)
        inserted_id = result.scalar_one_or_none()
        return inserted_id

    async def get_by_idempotency_key(self, idempotency_key: str) -> PaymentModel | None:
        """Найти платёж по ключу идемпотентности.

        Args:
            idempotency_key: Ключ из заголовка.

        Returns:
            Модель платежа или ``None``.
        """
        stmt = select(PaymentModel).where(
            PaymentModel.idempotency_key == idempotency_key
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_id(self, payment_id: uuid.UUID) -> PaymentModel | None:
        """Найти платёж по идентификатору.

        Args:
            payment_id: UUID платежа.

        Returns:
            Модель платежа или ``None``.
        """
        stmt = select(PaymentModel).where(PaymentModel.id == payment_id)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def claim_pending_for_processing(
        self,
        *,
        payment_id: uuid.UUID,
        lease_seconds: float,
    ) -> datetime | None:
        """Атомарно захватить pending-платёж под processing lease.

        Args:
            payment_id: UUID платежа.
            lease_seconds: Длительность lease.

        Returns:
            ``processing_lease_until`` (claim token) либо ``None``, если
            захват невозможен (не pending / чужой активный lease).
        """
        stmt = text(
            """
            UPDATE payments
            SET processing_lease_until =
                now() + make_interval(secs => :lease_seconds)
            WHERE id = :payment_id
              AND status = :pending_status
              AND (
                    processing_lease_until IS NULL
                    OR processing_lease_until <= now()
                  )
            RETURNING processing_lease_until
            """
        )
        result = await self._session.execute(
            stmt,
            {
                "payment_id": payment_id,
                "lease_seconds": float(lease_seconds),
                "pending_status": PaymentStatus.PENDING.value,
            },
        )
        return result.scalar_one_or_none()

    async def claim_terminal_for_webhook(
        self,
        *,
        payment_id: uuid.UUID,
        lease_seconds: float,
    ) -> datetime | None:
        """Захватить lease для доставки webhook терминального платежа.

        Args:
            payment_id: UUID платежа.
            lease_seconds: Длительность lease.

        Returns:
            Claim token либо ``None``.
        """
        stmt = text(
            """
            UPDATE payments
            SET processing_lease_until =
                now() + make_interval(secs => :lease_seconds)
            WHERE id = :payment_id
              AND status IN (:succeeded, :failed)
              AND webhook_delivery_status = :webhook_pending
              AND (
                    processing_lease_until IS NULL
                    OR processing_lease_until <= now()
                  )
            RETURNING processing_lease_until
            """
        )
        result = await self._session.execute(
            stmt,
            {
                "payment_id": payment_id,
                "lease_seconds": float(lease_seconds),
                "succeeded": PaymentStatus.SUCCEEDED.value,
                "failed": PaymentStatus.FAILED.value,
                "webhook_pending": WebhookDeliveryStatus.PENDING.value,
            },
        )
        return result.scalar_one_or_none()

    async def commit_terminal_status(
        self,
        *,
        payment_id: uuid.UUID,
        lease_token: datetime,
        status: PaymentStatus,
    ) -> bool:
        """CAS: pending + owner token → терминальный статус (lease сохраняется).

        Args:
            payment_id: UUID платежа.
            lease_token: ``processing_lease_until`` владельца.
            status: ``succeeded`` или ``failed``.

        Returns:
            ``True``, если строка обновлена текущим владельцем.
        """
        if status not in {PaymentStatus.SUCCEEDED, PaymentStatus.FAILED}:
            msg = f"terminal status required, got {status!r}"
            raise ValueError(msg)
        stmt = text(
            """
            UPDATE payments
            SET
                status = :status,
                processed_at = now()
            WHERE id = :payment_id
              AND status = :pending_status
              AND processing_lease_until = :lease_token
            RETURNING id
            """
        )
        result = await self._session.execute(
            stmt,
            {
                "payment_id": payment_id,
                "status": status.value,
                "pending_status": PaymentStatus.PENDING.value,
                "lease_token": lease_token,
            },
        )
        return result.scalar_one_or_none() is not None

    async def mark_webhook_attempt_started(
        self,
        *,
        payment_id: uuid.UUID,
        lease_token: datetime,
        attempt: int = 1,
    ) -> bool:
        """Зафиксировать начало HTTP-попытки (CAS, без повторного инкремента).

        Для этапа 004 attempt=1: повторная AMQP-доставка той же первой попытки
        не увеличивает счётчик, если он уже равен ``attempt``.

        Args:
            payment_id: UUID платежа.
            lease_token: Owner token.
            attempt: Номер попытки доставки (в 004 всегда 1).

        Returns:
            ``True``, если владелец обновил строку или attempt уже установлен.
        """
        stmt = text(
            """
            UPDATE payments
            SET webhook_attempts = :attempt
            WHERE id = :payment_id
              AND processing_lease_until = :lease_token
              AND webhook_delivery_status = :webhook_pending
              AND status IN (:succeeded, :failed)
              AND webhook_attempts < :attempt
            RETURNING id
            """
        )
        result = await self._session.execute(
            stmt,
            {
                "payment_id": payment_id,
                "lease_token": lease_token,
                "attempt": int(attempt),
                "webhook_pending": WebhookDeliveryStatus.PENDING.value,
                "succeeded": PaymentStatus.SUCCEEDED.value,
                "failed": PaymentStatus.FAILED.value,
            },
        )
        if result.scalar_one_or_none() is not None:
            return True
        # Уже зафиксирован тот же attempt у текущего владельца — идемпотентно OK.
        check = text(
            """
            SELECT id
            FROM payments
            WHERE id = :payment_id
              AND processing_lease_until = :lease_token
              AND webhook_delivery_status = :webhook_pending
              AND webhook_attempts >= :attempt
            """
        )
        existing = await self._session.execute(
            check,
            {
                "payment_id": payment_id,
                "lease_token": lease_token,
                "webhook_pending": WebhookDeliveryStatus.PENDING.value,
                "attempt": int(attempt),
            },
        )
        return existing.scalar_one_or_none() is not None

    async def mark_webhook_delivered(
        self,
        *,
        payment_id: uuid.UUID,
        lease_token: datetime,
    ) -> bool:
        """Отметить webhook delivered и освободить lease (CAS).

        Args:
            payment_id: UUID платежа.
            lease_token: Owner token.

        Returns:
            ``True``, если обновление выполнено владельцем.
        """
        stmt = text(
            """
            UPDATE payments
            SET
                webhook_delivery_status = :delivered,
                webhook_delivered_at = now(),
                processing_lease_until = NULL
            WHERE id = :payment_id
              AND processing_lease_until = :lease_token
              AND webhook_delivery_status = :webhook_pending
            RETURNING id
            """
        )
        result = await self._session.execute(
            stmt,
            {
                "payment_id": payment_id,
                "lease_token": lease_token,
                "delivered": WebhookDeliveryStatus.DELIVERED.value,
                "webhook_pending": WebhookDeliveryStatus.PENDING.value,
            },
        )
        return result.scalar_one_or_none() is not None

    async def release_processing_lease(
        self,
        *,
        payment_id: uuid.UUID,
        lease_token: datetime,
    ) -> bool:
        """Освободить processing lease текущего владельца.

        Args:
            payment_id: UUID платежа.
            lease_token: Owner token.

        Returns:
            ``True``, если lease сброшен владельцем.
        """
        stmt = text(
            """
            UPDATE payments
            SET processing_lease_until = NULL
            WHERE id = :payment_id
              AND processing_lease_until = :lease_token
            RETURNING id
            """
        )
        result = await self._session.execute(
            stmt,
            {"payment_id": payment_id, "lease_token": lease_token},
        )
        return result.scalar_one_or_none() is not None


class OutboxRepository:
    """Доступ к таблице outbox."""

    def __init__(self, session: AsyncSession) -> None:
        """Инициализировать репозиторий.

        Args:
            session: Текущая async-сессия SQLAlchemy.
        """
        self._session = session

    async def insert_payment_created(
        self,
        *,
        payment_id: uuid.UUID,
    ) -> OutboxModel:
        """Создать запись Outbox ``payment.created`` (attempt=1).

        Args:
            payment_id: Идентификатор связанного платежа.

        Returns:
            Созданная модель Outbox.
        """
        event_id = uuid.uuid4()
        now = datetime.now(UTC)
        payload: dict[str, Any] = {
            "schema_version": 1,
            "event_id": str(event_id),
            "payment_id": str(payment_id),
            "event_type": OutboxEventType.PAYMENT_CREATED.value,
            "attempt": 1,
        }
        row = OutboxModel(
            id=event_id,
            aggregate_id=payment_id,
            event_type=OutboxEventType.PAYMENT_CREATED.value,
            payload=payload,
            delivery_attempt=1,
            available_at=now,
            published_at=None,
            claimed_until=None,
            attempts=0,
            created_at=now,
        )
        self._session.add(row)
        await self._session.flush()
        return row

    async def claim_next(
        self,
        *,
        lease_seconds: float,
    ) -> ClaimedOutboxRow | None:
        """Атомарно захватить одну доступную Outbox-запись.

        Использует ``SELECT ... FOR UPDATE SKIP LOCKED`` + ``UPDATE`` в одной
        короткой транзакции вызывающего кода. Время lease берётся из ``now()`` БД.

        Args:
            lease_seconds: Длительность lease в секундах.

        Returns:
            Захваченная строка либо ``None``, если доступных нет.
        """
        stmt = text(
            """
            WITH candidate AS (
                SELECT id
                FROM outbox
                WHERE published_at IS NULL
                  AND available_at <= now()
                  AND (claimed_until IS NULL OR claimed_until <= now())
                ORDER BY available_at, created_at, id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE outbox AS o
            SET
                attempts = o.attempts + 1,
                claimed_until = now() + make_interval(secs => :lease_seconds)
            FROM candidate AS c
            WHERE o.id = c.id
            RETURNING
                o.id,
                o.aggregate_id,
                o.event_type,
                o.payload,
                o.delivery_attempt,
                o.available_at,
                o.published_at,
                o.claimed_until,
                o.attempts,
                o.created_at
            """
        )
        result = await self._session.execute(
            stmt,
            {"lease_seconds": float(lease_seconds)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        claimed_until = row["claimed_until"]
        if claimed_until is None:  # pragma: no cover - инвариант UPDATE
            msg = "claimed_until must be set after claim"
            raise RuntimeError(msg)
        return ClaimedOutboxRow(
            id=row["id"],
            aggregate_id=row["aggregate_id"],
            event_type=row["event_type"],
            payload=dict(row["payload"]),
            delivery_attempt=int(row["delivery_attempt"]),
            available_at=row["available_at"],
            published_at=row["published_at"],
            claimed_until=claimed_until,
            attempts=int(row["attempts"]),
            created_at=row["created_at"],
        )

    async def mark_published(
        self,
        *,
        outbox_id: uuid.UUID,
        claim_token: datetime,
    ) -> bool:
        """Отметить публикацию только при совпадении claim token (CAS).

        Args:
            outbox_id: UUID Outbox.
            claim_token: ``claimed_until``, полученный при claim.

        Returns:
            ``True``, если строка обновлена; ``False``, если CAS не совпал.
        """
        stmt = text(
            """
            UPDATE outbox
            SET
                published_at = now(),
                claimed_until = NULL
            WHERE id = :outbox_id
              AND claimed_until = :claim_token
              AND published_at IS NULL
            RETURNING id
            """
        )
        result = await self._session.execute(
            stmt,
            {"outbox_id": outbox_id, "claim_token": claim_token},
        )
        return result.scalar_one_or_none() is not None

    async def release_claim_with_backoff(
        self,
        *,
        outbox_id: uuid.UUID,
        claim_token: datetime,
        backoff_seconds: float,
    ) -> bool:
        """Освободить claim после неуспешной публикации и отложить запись.

        Args:
            outbox_id: UUID Outbox.
            claim_token: ``claimed_until`` владельца claim.
            backoff_seconds: Задержка до следующей попытки публикации.

        Returns:
            ``True``, если строка обновлена текущим владельцем claim.
        """
        stmt = text(
            """
            UPDATE outbox
            SET
                claimed_until = NULL,
                available_at = now() + make_interval(secs => :backoff_seconds)
            WHERE id = :outbox_id
              AND claimed_until = :claim_token
              AND published_at IS NULL
            RETURNING id
            """
        )
        result = await self._session.execute(
            stmt,
            {
                "outbox_id": outbox_id,
                "claim_token": claim_token,
                "backoff_seconds": float(backoff_seconds),
            },
        )
        return result.scalar_one_or_none() is not None
