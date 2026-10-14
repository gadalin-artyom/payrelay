"""Сценарии создания и чтения платежей с идемпотентностью."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from payrelay.application.request_hash import compute_request_hash
from payrelay.domain.errors import IdempotencyConflictError, PaymentNotFoundError
from payrelay.infrastructure.db.models import PaymentModel
from payrelay.infrastructure.db.repositories import OutboxRepository, PaymentRepository


@dataclass(frozen=True, slots=True)
class CreatePaymentCommand:
    """Команда создания платежа после HTTP-валидации."""

    amount: Decimal
    currency: str
    description: str
    metadata: dict[str, Any]
    webhook_url: str
    idempotency_key: str


@dataclass(frozen=True, slots=True)
class PaymentView:
    """Представление платежа для API-слоя."""

    payment_id: uuid.UUID
    amount: Decimal
    currency: str
    description: str
    metadata: dict[str, Any]
    status: str
    webhook_url: str
    webhook_delivery_status: str
    created_at: datetime
    processed_at: datetime | None


def _to_view(row: PaymentModel) -> PaymentView:
    """Преобразовать ORM-модель в представление.

    Args:
        row: Строка payments.

    Returns:
        `PaymentView` для сериализации ответа.
    """
    return PaymentView(
        payment_id=row.id,
        amount=row.amount,
        currency=row.currency,
        description=row.description,
        metadata=dict(row.metadata_),
        status=row.status,
        webhook_url=row.webhook_url,
        webhook_delivery_status=row.webhook_delivery_status,
        created_at=row.created_at,
        processed_at=row.processed_at,
    )


class PaymentService:
    """Application-сервис платежей (без обращения к брокеру/webhook)."""

    def __init__(self, session: AsyncSession) -> None:
        """Инициализировать сервис.

        Args:
            session: Async-сессия в рамках HTTP-запроса.
        """
        self._session = session
        self._payments = PaymentRepository(session)
        self._outbox = OutboxRepository(session)

    async def create_payment(self, command: CreatePaymentCommand) -> PaymentView:
        """Создать платёж и Outbox атомарно либо вернуть идемпотентный повтор.

        В одной транзакции PostgreSQL:
        1. ``INSERT ... ON CONFLICT DO NOTHING RETURNING``;
        2. при успехе — одна запись Outbox ``payment.created``;
        3. при конфликте — SELECT существующего и сравнение ``request_hash``.

        Args:
            command: Валидированная команда создания.

        Returns:
            Представление платежа (нового или существующего).

        Raises:
            IdempotencyConflictError: тот же ключ при другом теле запроса.
        """
        request_hash = compute_request_hash(
            amount=command.amount,
            currency=command.currency,
            description=command.description,
            metadata=command.metadata,
            webhook_url=command.webhook_url,
        )
        payment_id = uuid.uuid4()

        async with self._session.begin():
            inserted_id = await self._payments.insert_payment_if_absent(
                payment_id=payment_id,
                amount=command.amount,
                currency=command.currency,
                description=command.description,
                metadata=command.metadata,
                idempotency_key=command.idempotency_key,
                request_hash=request_hash,
                webhook_url=command.webhook_url,
            )
            if inserted_id is not None:
                await self._outbox.insert_payment_created(payment_id=inserted_id)
                created = await self._payments.get_by_id(inserted_id)
                if created is None:  # pragma: no cover - защитный инвариант
                    msg = "Payment disappeared after insert"
                    raise RuntimeError(msg)
                return _to_view(created)

            existing = await self._payments.get_by_idempotency_key(
                command.idempotency_key
            )
            if existing is None:  # pragma: no cover - защитный инвариант
                msg = "Idempotency conflict without existing payment"
                raise RuntimeError(msg)
            if existing.request_hash != request_hash:
                raise IdempotencyConflictError()
            return _to_view(existing)

    async def get_payment(self, payment_id: uuid.UUID) -> PaymentView:
        """Получить платёж по идентификатору.

        Args:
            payment_id: UUID платежа.

        Returns:
            Представление платежа.

        Raises:
            PaymentNotFoundError: если запись отсутствует.
        """
        row = await self._payments.get_by_id(payment_id)
        if row is None:
            raise PaymentNotFoundError(str(payment_id))
        return _to_view(row)
