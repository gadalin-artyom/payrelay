"""Маршруты POST/GET платежей."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from payrelay.api.dependencies import get_db_session, require_api_key
from payrelay.api.schemas import (
    CreatePaymentRequest,
    CreatePaymentResponse,
    PaymentResponse,
    validate_idempotency_key_value,
)
from payrelay.application.payments import CreatePaymentCommand, PaymentService
from payrelay.domain.errors import IdempotencyConflictError, PaymentNotFoundError

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
)


def _parse_idempotency_key(
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> str:
    """Извлечь и провалидировать заголовок Idempotency-Key.

    Args:
        idempotency_key: Сырое значение заголовка.

    Returns:
        Валидный ключ.

    Raises:
        HTTPException: 422 при отсутствии или невалидном ключе.
    """
    if idempotency_key is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=[
                {
                    "type": "missing",
                    "loc": ["header", "Idempotency-Key"],
                    "msg": "Field required",
                    "input": None,
                }
            ],
        )
    try:
        return validate_idempotency_key_value(idempotency_key)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=[
                {
                    "type": "value_error",
                    "loc": ["header", "Idempotency-Key"],
                    "msg": str(exc),
                    "input": idempotency_key,
                }
            ],
        ) from exc


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=CreatePaymentResponse,
    summary="Создать платёж",
)
async def create_payment(
    body: CreatePaymentRequest,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    idempotency_key: Annotated[str, Depends(_parse_idempotency_key)],
) -> CreatePaymentResponse:
    """Принять платёж в обработку (запись payments + outbox).

    Args:
        body: Валидированное тело запроса.
        session: Async-сессия БД.
        idempotency_key: Валидный Idempotency-Key.

    Returns:
        202 с payment_id, status и created_at.

    Raises:
        HTTPException: 409 при конфликте идемпотентности.
    """
    service = PaymentService(session)
    command = CreatePaymentCommand(
        amount=body.amount,
        currency=body.currency,
        description=body.description,
        metadata=body.metadata,
        webhook_url=body.webhook_url,
        idempotency_key=idempotency_key,
    )
    try:
        view = await service.create_payment(command)
    except IdempotencyConflictError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency-Key already used with a different request body",
        ) from exc
    return CreatePaymentResponse(
        payment_id=view.payment_id,
        status=view.status,
        created_at=view.created_at,
    )


@router.get(
    "/{payment_id}",
    response_model=PaymentResponse,
    summary="Получить платёж",
)
async def get_payment(
    payment_id: UUID,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> PaymentResponse:
    """Вернуть текущее состояние платежа.

    Args:
        payment_id: UUID платежа (некорректный формат → 422 на уровне FastAPI).
        session: Async-сессия БД.

    Returns:
        Полное представление платежа.

    Raises:
        HTTPException: 404 если платёж не найден.
    """
    service = PaymentService(session)
    try:
        view = await service.get_payment(payment_id)
    except PaymentNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Payment not found",
        ) from exc
    return PaymentResponse(
        payment_id=view.payment_id,
        amount=view.amount,
        currency=view.currency,
        description=view.description,
        metadata=view.metadata,
        status=view.status,
        webhook_url=view.webhook_url,
        webhook_delivery_status=view.webhook_delivery_status,
        created_at=view.created_at,
        processed_at=view.processed_at,
    )
