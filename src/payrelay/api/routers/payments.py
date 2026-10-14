from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from payrelay.api.dependencies import (
    get_db_session,
    require_api_key,
    require_idempotency_key,
)
from payrelay.api.openapi.responses import (
    CREATE_PAYMENT_RESPONSES,
    GET_PAYMENT_RESPONSES,
)
from payrelay.api.presenters import to_create_payment_response, to_payment_response
from payrelay.api.schemas import (
    CreatePaymentRequest,
    CreatePaymentResponse,
    PaymentResponse,
)
from payrelay.application.payments import CreatePaymentCommand, PaymentService

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
)


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=CreatePaymentResponse,
    responses=CREATE_PAYMENT_RESPONSES,
    summary="Создать платёж",
    description=(
        "Принимает платёж в обработку: атомарно создаёт запись payments и Outbox. "
        "Повтор с тем же Idempotency-Key и эквивалентным телом возвращает тот же "
        "payment_id (202). Повтор с другим телом — 409."
    ),
)
async def create_payment(
    body: CreatePaymentRequest,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    idempotency_key: Annotated[str, Depends(require_idempotency_key)],
) -> CreatePaymentResponse:
    """Принять платёж в обработку (запись payments + outbox).

    Args:
        body: Валидированное тело запроса.
        session: Async-сессия БД.
        idempotency_key: Валидный Idempotency-Key.

    Returns:
        202 с payment_id, status и created_at.

    Raises:
        IdempotencyConflictError: конфликт идемпотентности (HTTP 409 через handler).
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
    view = await service.create_payment(command)
    return to_create_payment_response(view)


@router.get(
    "/{payment_id}",
    response_model=PaymentResponse,
    responses=GET_PAYMENT_RESPONSES,
    summary="Получить платёж",
    description="Возвращает текущее состояние платежа по UUID.",
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
        PaymentNotFoundError: платёж не найден (HTTP 404 через handler).
    """
    view = await PaymentService(session).get_payment(payment_id)
    return to_payment_response(view)
