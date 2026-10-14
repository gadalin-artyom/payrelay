from __future__ import annotations

from payrelay.api.schemas import CreatePaymentResponse, PaymentResponse
from payrelay.application.payments import PaymentView


def to_create_payment_response(view: PaymentView) -> CreatePaymentResponse:
    """Преобразовать PaymentView в ответ создания платежа.

    Args:
        view: Представление платежа из application-слоя.

    Returns:
        DTO ответа 202.
    """
    return CreatePaymentResponse(
        payment_id=view.payment_id,
        status=view.status,
        created_at=view.created_at,
    )


def to_payment_response(view: PaymentView) -> PaymentResponse:
    """Преобразовать PaymentView в полный ответ GET платежа.

    Args:
        view: Представление платежа из application-слоя.

    Returns:
        DTO ответа 200.
    """
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
