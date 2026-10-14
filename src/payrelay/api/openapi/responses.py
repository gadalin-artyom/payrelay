from __future__ import annotations

from typing import Any

from fastapi import status

from payrelay.api.constants import IDEMPOTENCY_KEY_HEADER_NAME
from payrelay.api.messages import (
    AMOUNT_INVALID,
    API_KEY_INVALID,
    IDEMPOTENCY_CONFLICT,
    IDEMPOTENCY_KEY_MISSING,
    PAYMENT_NOT_FOUND,
    UUID_INVALID,
)
from payrelay.api.schemas import (
    ApiErrorResponse,
    CreatePaymentResponse,
    PaymentResponse,
    ValidationErrorResponse,
)

OpenAPIResponses = dict[int | str, dict[str, Any]]

UNAUTHORIZED_RESPONSE: dict[str, Any] = {
    "description": "Нет или неверен X-API-Key.",
    "model": ApiErrorResponse,
    "content": {"application/json": {"example": {"detail": API_KEY_INVALID}}},
}

CREATE_PAYMENT_RESPONSES: OpenAPIResponses = {
    status.HTTP_202_ACCEPTED: {
        "description": "Платёж принят в обработку.",
        "model": CreatePaymentResponse,
    },
    status.HTTP_401_UNAUTHORIZED: UNAUTHORIZED_RESPONSE,
    status.HTTP_409_CONFLICT: {
        "description": "Idempotency-Key уже использован с другим телом.",
        "model": ApiErrorResponse,
        "content": {"application/json": {"example": {"detail": IDEMPOTENCY_CONFLICT}}},
    },
    status.HTTP_422_UNPROCESSABLE_CONTENT: {
        "description": "Ошибка валидации тела или заголовка Idempotency-Key.",
        "model": ValidationErrorResponse,
        "content": {
            "application/json": {
                "examples": {
                    "missing_idempotency_key": {
                        "summary": "Нет Idempotency-Key",
                        "value": {
                            "detail": [
                                {
                                    "loc": [
                                        "header",
                                        IDEMPOTENCY_KEY_HEADER_NAME,
                                    ],
                                    "msg": IDEMPOTENCY_KEY_MISSING,
                                    "type": "missing",
                                }
                            ]
                        },
                    },
                    "invalid_amount": {
                        "summary": "Некорректная сумма",
                        "value": {
                            "detail": [
                                {
                                    "loc": ["body", "amount"],
                                    "msg": AMOUNT_INVALID,
                                    "type": "value_error",
                                }
                            ]
                        },
                    },
                }
            }
        },
    },
}

GET_PAYMENT_RESPONSES: OpenAPIResponses = {
    status.HTTP_200_OK: {
        "description": "Платёж найден.",
        "model": PaymentResponse,
    },
    status.HTTP_401_UNAUTHORIZED: UNAUTHORIZED_RESPONSE,
    status.HTTP_404_NOT_FOUND: {
        "description": "Платёж с указанным UUID не найден.",
        "model": ApiErrorResponse,
        "content": {"application/json": {"example": {"detail": PAYMENT_NOT_FOUND}}},
    },
    status.HTTP_422_UNPROCESSABLE_CONTENT: {
        "description": "Некорректный формат payment_id (не UUID).",
        "model": ValidationErrorResponse,
        "content": {
            "application/json": {
                "example": {
                    "detail": [
                        {
                            "loc": ["path", "payment_id"],
                            "msg": UUID_INVALID,
                            "type": "uuid_parsing",
                        }
                    ]
                }
            }
        },
    },
}
