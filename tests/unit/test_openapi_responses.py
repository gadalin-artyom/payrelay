from __future__ import annotations

import pytest
from fastapi import status

from payrelay.api.app import create_app
from payrelay.api.constants import IDEMPOTENCY_KEY_HEADER_NAME
from payrelay.api.messages import (
    AMOUNT_INVALID,
    API_KEY_INVALID,
    IDEMPOTENCY_CONFLICT,
    IDEMPOTENCY_KEY_MISSING,
    PAYMENT_NOT_FOUND,
    UUID_INVALID,
)
from payrelay.api.openapi.responses import (
    CREATE_PAYMENT_RESPONSES,
    GET_PAYMENT_RESPONSES,
)
from payrelay.api.schemas import (
    ApiErrorResponse,
    CreatePaymentResponse,
    PaymentResponse,
    ValidationErrorResponse,
)
from payrelay.core.config import Settings


@pytest.mark.unit
def test_create_payment_responses_declare_expected_statuses() -> None:
    """CREATE_PAYMENT_RESPONSES содержит 202/401/409/422 и русские примеры."""
    assert status.HTTP_202_ACCEPTED in CREATE_PAYMENT_RESPONSES
    assert status.HTTP_401_UNAUTHORIZED in CREATE_PAYMENT_RESPONSES
    assert status.HTTP_409_CONFLICT in CREATE_PAYMENT_RESPONSES
    assert status.HTTP_422_UNPROCESSABLE_CONTENT in CREATE_PAYMENT_RESPONSES

    assert (
        CREATE_PAYMENT_RESPONSES[status.HTTP_202_ACCEPTED]["model"]
        is CreatePaymentResponse
    )
    assert (
        CREATE_PAYMENT_RESPONSES[status.HTTP_401_UNAUTHORIZED]["model"]
        is ApiErrorResponse
    )
    assert (
        CREATE_PAYMENT_RESPONSES[status.HTTP_409_CONFLICT]["model"] is ApiErrorResponse
    )
    assert (
        CREATE_PAYMENT_RESPONSES[status.HTTP_422_UNPROCESSABLE_CONTENT]["model"]
        is ValidationErrorResponse
    )

    unauthorized = CREATE_PAYMENT_RESPONSES[status.HTTP_401_UNAUTHORIZED]
    assert unauthorized["content"]["application/json"]["example"] == {
        "detail": API_KEY_INVALID
    }

    conflict = CREATE_PAYMENT_RESPONSES[status.HTTP_409_CONFLICT]
    assert conflict["content"]["application/json"]["example"] == {
        "detail": IDEMPOTENCY_CONFLICT
    }

    examples = CREATE_PAYMENT_RESPONSES[status.HTTP_422_UNPROCESSABLE_CONTENT][
        "content"
    ]["application/json"]["examples"]
    missing = examples["missing_idempotency_key"]["value"]["detail"][0]
    assert missing["loc"] == ["header", IDEMPOTENCY_KEY_HEADER_NAME]
    assert missing["msg"] == IDEMPOTENCY_KEY_MISSING
    assert "api_key" not in str(missing).lower()
    assert "secret" not in str(missing).lower()

    invalid_amount = examples["invalid_amount"]["value"]["detail"][0]
    assert invalid_amount["loc"] == ["body", "amount"]
    assert invalid_amount["msg"] == AMOUNT_INVALID


@pytest.mark.unit
def test_get_payment_responses_declare_expected_statuses() -> None:
    """GET_PAYMENT_RESPONSES содержит 200/401/404/422 и русский пример UUID."""
    assert status.HTTP_200_OK in GET_PAYMENT_RESPONSES
    assert status.HTTP_401_UNAUTHORIZED in GET_PAYMENT_RESPONSES
    assert status.HTTP_404_NOT_FOUND in GET_PAYMENT_RESPONSES
    assert status.HTTP_422_UNPROCESSABLE_CONTENT in GET_PAYMENT_RESPONSES

    assert GET_PAYMENT_RESPONSES[status.HTTP_200_OK]["model"] is PaymentResponse
    assert GET_PAYMENT_RESPONSES[status.HTTP_404_NOT_FOUND]["model"] is ApiErrorResponse
    assert (
        GET_PAYMENT_RESPONSES[status.HTTP_422_UNPROCESSABLE_CONTENT]["model"]
        is ValidationErrorResponse
    )

    not_found = GET_PAYMENT_RESPONSES[status.HTTP_404_NOT_FOUND]
    assert not_found["content"]["application/json"]["example"] == {
        "detail": PAYMENT_NOT_FOUND
    }

    uuid_example = GET_PAYMENT_RESPONSES[status.HTTP_422_UNPROCESSABLE_CONTENT][
        "content"
    ]["application/json"]["example"]["detail"][0]
    assert uuid_example["loc"] == ["path", "payment_id"]
    assert uuid_example["msg"] == UUID_INVALID
    assert uuid_example["type"] == "uuid_parsing"


@pytest.mark.unit
def test_openapi_routes_link_response_declarations(
    test_settings: Settings,
) -> None:
    """Роуты POST/GET ссылаются на вынесенные responses и сохраняют контракт."""
    settings = test_settings.model_copy(update={"enable_docs": True})
    app = create_app(settings)
    schema = app.openapi()

    post_payments = schema["paths"]["/api/v1/payments"]["post"]
    get_payment = schema["paths"]["/api/v1/payments/{payment_id}"]["get"]

    assert "202" in post_payments["responses"]
    assert "401" in post_payments["responses"]
    assert "409" in post_payments["responses"]
    assert "422" in post_payments["responses"]
    assert post_payments["operationId"] == "create_payment_api_v1_payments_post"

    assert "200" in get_payment["responses"]
    assert "401" in get_payment["responses"]
    assert "404" in get_payment["responses"]
    assert "422" in get_payment["responses"]
    assert get_payment["operationId"] == "get_payment_api_v1_payments__payment_id__get"

    security_schemes = schema["components"]["securitySchemes"]
    assert any(
        scheme.get("name") == "X-API-Key" or scheme.get("type") == "apiKey"
        for scheme in security_schemes.values()
    )

    parameters = post_payments.get("parameters", [])
    assert any(
        item.get("name") == "Idempotency-Key" or "Idempotency-Key" in str(item)
        for item in parameters
    )

    post_422 = str(post_payments["responses"]["422"])
    get_422 = str(get_payment["responses"]["422"])
    assert IDEMPOTENCY_KEY_MISSING in post_422 or "Idempotency-Key" in post_422
    assert UUID_INVALID in get_422 or "payment_id" in get_422
    assert "input" not in post_422.lower() or "api_key" not in post_422.lower()
