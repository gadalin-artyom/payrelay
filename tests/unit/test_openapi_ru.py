from __future__ import annotations

import pytest

from payrelay.api.app import create_app
from payrelay.core.config import Settings


@pytest.mark.unit
def test_openapi_contains_security_and_russian_errors(
    test_settings: Settings,
) -> None:
    """OpenAPI описывает X-API-Key, Idempotency-Key и русские примеры ошибок."""
    settings = test_settings.model_copy(update={"enable_docs": True})
    app = create_app(settings)
    schema = app.openapi()

    assert "Неверный или отсутствующий API-ключ" in str(schema)
    assert "Платёж не найден" in str(schema)
    assert "Idempotency-Key уже использован" in str(schema)

    security_schemes = schema.get("components", {}).get("securitySchemes", {})
    assert any(
        scheme.get("name") == "X-API-Key"
        or key.lower() in {"api_key", "apikeyheader"}
        or scheme.get("type") == "apiKey"
        for key, scheme in security_schemes.items()
    ), security_schemes

    paths = schema["paths"]
    post_payments = paths["/api/v1/payments"]["post"]
    assert "202" in post_payments["responses"]
    assert "401" in post_payments["responses"]
    assert "409" in post_payments["responses"]
    assert "422" in post_payments["responses"]

    parameters = post_payments.get("parameters", [])
    idem_params = [
        item
        for item in parameters
        if item.get("name") == "Idempotency-Key" or "Idempotency-Key" in str(item)
    ]
    assert idem_params, parameters
    assert "UUID" in str(idem_params) or "идемпотент" in str(idem_params).lower()

    get_payment = paths["/api/v1/payments/{payment_id}"]["get"]
    assert "404" in get_payment["responses"]
    assert "422" in get_payment["responses"]

    create_schema = schema["components"]["schemas"]["CreatePaymentRequest"]
    amount_schema = create_schema["properties"]["amount"]
    assert "description" in amount_schema
    amount_desc = amount_schema["description"].lower()
    assert "сумм" in amount_desc or "Сумма" in str(amount_schema)
