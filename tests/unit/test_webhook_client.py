"""U12/U16: webhook payload, стабильный event id, SSRF policy."""

from __future__ import annotations

import ipaddress
import socket
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import patch

import httpx
import pytest

from payrelay.infrastructure.webhooks.http_client import (
    WebhookHttpClient,
    WebhookUrlBlockedError,
    build_webhook_event_id,
    build_webhook_payload,
    resolve_and_validate_host,
    validate_webhook_url_for_delivery,
)

pytestmark = pytest.mark.unit


def test_u12_webhook_event_id_stable_uuid5() -> None:
    """U12: webhook_event_id стабилен и совпадает в header/body."""
    payment_id = uuid.UUID("11111111-1111-4111-8111-111111111111")
    first = build_webhook_event_id(payment_id)
    second = build_webhook_event_id(payment_id)
    assert first == second
    assert first.version == 5
    other = build_webhook_event_id(uuid.uuid4())
    assert other != first


def test_u12_payload_amount_two_decimals_and_header_match() -> None:
    """U12: amount с двумя знаками; header = webhook_event_id."""
    payment_id = uuid.uuid4()
    payload = build_webhook_payload(
        payment_id=payment_id,
        status="succeeded",
        amount=Decimal("1500.5"),
        currency="RUB",
        processed_at=datetime(2026, 10, 14, 10, 0, 3, tzinfo=UTC),
    )
    body = payload.to_json_dict()
    assert body["amount"] == "1500.50"
    assert body["status"] == "succeeded"
    assert body["webhook_event_id"] == str(payload.webhook_event_id)
    assert body["processed_at"] == "2026-10-14T10:00:03Z"


@pytest.mark.asyncio
async def test_u12_http_2xx_marks_ok() -> None:
    """U12: любой 2xx — успех доставки."""
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(204)

    transport = httpx.MockTransport(handler)
    client = WebhookHttpClient(
        timeout_seconds=2.0,
        allow_private_urls=True,
        transport=transport,
    )
    payload = build_webhook_payload(
        payment_id=uuid.uuid4(),
        status="succeeded",
        amount=Decimal("10.00"),
        currency="USD",
        processed_at=datetime.now(UTC),
    )
    # allow_private + literal IP избегает внешней DNS в unit.
    result = await client.deliver(url="http://127.0.0.1/hook", payload=payload)
    await client.aclose()
    assert result.ok is True
    assert result.status_code == 204
    assert len(calls) == 1
    assert calls[0].headers["X-Webhook-Event-ID"] == str(payload.webhook_event_id)


@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.com/x",
        "https://user:pass@example.com/x",
        "http://localhost/hook",
        "http://127.0.0.1/hook",
        "http://10.0.0.5/hook",
        "http://192.168.1.1/hook",
        "http://169.254.169.254/latest",
    ],
)
def test_u16_static_url_rejected_without_private_flag(url: str) -> None:
    """U16: scheme/credentials/loopback/private отклоняются в production policy."""
    with pytest.raises(WebhookUrlBlockedError):
        validate_webhook_url_for_delivery(url, allow_private=False)


def test_u16_private_allowed_only_with_flag() -> None:
    """U16: private URL допустим только при allow_private=True."""
    validate_webhook_url_for_delivery("http://127.0.0.1/hook", allow_private=True)
    with pytest.raises(WebhookUrlBlockedError):
        validate_webhook_url_for_delivery("http://127.0.0.1/hook", allow_private=False)


def test_u16_dns_private_resolution_blocked() -> None:
    """U16: DNS A/AAAA в private сеть → fail-closed."""

    def fake_getaddrinfo(*_args: object, **_kwargs: object) -> list[tuple]:
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.1.2.3", 0))]

    target = "payrelay.infrastructure.webhooks.http_client.socket.getaddrinfo"
    with patch(target, fake_getaddrinfo):
        with pytest.raises(WebhookUrlBlockedError):
            resolve_and_validate_host("internal.example", allow_private=False)


def test_u16_dns_public_resolution_ok() -> None:
    """U16: публичный DNS проходит проверку."""

    def fake_getaddrinfo(*_args: object, **_kwargs: object) -> list[tuple]:
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0))]

    target = "payrelay.infrastructure.webhooks.http_client.socket.getaddrinfo"
    with patch(target, fake_getaddrinfo):
        addresses = resolve_and_validate_host("example.com", allow_private=False)
    assert addresses == ["93.184.216.34"]
    assert not ipaddress.ip_address(addresses[0]).is_private


@pytest.mark.asyncio
async def test_u16_redirect_not_followed() -> None:
    """U16: клиент не следует redirect (follow_redirects=False)."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"Location": "http://127.0.0.1/evil"})

    transport = httpx.MockTransport(handler)
    client = WebhookHttpClient(
        timeout_seconds=2.0,
        allow_private_urls=True,
        transport=transport,
    )
    payload = build_webhook_payload(
        payment_id=uuid.uuid4(),
        status="failed",
        amount=Decimal("1.00"),
        currency="EUR",
        processed_at=datetime.now(UTC),
    )
    result = await client.deliver(url="http://127.0.0.1/hook", payload=payload)
    await client.aclose()
    assert result.ok is False
    assert result.status_code == 302
