from __future__ import annotations

import pytest

from payrelay.domain.enums import OutboxEventType
from payrelay.domain.webhook_retry_rules import (
    is_valid_event_attempt,
    next_system_attempt,
    next_webhook_attempt,
    parse_system_attempt,
    system_retry_routing_key,
    webhook_retry_delay_seconds,
)

pytestmark = pytest.mark.unit


def test_webhook_retry_delays_2_and_4() -> None:
    """После #1 → 2s, после #2 → 4s, после #3 → None."""
    assert webhook_retry_delay_seconds(failed_attempt=1) == 2
    assert webhook_retry_delay_seconds(failed_attempt=2) == 4
    assert webhook_retry_delay_seconds(failed_attempt=3) is None
    assert next_webhook_attempt(failed_attempt=1) == 2
    assert next_webhook_attempt(failed_attempt=2) == 3
    assert next_webhook_attempt(failed_attempt=3) is None


def test_event_attempt_validation() -> None:
    """payment.created только attempt=1; webhook_retry — 2/3."""
    assert is_valid_event_attempt(
        event_type=OutboxEventType.PAYMENT_CREATED.value,
        attempt=1,
    )
    assert not is_valid_event_attempt(
        event_type=OutboxEventType.PAYMENT_CREATED.value,
        attempt=2,
    )
    assert is_valid_event_attempt(
        event_type=OutboxEventType.PAYMENT_WEBHOOK_RETRY.value,
        attempt=2,
    )
    assert is_valid_event_attempt(
        event_type=OutboxEventType.PAYMENT_WEBHOOK_RETRY.value,
        attempt=3,
    )
    assert not is_valid_event_attempt(
        event_type=OutboxEventType.PAYMENT_WEBHOOK_RETRY.value,
        attempt=1,
    )


def test_u15_system_attempt_parsing_and_routing() -> None:
    """U15: system_attempt 1→2→3→DLQ; отсутствие заголовка = 1."""
    assert parse_system_attempt(None) == 1
    assert parse_system_attempt("2") == 2
    assert parse_system_attempt(3) == 3
    assert parse_system_attempt(0) is None
    assert parse_system_attempt("x") is None
    assert parse_system_attempt(True) is None

    assert system_retry_routing_key(current_system_attempt=1) == "payments.retry.2"
    assert system_retry_routing_key(current_system_attempt=2) == "payments.retry.3"
    assert system_retry_routing_key(current_system_attempt=3) is None
    assert next_system_attempt(current_system_attempt=1) == 2
    assert next_system_attempt(current_system_attempt=2) == 3
    assert next_system_attempt(current_system_attempt=3) is None
