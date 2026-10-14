from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest

from payrelay.domain.enums import PaymentStatus
from payrelay.infrastructure.gateway.simulator import (
    GatewaySimulator,
    deterministic_delay_seconds,
    deterministic_outcome,
)

pytestmark = pytest.mark.unit


def test_u09_outcome_deterministic_for_same_uuid() -> None:
    """U09: повторный расчёт outcome стабилен для одного payment_id."""
    payment_id = uuid.UUID("11111111-1111-4111-8111-111111111111")
    first = deterministic_outcome(payment_id)
    second = deterministic_outcome(payment_id)
    assert first is second
    assert first in {PaymentStatus.SUCCEEDED, PaymentStatus.FAILED}


def test_u09_succeeded_and_failed_branches_exist() -> None:
    """U09: встречаются обе ветки succeeded и failed."""
    seen: set[PaymentStatus] = set()
    for _ in range(5000):
        seen.add(deterministic_outcome(uuid.uuid4()))
        if seen == {PaymentStatus.SUCCEEDED, PaymentStatus.FAILED}:
            break
    assert seen == {PaymentStatus.SUCCEEDED, PaymentStatus.FAILED}


def test_u09_delay_in_range_and_injectable_sleep() -> None:
    """U09: delay в [2, 5], sleep инъецируется (без реального ожидания)."""
    payment_id = uuid.uuid4()
    delay = deterministic_delay_seconds(payment_id, min_seconds=2.0, max_seconds=5.0)
    assert 2.0 <= delay <= 5.0

    sleep = AsyncMock()
    simulator = GatewaySimulator(
        sleep=sleep,
        min_delay_seconds=2.0,
        max_delay_seconds=5.0,
    )
    preview = simulator.preview(payment_id)
    assert preview.delay_seconds == delay


@pytest.mark.asyncio
async def test_u09_charge_uses_injected_sleep_and_forced_outcome() -> None:
    """U09: charge вызывает sleep и уважает forced_outcome."""
    sleep = AsyncMock()
    simulator = GatewaySimulator(
        sleep=sleep,
        min_delay_seconds=2.0,
        max_delay_seconds=5.0,
        forced_outcome=PaymentStatus.FAILED,
    )
    payment_id = uuid.uuid4()
    status = await simulator.charge(payment_id)
    assert status is PaymentStatus.FAILED
    sleep.assert_awaited_once()
    awaited_delay = sleep.await_args.args[0]
    assert 2.0 <= awaited_delay <= 5.0


def test_u09_distribution_roughly_nine_to_one() -> None:
    """U09: доля failed около 10% на большой выборке UUID."""
    failed = 0
    total = 10_000
    for _ in range(total):
        if deterministic_outcome(uuid.uuid4()) is PaymentStatus.FAILED:
            failed += 1
    ratio = failed / total
    assert 0.05 <= ratio <= 0.15
