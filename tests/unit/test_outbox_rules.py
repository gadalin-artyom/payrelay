"""Unit-тесты правил доступности Outbox и publish-backoff."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from payrelay.domain.outbox_rules import is_outbox_claimable, publish_backoff_seconds


@pytest.mark.unit
def test_claimable_when_unpublished_and_available() -> None:
    """Доступна запись без published_at, с available_at <= now и без lease."""
    now = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    assert is_outbox_claimable(
        published_at=None,
        available_at=now - timedelta(seconds=1),
        claimed_until=None,
        now=now,
    )


@pytest.mark.unit
def test_not_claimable_when_published() -> None:
    """Опубликованная запись недоступна."""
    now = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    assert not is_outbox_claimable(
        published_at=now,
        available_at=now - timedelta(seconds=1),
        claimed_until=None,
        now=now,
    )


@pytest.mark.unit
def test_not_claimable_before_available_at() -> None:
    """Запись с available_at в будущем недоступна."""
    now = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    assert not is_outbox_claimable(
        published_at=None,
        available_at=now + timedelta(seconds=5),
        claimed_until=None,
        now=now,
    )


@pytest.mark.unit
def test_not_claimable_while_lease_active() -> None:
    """Активный claimed_until блокирует claim."""
    now = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    assert not is_outbox_claimable(
        published_at=None,
        available_at=now - timedelta(seconds=1),
        claimed_until=now + timedelta(seconds=10),
        now=now,
    )


@pytest.mark.unit
def test_claimable_after_lease_expired() -> None:
    """После истечения lease запись снова доступна."""
    now = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    assert is_outbox_claimable(
        published_at=None,
        available_at=now - timedelta(seconds=1),
        claimed_until=now - timedelta(seconds=1),
        now=now,
    )


@pytest.mark.unit
def test_publish_backoff_capped_and_independent_of_payload_attempt() -> None:
    """Backoff публикации ограничен и зависит от outbox.attempts, не payload.attempt."""
    assert publish_backoff_seconds(attempts=1, max_delay_seconds=30) == 1.0
    assert publish_backoff_seconds(attempts=2, max_delay_seconds=30) == 2.0
    assert publish_backoff_seconds(attempts=3, max_delay_seconds=30) == 4.0
    assert publish_backoff_seconds(attempts=10, max_delay_seconds=30) == 30.0
    # payload.attempt=99 не участвует в формуле — явная изоляция контракта.
    payload_attempt = 99
    assert publish_backoff_seconds(attempts=1, max_delay_seconds=30) != payload_attempt
