from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import SecretStr

from payrelay.application.outbox_dispatcher import OutboxDispatcher
from payrelay.core.config import Settings
from payrelay.infrastructure.db.repositories import ClaimedOutboxRow
from payrelay.infrastructure.messaging.publisher import OutboxPublishError


def _settings() -> Settings:
    return Settings(
        database_url=SecretStr(
            "postgresql+asyncpg://payrelay:payrelay@localhost:5432/payrelay_test"
        ),
        rabbitmq_url=SecretStr("amqp://payrelay:payrelay@localhost:5672/"),
        api_key=SecretStr("test-api-key-not-for-production"),
        app_env="test",
        outbox_batch_size=3,
        outbox_lease_seconds=30,
        outbox_publish_timeout_seconds=5,
        outbox_retry_max_delay_seconds=30,
        outbox_poll_interval_seconds=0.01,
    )


def _claimed(*, attempts: int = 1) -> ClaimedOutboxRow:
    event_id = uuid.uuid4()
    payment_id = uuid.uuid4()
    now = datetime.now(UTC)
    return ClaimedOutboxRow(
        id=event_id,
        aggregate_id=payment_id,
        event_type="payment.created",
        payload={
            "schema_version": 1,
            "event_id": str(event_id),
            "payment_id": str(payment_id),
            "event_type": "payment.created",
            "attempt": 1,
        },
        delivery_attempt=1,
        available_at=now - timedelta(seconds=1),
        published_at=None,
        claimed_until=now + timedelta(seconds=30),
        attempts=attempts,
        created_at=now,
    )


class _Harness(OutboxDispatcher):
    """Диспетчер с подменяемыми TX-шагами для детерминированных unit-тестов."""

    def __init__(
        self,
        *,
        session_factory: MagicMock,
        publisher: AsyncMock,
        settings: Settings,
    ) -> None:
        super().__init__(
            session_factory=session_factory,
            publisher=publisher,
            settings=settings,
        )
        self.claim_mock = AsyncMock(return_value=None)
        self.mark_mock = AsyncMock(return_value=True)
        self.release_mock = AsyncMock()

    async def _claim_one(self) -> ClaimedOutboxRow | None:
        return await self.claim_mock()

    async def _mark_published(self, claimed: ClaimedOutboxRow) -> bool:
        return await self.mark_mock(claimed)

    async def _release_after_failure(self, claimed: ClaimedOutboxRow) -> None:
        await self.release_mock(claimed)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_confirm_success_marks_published() -> None:
    """Успешный publish вызывает mark-published."""
    claimed = _claimed()
    dispatcher = _Harness(
        session_factory=MagicMock(),
        publisher=AsyncMock(),
        settings=_settings(),
    )
    dispatcher.publisher.publish_outbox_payload = AsyncMock(return_value=MagicMock())
    dispatcher.claim_mock = AsyncMock(return_value=claimed)

    assert await dispatcher.process_one() is True
    dispatcher.publisher.publish_outbox_payload.assert_awaited_once()
    dispatcher.mark_mock.assert_awaited_once_with(claimed)
    dispatcher.release_mock.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_publish_error_forbids_mark_published() -> None:
    """Ошибка/timeout publish не вызывает mark-published, а освобождает claim."""
    claimed = _claimed(attempts=2)
    dispatcher = _Harness(
        session_factory=MagicMock(),
        publisher=AsyncMock(),
        settings=_settings(),
    )
    dispatcher.publisher.publish_outbox_payload = AsyncMock(
        side_effect=OutboxPublishError("timeout")
    )
    dispatcher.claim_mock = AsyncMock(return_value=claimed)

    assert await dispatcher.process_one() is True
    dispatcher.mark_mock.assert_not_awaited()
    dispatcher.release_mock.assert_awaited_once_with(claimed)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_request_stop_refuses_new_claims() -> None:
    """После request_stop новые claims не выполняются."""
    dispatcher = _Harness(
        session_factory=MagicMock(),
        publisher=AsyncMock(),
        settings=_settings(),
    )
    dispatcher.claim_mock = AsyncMock(return_value=_claimed())
    dispatcher.request_stop()

    assert await dispatcher.process_one() is False
    dispatcher.claim_mock.assert_not_awaited()
    assert await dispatcher.run_tick() == 0
