"""I01: проверка Alembic-миграций на чистой test DB."""

from __future__ import annotations

import os

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests.integration.conftest import _run_alembic, assert_safe_test_database_url


@pytest.mark.integration
@pytest.mark.asyncio
async def test_i01_upgrade_creates_expected_schema(
    session_factory: async_sessionmaker[AsyncSession],
    integration_database_url: str,
) -> None:
    """I01: после upgrade head есть payments/outbox (+ alembic_version)."""
    assert_safe_test_database_url(integration_database_url)

    async with session_factory() as session:
        result = await session.execute(
            text(
                """
                SELECT tablename
                FROM pg_tables
                WHERE schemaname = 'public'
                ORDER BY tablename
                """
            )
        )
        tables = {row[0] for row in result.fetchall()}

    assert "payments" in tables
    assert "outbox" in tables
    assert "alembic_version" in tables
    # Ровно две предметные таблицы (+ служебная alembic_version).
    business_tables = tables - {"alembic_version"}
    assert business_tables == {"payments", "outbox"}

    async with session_factory() as session:
        uniq = await session.execute(
            text(
                """
                SELECT 1
                FROM pg_constraint
                WHERE conname = 'payments_idempotency_key_key'
                   OR conname LIKE '%idempotency_key%'
                LIMIT 1
                """
            )
        )
        assert uniq.scalar_one_or_none() is not None

        outbox_uniq = await session.execute(
            text(
                """
                SELECT 1
                FROM pg_constraint
                WHERE conname = 'uq_outbox_aggregate_event_attempt'
                """
            )
        )
        assert outbox_uniq.scalar_one_or_none() is not None

        partial_idx = await session.execute(
            text(
                """
                SELECT 1
                FROM pg_indexes
                WHERE indexname = 'ix_outbox_unpublished_available'
                """
            )
        )
        assert partial_idx.scalar_one_or_none() is not None


@pytest.mark.integration
def test_i01_downgrade_and_upgrade_on_test_db_only(
    integration_database_url: str,
) -> None:
    """I01: downgrade → upgrade на изолированной test DB восстанавливает схему."""
    assert_safe_test_database_url(integration_database_url)
    os.environ["DATABASE_URL"] = integration_database_url
    _run_alembic("downgrade", "base")
    _run_alembic("upgrade", "head")
