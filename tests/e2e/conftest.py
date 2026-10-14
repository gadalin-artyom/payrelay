from __future__ import annotations

import os
import time
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from payrelay.core.config import Settings, get_settings
from payrelay.infrastructure.db.session import create_engine, create_session_factory
from payrelay.infrastructure.messaging.broker import create_rabbit_broker
from payrelay.infrastructure.messaging.topology import (
    QUEUE_DLQ,
    QUEUE_PAYMENTS_NEW,
    declare_payments_topology,
)
from tests.e2e.process_harness import ProcessHarness, build_e2e_env
from tests.e2e.webhook_receiver import WebhookReceiver
from tests.integration.conftest import (
    assert_safe_test_database_url,
    assert_safe_test_rabbitmq_url,
)
from tests.integration.test_outbox_publisher import _drain_queue

_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _run_alembic_upgrade() -> None:
    """Применить Alembic upgrade head к текущему DATABASE_URL."""
    from alembic import command
    from alembic.config import Config

    cfg = Config(str(_PROJECT_ROOT / "alembic.ini"))
    command.upgrade(cfg, "head")


@pytest.fixture(scope="session")
def e2e_database_url() -> str:
    """Безопасный DSN для E2E.

    Returns:
        DATABASE_URL тестовой БД.

    Raises:
        RuntimeError: если DSN отсутствует или небезопасен.
    """
    url = os.environ.get("DATABASE_URL")
    if not url:
        msg = "DATABASE_URL is required for e2e tests"
        raise RuntimeError(msg)
    assert_safe_test_database_url(url)
    return url


@pytest.fixture(scope="session")
def e2e_rabbitmq_url() -> str:
    """Безопасный AMQP URL для E2E.

    Returns:
        RABBITMQ_URL тестового брокера.

    Raises:
        RuntimeError: если URL отсутствует или небезопасен.
    """
    url = os.environ.get("RABBITMQ_URL")
    if not url:
        msg = "RABBITMQ_URL is required for e2e tests"
        raise RuntimeError(msg)
    assert_safe_test_rabbitmq_url(url)
    return url


@pytest.fixture(scope="session")
def e2e_api_key() -> str:
    """Тестовый API-ключ.

    Returns:
        Значение ключа из окружения или безопасный placeholder.
    """
    return os.environ.get("API_KEY", "test-api-key-not-for-production")


@pytest.fixture(scope="session")
def e2e_settings(
    e2e_database_url: str,
    e2e_rabbitmq_url: str,
    e2e_api_key: str,
) -> Settings:
    """Settings для прямых проверок БД/брокера из E2E.

    Args:
        e2e_database_url: DSN.
        e2e_rabbitmq_url: AMQP URL.
        e2e_api_key: API-ключ.

    Returns:
        Экземпляр Settings.
    """
    get_settings.cache_clear()
    return Settings(
        database_url=SecretStr(e2e_database_url),
        rabbitmq_url=SecretStr(e2e_rabbitmq_url),
        api_key=SecretStr(e2e_api_key),
        app_env="test",
        enable_docs=False,
        webhook_allow_private_urls=True,
        outbox_poll_interval_seconds=0.1,
        outbox_batch_size=50,
        outbox_lease_seconds=30,
        outbox_publish_timeout_seconds=5,
        gateway_min_delay_seconds=0.05,
        gateway_max_delay_seconds=0.05,
        processing_lease_seconds=20.0,
    )


@pytest.fixture(scope="session", autouse=True)
def e2e_migrated_schema(e2e_database_url: str) -> Iterator[None]:
    """Один раз применить миграции к test DB для E2E.

    Args:
        e2e_database_url: Проверенный DSN.

    Yields:
        Управление сюитой.
    """
    assert_safe_test_database_url(e2e_database_url)
    os.environ["DATABASE_URL"] = e2e_database_url
    get_settings.cache_clear()
    _run_alembic_upgrade()
    yield


@pytest.fixture
async def e2e_engine(e2e_settings: Settings) -> AsyncIterator[AsyncEngine]:
    """AsyncEngine на test DB.

    Args:
        e2e_settings: Настройки.

    Yields:
        Движок SQLAlchemy.
    """
    eng = create_engine(e2e_settings, pool_size=5, max_overflow=5)
    try:
        yield eng
    finally:
        await eng.dispose()


@pytest.fixture
async def e2e_session_factory(
    e2e_engine: AsyncEngine,
) -> async_sessionmaker[AsyncSession]:
    """Фабрика сессий для E2E проверок БД.

    Args:
        e2e_engine: Движок.

    Returns:
        async_sessionmaker.
    """
    return create_session_factory(e2e_engine)


@pytest.fixture
async def e2e_clean_tables(
    e2e_session_factory: async_sessionmaker[AsyncSession],
    e2e_database_url: str,
    e2e_settings: Settings,
) -> AsyncIterator[None]:
    """Очистить таблицы и тестовые очереди перед сценарием.

    Args:
        e2e_session_factory: Фабрика сессий.
        e2e_database_url: DSN для safety-check.
        e2e_settings: Настройки брокера.

    Yields:
        Управление тесту.
    """
    assert_safe_test_database_url(e2e_database_url)
    async with e2e_session_factory() as session:
        async with session.begin():
            await session.execute(text("TRUNCATE outbox, payments CASCADE"))
    broker = create_rabbit_broker(e2e_settings, fail_fast=True)
    await broker.connect()
    await declare_payments_topology(broker)
    try:
        await _drain_queue(broker, QUEUE_PAYMENTS_NEW, limit=200)
        await _drain_queue(broker, QUEUE_DLQ, limit=200)
    finally:
        await broker.stop()
    yield


@pytest.fixture
def webhook_receiver() -> Iterator[WebhookReceiver]:
    """Запустить изолированный webhook receiver на localhost.

    Yields:
        Готовый WebhookReceiver.
    """
    receiver = WebhookReceiver(host="127.0.0.1", port=8199)
    receiver.start()
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        try:
            response = httpx.get(f"{receiver.base_url}/health", timeout=0.5)
            if response.status_code == 200:
                break
        except (httpx.HTTPError, OSError):
            time.sleep(0.1)
    else:
        receiver.stop()
        msg = "webhook receiver failed to become ready"
        raise RuntimeError(msg)
    try:
        yield receiver
    finally:
        receiver.stop()


@pytest.fixture
def process_harness(
    e2e_database_url: str,
    e2e_rabbitmq_url: str,
    e2e_api_key: str,
    e2e_clean_tables: None,
    webhook_receiver: WebhookReceiver,
) -> Iterator[ProcessHarness]:
    """Полный набор процессов: API + publisher + consumer.

    Args:
        e2e_database_url: DSN.
        e2e_rabbitmq_url: AMQP URL.
        e2e_api_key: API-ключ.
        e2e_clean_tables: Чистые таблицы/очереди.
        webhook_receiver: Receiver должен быть уже поднятым.

    Yields:
        ProcessHarness с готовым API.
    """
    del webhook_receiver  # зависимость порядка старта
    env = build_e2e_env(
        database_url=e2e_database_url,
        rabbitmq_url=e2e_rabbitmq_url,
        api_key=e2e_api_key,
    )
    harness = ProcessHarness(base_env=env, api_key=e2e_api_key)
    harness.start_all_roles()
    try:
        harness.wait_api_ready(timeout=45.0)
        yield harness
    finally:
        harness.stop_all()


@pytest.fixture
def api_only_harness(
    e2e_database_url: str,
    e2e_rabbitmq_url: str,
    e2e_api_key: str,
    e2e_clean_tables: None,
    webhook_receiver: WebhookReceiver,
) -> Iterator[ProcessHarness]:
    """Только API (без publisher/consumer) для сценария crash/recovery Outbox.

    Args:
        e2e_database_url: DSN.
        e2e_rabbitmq_url: AMQP URL.
        e2e_api_key: API-ключ.
        e2e_clean_tables: Чистые таблицы.
        webhook_receiver: Receiver (для последующего webhook_url).

    Yields:
        ProcessHarness только с API.
    """
    del webhook_receiver
    env = build_e2e_env(
        database_url=e2e_database_url,
        rabbitmq_url=e2e_rabbitmq_url,
        api_key=e2e_api_key,
    )
    harness = ProcessHarness(base_env=env, api_key=e2e_api_key)
    harness.start_role("api")
    try:
        harness.wait_api_ready(timeout=45.0)
        yield harness
    finally:
        harness.stop_all()
