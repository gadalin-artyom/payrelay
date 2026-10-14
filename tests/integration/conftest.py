from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from urllib.parse import urlparse

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from payrelay.api.app import create_app
from payrelay.core.config import Settings, get_settings
from payrelay.infrastructure.db.session import create_engine, create_session_factory

_ALLOWED_TEST_DB_NAMES = frozenset({"payrelay_test"})
_ALLOWED_TEST_HOSTS = frozenset({"postgres-test", "localhost", "127.0.0.1"})
_ALLOWED_TEST_RABBIT_USERS = frozenset({"payrelay_test"})
_ALLOWED_TEST_RABBIT_HOSTS = frozenset({"rabbitmq-test", "localhost", "127.0.0.1"})
_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _normalize_db_url(url: str) -> str:
    """Привести DSN к виду, понятному urlparse.

    Args:
        url: Исходный DSN SQLAlchemy.

    Returns:
        URL со схемой postgresql://.
    """
    if url.startswith("postgresql+asyncpg://"):
        return "postgresql://" + url.removeprefix("postgresql+asyncpg://")
    return url


def assert_safe_test_database_url(database_url: str) -> None:
    """Отказать в работе, если DSN не указывает на тестовую БД.

    Args:
        database_url: DSN из окружения.

    Raises:
        RuntimeError: при небезопасном имени БД, хосте или совпадении с prod-шаблоном.
    """
    normalized = _normalize_db_url(database_url)
    parsed = urlparse(normalized)
    db_name = parsed.path.lstrip("/").split("?")[0]
    host = parsed.hostname or ""

    if db_name not in _ALLOWED_TEST_DB_NAMES:
        msg = (
            f"Refusing integration tests: database name must be "
            f"'payrelay_test', got {db_name!r}"
        )
        raise RuntimeError(msg)

    if host not in _ALLOWED_TEST_HOSTS:
        msg = (
            f"Refusing integration tests: host must be one of "
            f"{sorted(_ALLOWED_TEST_HOSTS)}, got {host!r}"
        )
        raise RuntimeError(msg)

    # Явный запрет «обычного» DSN compose без суффикса test.
    if "@postgres:" in database_url and "postgres-test" not in database_url:
        msg = "Refusing integration tests against non-test postgres service"
        raise RuntimeError(msg)


def assert_safe_test_rabbitmq_url(rabbitmq_url: str) -> None:
    """Отказать в работе, если URL RabbitMQ не тестовый.

    Args:
        rabbitmq_url: AMQP URL из окружения.

    Raises:
        RuntimeError: при небезопасном хосте/пользователе или prod-шаблоне.
    """
    parsed = urlparse(rabbitmq_url)
    host = parsed.hostname or ""
    user = parsed.username or ""

    if host not in _ALLOWED_TEST_RABBIT_HOSTS:
        msg = (
            f"Refusing integration tests: RabbitMQ host must be one of "
            f"{sorted(_ALLOWED_TEST_RABBIT_HOSTS)}, got {host!r}"
        )
        raise RuntimeError(msg)

    if user and user not in _ALLOWED_TEST_RABBIT_USERS:
        msg = (
            f"Refusing integration tests: RabbitMQ user must be "
            f"'payrelay_test', got {user!r}"
        )
        raise RuntimeError(msg)

    if "@rabbitmq:" in rabbitmq_url and "rabbitmq-test" not in rabbitmq_url:
        msg = "Refusing integration tests against non-test rabbitmq service"
        raise RuntimeError(msg)


def _run_alembic(command_name: str, revision: str) -> None:
    """Выполнить команду Alembic против текущего DATABASE_URL.

    Args:
        command_name: ``upgrade`` или ``downgrade``.
        revision: Цель ревизии (``head``, ``base``, ...).
    """
    from alembic import command
    from alembic.config import Config

    cfg = Config(str(_PROJECT_ROOT / "alembic.ini"))
    if command_name == "upgrade":
        command.upgrade(cfg, revision)
    elif command_name == "downgrade":
        command.downgrade(cfg, revision)
    else:
        msg = f"Unsupported alembic command: {command_name}"
        raise ValueError(msg)


@pytest.fixture(scope="session")
def integration_database_url() -> str:
    """Вернуть и проверить DATABASE_URL для integration-профиля.

    Returns:
        Безопасный тестовый DSN.

    Raises:
        RuntimeError: если переменная отсутствует или DSN небезопасен.
    """
    url = os.environ.get("DATABASE_URL")
    if not url:
        msg = "DATABASE_URL is required for integration tests"
        raise RuntimeError(msg)
    assert_safe_test_database_url(url)
    return url


@pytest.fixture(scope="session")
def integration_rabbitmq_url() -> str:
    """Вернуть и проверить RABBITMQ_URL для integration-профиля.

    Returns:
        Безопасный тестовый AMQP URL.

    Raises:
        RuntimeError: если переменная отсутствует или URL небезопасен.
    """
    url = os.environ.get("RABBITMQ_URL")
    if not url:
        msg = "RABBITMQ_URL is required for integration tests"
        raise RuntimeError(msg)
    assert_safe_test_rabbitmq_url(url)
    return url


@pytest.fixture(scope="session")
def integration_settings(
    integration_database_url: str,
    integration_rabbitmq_url: str,
) -> Settings:
    """Настройки приложения для integration-тестов.

    Args:
        integration_database_url: Проверенный DSN.
        integration_rabbitmq_url: Проверенный AMQP URL.

    Returns:
        Экземпляр `Settings`.
    """
    get_settings.cache_clear()
    api_key = os.environ.get("API_KEY", "test-api-key-not-for-production")
    return Settings(
        database_url=SecretStr(integration_database_url),
        rabbitmq_url=SecretStr(integration_rabbitmq_url),
        api_key=SecretStr(api_key),
        app_env="test",
        enable_docs=False,
        webhook_allow_private_urls=True,
        outbox_poll_interval_seconds=0.05,
        outbox_batch_size=10,
        outbox_lease_seconds=30,
        outbox_publish_timeout_seconds=5,
        outbox_retry_max_delay_seconds=30,
    )


@pytest.fixture(scope="session", autouse=True)
def migrated_schema(integration_database_url: str) -> Iterator[None]:
    """Один раз применить Alembic upgrade head к test DB.

    Args:
        integration_database_url: Проверенный DSN (для side-effect проверки).

    Yields:
        Управление сюитой тестов.
    """
    assert_safe_test_database_url(integration_database_url)
    # Settings в alembic/env.py читает DATABASE_URL из окружения.
    os.environ["DATABASE_URL"] = integration_database_url
    get_settings.cache_clear()
    _run_alembic("upgrade", "head")
    yield


@pytest.fixture
async def engine(
    integration_settings: Settings,
    integration_database_url: str,
) -> AsyncIterator[AsyncEngine]:
    """AsyncEngine, привязанный только к test DB.

    Args:
        integration_settings: Настройки.
        integration_database_url: DSN для повторной проверки.

    Yields:
        Движок SQLAlchemy.
    """
    assert_safe_test_database_url(integration_database_url)
    eng = create_engine(integration_settings, pool_size=10, max_overflow=20)
    try:
        yield eng
    finally:
        await eng.dispose()


@pytest.fixture
async def session_factory(
    engine: AsyncEngine,
) -> async_sessionmaker[AsyncSession]:
    """Фабрика сессий для прямых проверок БД.

    Args:
        engine: Тестовый движок.

    Returns:
        `async_sessionmaker`.
    """
    return create_session_factory(engine)


@pytest.fixture
async def clean_tables(
    session_factory: async_sessionmaker[AsyncSession],
    integration_database_url: str,
) -> AsyncIterator[None]:
    """Очистить payments/outbox перед тестом (только test DB).

    Args:
        session_factory: Фабрика сессий.
        integration_database_url: DSN для safety-check.

    Yields:
        Управление тесту.
    """
    assert_safe_test_database_url(integration_database_url)
    async with session_factory() as session:
        async with session.begin():
            await session.execute(text("TRUNCATE outbox, payments CASCADE"))
    yield


@pytest.fixture
async def api_client(
    integration_settings: Settings,
    clean_tables: None,
) -> AsyncIterator[tuple[AsyncClient, str]]:
    """HTTPX ASGI-клиент с ручным lifespan (реальная test DB).

    Args:
        integration_settings: Настройки.
        clean_tables: Гарантия чистых таблиц.

    Yields:
        Кортеж (client, api_key).
    """
    app = create_app(integration_settings)
    api_key = integration_settings.api_key.get_secret_value()
    # httpx.ASGITransport в используемой версии не принимает lifespan=;
    # поднимаем FastAPI lifespan явно.
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(
            transport=transport,
            base_url="http://test",
        ) as client:
            yield client, api_key
