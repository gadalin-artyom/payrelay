from __future__ import annotations

from collections.abc import Iterator

import pytest
from pydantic import SecretStr

from payrelay.core.config import Settings, get_settings


@pytest.fixture(autouse=True)
def _clear_settings_cache() -> Iterator[None]:
    """Сбрасывать кэш Settings между тестами.

    Yields:
        Управление обратно в тест.
    """
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def test_settings(monkeypatch: pytest.MonkeyPatch) -> Settings:
    """Подготовить детерминированные настройки для unit-тестов.

    Args:
        monkeypatch: Фикстура pytest для подмены env.

    Returns:
        Экземпляр `Settings` без чтения секретных файлов.
    """
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+asyncpg://payrelay:payrelay@localhost:5432/payrelay_test",
    )
    monkeypatch.setenv("RABBITMQ_URL", "amqp://payrelay:payrelay@localhost:5672/")
    monkeypatch.setenv("API_KEY", "test-api-key-not-for-production")
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("ENABLE_DOCS", "false")
    # Не подхватываем локальный .env с чужими секретами.
    monkeypatch.delenv("ENV_FILE", raising=False)
    get_settings.cache_clear()
    settings = Settings(
        database_url=SecretStr(
            "postgresql+asyncpg://payrelay:payrelay@localhost:5432/payrelay_test"
        ),
        rabbitmq_url=SecretStr("amqp://payrelay:payrelay@localhost:5672/"),
        api_key=SecretStr("test-api-key-not-for-production"),
        app_env="test",
        enable_docs=False,
    )
    return settings


@pytest.fixture
def api_key(test_settings: Settings) -> str:
    """Вернуть тестовый API-ключ.

    Args:
        test_settings: Настройки теста.

    Returns:
        Строковое значение ключа.
    """
    return test_settings.api_key.get_secret_value()


def pytest_configure(config: pytest.Config) -> None:
    """Зарегистрировать маркеры (дублирует pyproject, полезно для IDE).

    Args:
        config: Конфигурация pytest.
    """
    config.addinivalue_line("markers", "unit: юнит-тесты без внешней инфраструктуры")
    config.addinivalue_line(
        "markers",
        "integration: интеграционные тесты с PostgreSQL/RabbitMQ",
    )
    config.addinivalue_line("markers", "e2e: сквозные сценарии")
