"""Конфигурация приложения из переменных окружения."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Настройки PayRelay, загружаемые из окружения.

    Секретные значения хранятся в `SecretStr` и не должны попадать в логи.
    Файл `.env` приложением не читается напрямую: переменные передаёт
    процесс запуска (shell export, Docker Compose `env_file`).
    """

    model_config = SettingsConfigDict(
        env_ignore_empty=True,
        extra="ignore",
    )

    app_env: Literal["development", "test", "production"] = Field(
        default="development",
        description="Режим окружения приложения.",
    )
    log_level: str = Field(default="INFO", description="Уровень логирования.")
    log_json: bool = Field(
        default=False,
        description="Включить JSON-формат логов (иначе компактный консольный).",
    )
    enable_docs: bool = Field(
        default=False,
        description="Включить Swagger/OpenAPI (только для локальной разработки).",
    )

    database_url: SecretStr = Field(
        ...,
        description="DSN PostgreSQL (asyncpg), например postgresql+asyncpg://...",
    )
    rabbitmq_url: SecretStr = Field(
        ...,
        description="URL подключения к RabbitMQ.",
    )
    api_key: SecretStr = Field(
        ...,
        description="Статический API-ключ для заголовка X-API-Key.",
    )

    webhook_timeout_seconds: float = Field(
        default=5.0,
        gt=0,
        description="Таймаут HTTP-запроса webhook.",
    )
    webhook_max_attempts: int = Field(
        default=3,
        ge=1,
        description="Максимум попыток доставки webhook (включая первую).",
    )
    webhook_allow_private_urls: bool = Field(
        default=False,
        description="Разрешить private/loopback URL webhook (только test/dev).",
    )

    outbox_poll_interval_seconds: float = Field(
        default=1.0,
        gt=0,
        description="Интервал опроса Outbox диспетчером.",
    )
    outbox_batch_size: int = Field(
        default=50,
        ge=1,
        description="Размер пачки записей Outbox за один цикл.",
    )
    outbox_claim_seconds: float = Field(
        default=30.0,
        gt=0,
        description="Длительность lease на публикацию Outbox-записи.",
    )

    processing_lease_seconds: float = Field(
        default=60.0,
        gt=0,
        description="Длительность lease обработки платежа consumer-ом.",
    )
    processing_prefetch_count: int = Field(
        default=10,
        ge=1,
        description="prefetch_count для consumer RabbitMQ.",
    )

    @property
    def is_production(self) -> bool:
        """Признак production-окружения.

        Returns:
            True, если `app_env` равен production.
        """
        return self.app_env == "production"

    @property
    def docs_enabled(self) -> bool:
        """Нужно ли отдавать OpenAPI/Swagger.

        Returns:
            True только вне production и при явном `enable_docs`.
        """
        return self.enable_docs and not self.is_production


@lru_cache
def get_settings() -> Settings:
    """Вернуть кэшированный экземпляр настроек.

    Returns:
        Валидированный объект `Settings`.

    Raises:
        ValidationError: если обязательные переменные окружения отсутствуют
            или имеют неверный тип.
    """
    return Settings()
