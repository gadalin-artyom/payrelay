"""Конфигурация приложения из переменных окружения."""

from functools import lru_cache
from typing import Literal, Self

from pydantic import Field, SecretStr, model_validator
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
        default=0.5,
        gt=0,
        description="Пауза после пустого опроса Outbox (без busy loop).",
    )
    outbox_batch_size: int = Field(
        default=50,
        ge=1,
        description="Максимум событий Outbox за один тик диспетчера.",
    )
    outbox_lease_seconds: float = Field(
        default=30.0,
        gt=0,
        description="Длительность lease (claimed_until) на публикацию Outbox.",
    )
    outbox_publish_timeout_seconds: float = Field(
        default=5.0,
        gt=0,
        description="Таймаут ожидания broker confirm при публикации.",
    )
    outbox_retry_max_delay_seconds: float = Field(
        default=30.0,
        gt=0,
        description="Верхняя граница backoff повторной публикации в брокер.",
    )
    outbox_shutdown_grace_seconds: float = Field(
        default=15.0,
        gt=0,
        description="Grace timeout завершения текущей операции publisher.",
    )
    outbox_broker_reconnect_delay_seconds: float = Field(
        default=2.0,
        gt=0,
        description="Пауза перед повторным подключением к RabbitMQ.",
    )
    outbox_db_retry_delay_seconds: float = Field(
        default=2.0,
        gt=0,
        description="Пауза при временной недоступности PostgreSQL.",
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

    @model_validator(mode="after")
    def _validate_outbox_lease_vs_confirm(self) -> Self:
        """Гарантировать запас lease относительно confirm timeout.

        Returns:
            Валидированный экземпляр настроек.

        Raises:
            ValueError: если lease недостаточно длиннее confirm timeout.
        """
        # Запас на DB-операции вокруг publish (claim/mark).
        min_lease = self.outbox_publish_timeout_seconds + 5.0
        if self.outbox_lease_seconds < min_lease:
            msg = (
                "OUTBOX_LEASE_SECONDS must be >= OUTBOX_PUBLISH_TIMEOUT_SECONDS + 5 "
                f"(got lease={self.outbox_lease_seconds}, "
                f"confirm={self.outbox_publish_timeout_seconds})"
            )
            raise ValueError(msg)
        return self

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
