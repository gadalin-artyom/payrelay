"""Настройка структурированного логирования."""

from __future__ import annotations

import logging
import sys
from typing import Any

from payrelay.core.config import Settings

_RESERVED_LOG_RECORD_KEYS = frozenset(
    {
        "name",
        "msg",
        "args",
        "created",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "message",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "exc_info",
        "exc_text",
        "thread",
        "threadName",
        "taskName",
    }
)


class JsonFormatter(logging.Formatter):
    """Форматтер JSON-логов с полями трассировки."""

    def format(self, record: logging.LogRecord) -> str:
        """Сериализовать запись лога в одну JSON-строку.

        Args:
            record: Запись стандартного logging.

        Returns:
            JSON-строка без переносов.
        """
        import json
        from datetime import UTC, datetime

        payload: dict[str, Any] = {
            "ts": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key in _RESERVED_LOG_RECORD_KEYS or key.startswith("_"):
                continue
            payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


class CompactFormatter(logging.Formatter):
    """Компактный консольный формат для локальной разработки."""

    def format(self, record: logging.LogRecord) -> str:
        """Собрать компактную строку лога с extra-полями.

        Args:
            record: Запись стандартного logging.

        Returns:
            Человекочитаемая строка лога.
        """
        base = super().format(record)
        extras: list[str] = []
        for key, value in record.__dict__.items():
            if key in _RESERVED_LOG_RECORD_KEYS or key.startswith("_"):
                continue
            extras.append(f"{key}={value}")
        if extras:
            return f"{base} | {' '.join(extras)}"
        return base


def setup_logging(settings: Settings) -> None:
    """Настроить корневой логгер приложения.

    Args:
        settings: Настройки приложения с уровнем и форматом логов.
    """
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(settings.log_level.upper())

    handler = logging.StreamHandler(sys.stdout)
    if settings.log_json:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            CompactFormatter(
                fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
        )
    root.addHandler(handler)

    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
