"""Фабрика async-движка и сессий SQLAlchemy."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from payrelay.core.config import Settings


def create_engine(settings: Settings, **engine_kwargs: Any) -> AsyncEngine:
    """Создать AsyncEngine для PostgreSQL (asyncpg).

    Args:
        settings: Настройки с DSN.
        **engine_kwargs: Дополнительные аргументы `create_async_engine`.

    Returns:
        Настроенный асинхронный движок.
    """
    url = settings.database_url.get_secret_value()
    kwargs: dict[str, Any] = {"pool_pre_ping": True}
    kwargs.update(engine_kwargs)
    return create_async_engine(url, **kwargs)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Создать фабрику `AsyncSession`.

    Args:
        engine: Асинхронный движок SQLAlchemy.

    Returns:
        `async_sessionmaker` с `expire_on_commit=False`.
    """
    return async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )


async def session_scope(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    """Выдать сессию на время запроса и гарантированно закрыть её.

    Args:
        session_factory: Фабрика сессий приложения.

    Yields:
        Экземпляр `AsyncSession`.
    """
    async with session_factory() as session:
        yield session
