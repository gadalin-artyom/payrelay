from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from payrelay.api.dependencies import require_api_key
from payrelay.api.exception_handlers import register_exception_handlers
from payrelay.api.routers.payments import router as payments_router
from payrelay.core.config import Settings, get_settings
from payrelay.infrastructure.db.session import create_engine, create_session_factory


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Инициализировать и корректно закрыть AsyncEngine.

    Args:
        app: Экземпляр FastAPI.

    Yields:
        Управление на время жизни приложения.
    """
    settings: Settings = app.state.settings
    engine: AsyncEngine = create_engine(settings)
    session_factory: async_sessionmaker[AsyncSession] = create_session_factory(engine)
    app.state.engine = engine
    app.state.session_factory = session_factory
    try:
        yield
    finally:
        await engine.dispose()


def create_app(settings: Settings | None = None) -> FastAPI:
    """Создать экземпляр FastAPI с платежным API и health.

    Args:
        settings: Явные настройки; если None — загружаются из окружения.

    Returns:
        Настроенное приложение FastAPI.
    """
    resolved = settings or get_settings()
    docs_url = "/docs" if resolved.docs_enabled else None
    redoc_url = "/redoc" if resolved.docs_enabled else None
    openapi_url = "/openapi.json" if resolved.docs_enabled else None

    app = FastAPI(
        title="PayRelay",
        version="0.1.0",
        description=(
            "Асинхронный сервис приёма платежей. "
            "Авторизация: заголовок X-API-Key (кнопка Authorize в Swagger). "
            "Для POST /api/v1/payments клиент обязан передать Idempotency-Key "
            "(рекомендуется UUID v4). Сообщения об ошибках API — на русском; "
            "идентификаторы полей и заголовков не локализуются. "
            "Документация доступна только при ENABLE_DOCS=true вне production."
        ),
        docs_url=docs_url,
        redoc_url=redoc_url,
        openapi_url=openapi_url,
        lifespan=_lifespan,
    )
    app.state.settings = resolved
    register_exception_handlers(app)
    app.include_router(payments_router)

    @app.get(
        "/health/live",
        tags=["health"],
        summary="Проверка жизнеспособности",
        description="Liveness probe. Требует валидный X-API-Key.",
        dependencies=[Depends(require_api_key)],
    )
    async def health_live() -> JSONResponse:
        """Вернуть признак жизнеспособности процесса API.

        Returns:
            JSON со статусом `ok`.
        """
        return JSONResponse(content={"status": "ok"})

    return app
