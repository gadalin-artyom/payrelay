"""Фабрика FastAPI-приложения."""

from __future__ import annotations

from fastapi import Depends, FastAPI
from fastapi.responses import JSONResponse

from payrelay.api.dependencies import require_api_key
from payrelay.core.config import Settings, get_settings


def create_app(settings: Settings | None = None) -> FastAPI:
    """Создать экземпляр FastAPI с техническими маршрутами этапа 001.

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
        docs_url=docs_url,
        redoc_url=redoc_url,
        openapi_url=openapi_url,
    )
    app.state.settings = resolved

    @app.get(
        "/health/live",
        tags=["health"],
        summary="Liveness probe",
        dependencies=[Depends(require_api_key)],
    )
    async def health_live() -> JSONResponse:
        """Вернуть признак жизнеспособности процесса API.

        Returns:
            JSON со статусом `ok`.
        """
        return JSONResponse(content={"status": "ok"})

    return app
