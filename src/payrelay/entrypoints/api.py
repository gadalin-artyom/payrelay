from __future__ import annotations

import logging

import uvicorn

from payrelay.api.app import create_app
from payrelay.core.config import get_settings
from payrelay.core.logging import setup_logging

logger = logging.getLogger(__name__)


def main() -> None:
    """Запустить FastAPI через uvicorn.

    Settings валидируются при старте. Секреты в логи не выводятся.
    """
    settings = get_settings()
    setup_logging(settings)
    app = create_app(settings)
    logger.info(
        "Запуск API",
        extra={"app_env": settings.app_env, "docs_enabled": settings.docs_enabled},
    )
    uvicorn.run(app, host="0.0.0.0", port=8000, log_config=None)


if __name__ == "__main__":
    main()
