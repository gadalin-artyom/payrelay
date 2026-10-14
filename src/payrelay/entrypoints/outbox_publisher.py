"""Точка входа outbox-publisher (заглушка этапа 001)."""

from __future__ import annotations

import asyncio
import logging
import signal

from payrelay.core.config import get_settings
from payrelay.core.logging import setup_logging

logger = logging.getLogger(__name__)


async def _run_stub() -> None:
    """Удерживать процесс до SIGTERM/SIGINT без busy loop и без публикации.

    Реальная публикация Outbox появится на этапе 003.
    """
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_stop() -> None:
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _request_stop)

    logger.info(
        "Outbox-publisher stub запущен; ожидание сигнала завершения "
        "(публикация ещё не реализована)"
    )
    await stop_event.wait()
    logger.info("Outbox-publisher stub завершён")


def main() -> None:
    """Запустить заглушку роли outbox-publisher.

    Settings валидируются при старте. Сообщения в брокер не публикуются.
    """
    settings = get_settings()
    setup_logging(settings)
    logger.info("Старт outbox-publisher", extra={"app_env": settings.app_env})
    asyncio.run(_run_stub())


if __name__ == "__main__":
    main()
