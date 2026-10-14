"""Точка входа consumer (заглушка этапа 001)."""

from __future__ import annotations

import asyncio
import logging
import signal

from payrelay.core.config import get_settings
from payrelay.core.logging import setup_logging

logger = logging.getLogger(__name__)


async def _run_stub() -> None:
    """Удерживать процесс до SIGTERM/SIGINT без busy loop и без потребления очереди.

    Реальная подписка на RabbitMQ появится на этапе 004.
    """
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_stop() -> None:
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _request_stop)

    logger.info(
        "Consumer stub запущен; ожидание сигнала завершения "
        "(обработка платежей ещё не реализована)"
    )
    await stop_event.wait()
    logger.info("Consumer stub завершён")


def main() -> None:
    """Запустить заглушку consumer-роли.

    Settings валидируются при старте. Сообщения из очереди не читаются.
    """
    settings = get_settings()
    setup_logging(settings)
    logger.info("Старт consumer", extra={"app_env": settings.app_env})
    asyncio.run(_run_stub())


if __name__ == "__main__":
    main()
