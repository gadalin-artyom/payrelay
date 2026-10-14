"""Точка входа роли outbox-publisher."""

from __future__ import annotations

import asyncio
import logging
import signal

from faststream.rabbit import RabbitBroker

from payrelay.application.outbox_dispatcher import OutboxDispatcher, run_dispatcher_loop
from payrelay.core.config import Settings, get_settings
from payrelay.core.logging import setup_logging
from payrelay.infrastructure.db.session import create_engine, create_session_factory
from payrelay.infrastructure.messaging.broker import create_rabbit_broker
from payrelay.infrastructure.messaging.publisher import OutboxAmqpPublisher
from payrelay.infrastructure.messaging.topology import declare_payments_topology

logger = logging.getLogger(__name__)


async def _connect_broker_with_backoff(
    settings: Settings,
    stop_event: asyncio.Event,
) -> tuple[RabbitBroker, OutboxAmqpPublisher]:
    """Подключить брокер и объявить topology с повторными попытками.

    Args:
        settings: Настройки приложения.
        stop_event: Сигнал остановки.

    Returns:
        Кортеж ``(broker, publisher)``.

    Raises:
        asyncio.CancelledError: при отмене ожидания.
        RuntimeError: если остановка запрошена до успешного connect.
    """
    delay = settings.outbox_broker_reconnect_delay_seconds
    while not stop_event.is_set():
        broker = create_rabbit_broker(settings, fail_fast=False)
        try:
            await broker.connect()
            await declare_payments_topology(broker)
            publisher = OutboxAmqpPublisher(
                broker,
                confirm_timeout_seconds=settings.outbox_publish_timeout_seconds,
            )
            logger.info("RabbitMQ connected; payments topology declared")
            return broker, publisher
        except Exception:
            logger.exception(
                "RabbitMQ unavailable; retrying",
                extra={"retry_delay_seconds": delay},
            )
            try:
                await broker.stop()
            except Exception:
                logger.debug("Broker stop after failed connect ignored", exc_info=True)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=delay)
            except TimeoutError:
                continue
    msg = "Shutdown requested before RabbitMQ connection"
    raise RuntimeError(msg)


async def _run_publisher(settings: Settings) -> None:
    """Основной lifecycle publisher: DB + Rabbit + poll loop.

    Args:
        settings: Валидированные настройки.
    """
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_stop() -> None:
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _request_stop)

    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    broker = None
    dispatcher: OutboxDispatcher | None = None

    try:
        broker, publisher = await _connect_broker_with_backoff(settings, stop_event)
        dispatcher = OutboxDispatcher(
            session_factory=session_factory,
            publisher=publisher,
            settings=settings,
        )

        async def _watch_stop() -> None:
            await stop_event.wait()
            if dispatcher is not None:
                dispatcher.request_stop()

        watcher = asyncio.create_task(_watch_stop())
        try:
            await asyncio.wait_for(
                run_dispatcher_loop(
                    dispatcher,
                    stop_event=stop_event,
                    sleep=asyncio.sleep,
                ),
                timeout=None,
            )
        finally:
            watcher.cancel()
            try:
                await watcher
            except asyncio.CancelledError:
                pass
    except Exception:
        logger.exception("Outbox-publisher terminated with error")
        raise
    finally:
        if dispatcher is not None:
            dispatcher.request_stop()
        if broker is not None:
            try:
                await asyncio.wait_for(
                    broker.stop(),
                    timeout=settings.outbox_shutdown_grace_seconds,
                )
            except Exception:
                logger.exception("Error while stopping RabbitMQ broker")
        await engine.dispose()
        logger.info("Outbox-publisher stopped")


def main() -> None:
    """Запустить роль outbox-publisher.

    Settings валидируются при старте. Секреты в логи не выводятся.
    """
    settings = get_settings()
    setup_logging(settings)
    logger.info("Старт outbox-publisher", extra={"app_env": settings.app_env})
    asyncio.run(_run_publisher(settings))


if __name__ == "__main__":
    main()
