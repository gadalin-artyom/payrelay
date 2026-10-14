"""Точка входа роли consumer (FastStream + gateway + webhook)."""

from __future__ import annotations

import asyncio
import logging
import signal

from faststream.rabbit import RabbitBroker

from payrelay.application.process_payment import ProcessPaymentService
from payrelay.core.config import Settings, get_settings
from payrelay.core.logging import setup_logging
from payrelay.infrastructure.db.session import create_engine, create_session_factory
from payrelay.infrastructure.gateway.simulator import GatewaySimulator
from payrelay.infrastructure.messaging.broker import create_rabbit_broker
from payrelay.infrastructure.messaging.consumer import register_payments_consumer
from payrelay.infrastructure.messaging.topology import declare_payments_topology
from payrelay.infrastructure.webhooks.http_client import WebhookHttpClient

logger = logging.getLogger(__name__)


async def _connect_broker_with_backoff(
    settings: Settings,
    stop_event: asyncio.Event,
) -> RabbitBroker:
    """Подключить брокер consumer с prefetch и объявить topology.

    Args:
        settings: Настройки приложения.
        stop_event: Сигнал остановки.

    Returns:
        Подключённый `RabbitBroker`.

    Raises:
        RuntimeError: если остановка запрошена до успешного connect.
    """
    delay = settings.outbox_broker_reconnect_delay_seconds
    while not stop_event.is_set():
        broker = create_rabbit_broker(
            settings,
            fail_fast=False,
            prefetch_count=settings.processing_prefetch_count,
        )
        try:
            await broker.connect()
            await declare_payments_topology(broker)
            logger.info(
                "RabbitMQ connected; payments topology declared",
                extra={"prefetch_count": settings.processing_prefetch_count},
            )
            return broker
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


async def _run_consumer(settings: Settings) -> None:
    """Lifecycle consumer: DB + Rabbit + HTTP webhook client.

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
    webhook_client = WebhookHttpClient(
        timeout_seconds=settings.webhook_timeout_seconds,
        connect_timeout_seconds=settings.webhook_connect_timeout_seconds,
        allow_private_urls=settings.webhook_allow_private_urls,
    )
    gateway = GatewaySimulator(
        sleep=asyncio.sleep,
        min_delay_seconds=settings.gateway_min_delay_seconds,
        max_delay_seconds=settings.gateway_max_delay_seconds,
    )
    service = ProcessPaymentService(
        session_factory=session_factory,
        gateway=gateway,
        webhook_client=webhook_client,
        settings=settings,
        sleep=asyncio.sleep,
    )
    broker: RabbitBroker | None = None

    try:
        broker = await _connect_broker_with_backoff(settings, stop_event)
        register_payments_consumer(broker, service)
        await broker.start()
        logger.info("Consumer started; waiting for payments.new messages")
        await stop_event.wait()
        logger.info("Shutdown signal received; stopping consumer")
    except Exception:
        logger.exception("Consumer terminated with error")
        raise
    finally:
        if broker is not None:
            try:
                await asyncio.wait_for(
                    broker.stop(),
                    timeout=settings.consumer_shutdown_grace_seconds,
                )
            except Exception:
                logger.exception("Error while stopping RabbitMQ broker")
        try:
            await webhook_client.aclose()
        except Exception:
            logger.exception("Error while closing webhook HTTP client")
        await engine.dispose()
        logger.info("Consumer stopped")


def main() -> None:
    """Запустить роль consumer.

    Settings валидируются при старте. Секреты в логи не выводятся.
    """
    settings = get_settings()
    setup_logging(settings)
    logger.info("Старт consumer", extra={"app_env": settings.app_env})
    asyncio.run(_run_consumer(settings))


if __name__ == "__main__":
    main()
