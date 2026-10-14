"""Фабрика RabbitBroker с publisher confirms."""

from __future__ import annotations

from faststream.rabbit import Channel, RabbitBroker

from payrelay.core.config import Settings


def create_rabbit_broker(settings: Settings, *, fail_fast: bool = True) -> RabbitBroker:
    """Создать брокер с publisher confirms и mandatory-return raises.

    Args:
        settings: Настройки с `rabbitmq_url`.
        fail_fast: Пробрасывать ошибку соединения при старте.

    Returns:
        Настроенный, ещё не подключённый `RabbitBroker`.
    """
    channel = Channel(
        publisher_confirms=True,
        on_return_raises=True,
    )
    return RabbitBroker(
        url=settings.rabbitmq_url.get_secret_value(),
        default_channel=channel,
        fail_fast=fail_fast,
        reconnect_interval=5.0,
    )
