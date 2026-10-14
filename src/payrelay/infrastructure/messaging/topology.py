"""Имена и объявление топологии RabbitMQ для PayRelay.

Единый источник имён exchanges/queues/routing keys для publisher
и будущего consumer (этапы 004–005).
"""

from __future__ import annotations

from dataclasses import dataclass

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

# --- Имена и routing keys (не менять без согласования контракта) ---

PAYMENTS_EXCHANGE = "payments.exchange"
PAYMENTS_DLX = "payments.dlx"

QUEUE_PAYMENTS_NEW = "payments.new"
QUEUE_RETRY_2 = "payments.retry.2"
QUEUE_RETRY_3 = "payments.retry.3"
QUEUE_DLQ = "payments.dlq"

ROUTING_KEY_NEW = "payments.new"
ROUTING_KEY_RETRY_2 = "payments.retry.2"
ROUTING_KEY_RETRY_3 = "payments.retry.3"
ROUTING_KEY_DLQ = "payments.dlq"

RETRY_2_TTL_MS = 2000
RETRY_3_TTL_MS = 4000


@dataclass(frozen=True, slots=True)
class PaymentsTopology:
    """Объекты FastStream для durable topology PayRelay."""

    payments_exchange: RabbitExchange
    dlx_exchange: RabbitExchange
    payments_new: RabbitQueue
    retry_2: RabbitQueue
    retry_3: RabbitQueue
    dlq: RabbitQueue


def build_payments_topology() -> PaymentsTopology:
    """Собрать схемы exchanges/queues без сетевых вызовов.

    Returns:
        Набор `RabbitExchange` / `RabbitQueue` с ожидаемыми аргументами.
    """
    payments_exchange = RabbitExchange(
        PAYMENTS_EXCHANGE,
        type=ExchangeType.DIRECT,
        durable=True,
        auto_delete=False,
    )
    dlx_exchange = RabbitExchange(
        PAYMENTS_DLX,
        type=ExchangeType.DIRECT,
        durable=True,
        auto_delete=False,
    )
    payments_new = RabbitQueue(
        QUEUE_PAYMENTS_NEW,
        durable=True,
        exclusive=False,
        auto_delete=False,
        arguments={
            "x-dead-letter-exchange": PAYMENTS_DLX,
            "x-dead-letter-routing-key": ROUTING_KEY_DLQ,
        },
        routing_key=ROUTING_KEY_NEW,
    )
    retry_2 = RabbitQueue(
        QUEUE_RETRY_2,
        durable=True,
        exclusive=False,
        auto_delete=False,
        arguments={
            "x-message-ttl": RETRY_2_TTL_MS,
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE,
            "x-dead-letter-routing-key": ROUTING_KEY_NEW,
        },
        routing_key=ROUTING_KEY_RETRY_2,
    )
    retry_3 = RabbitQueue(
        QUEUE_RETRY_3,
        durable=True,
        exclusive=False,
        auto_delete=False,
        arguments={
            "x-message-ttl": RETRY_3_TTL_MS,
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE,
            "x-dead-letter-routing-key": ROUTING_KEY_NEW,
        },
        routing_key=ROUTING_KEY_RETRY_3,
    )
    dlq = RabbitQueue(
        QUEUE_DLQ,
        durable=True,
        exclusive=False,
        auto_delete=False,
        routing_key=ROUTING_KEY_DLQ,
    )
    return PaymentsTopology(
        payments_exchange=payments_exchange,
        dlx_exchange=dlx_exchange,
        payments_new=payments_new,
        retry_2=retry_2,
        retry_3=retry_3,
        dlq=dlq,
    )


async def declare_payments_topology(broker: RabbitBroker) -> PaymentsTopology:
    """Объявить exchanges/queues и привязки (идемпотентно).

    Не удаляет существующие объекты. При PRECONDITION_FAILED (несовместимая
    топология) исключение пробрасывается наверх — автоматического delete нет.

    Args:
        broker: Подключённый `RabbitBroker`.

    Returns:
        Схемы топологии, использованные при declare/bind.
    """
    topology = build_payments_topology()

    payments_ex = await broker.declare_exchange(topology.payments_exchange)
    dlx_ex = await broker.declare_exchange(topology.dlx_exchange)

    new_q = await broker.declare_queue(topology.payments_new)
    retry2_q = await broker.declare_queue(topology.retry_2)
    retry3_q = await broker.declare_queue(topology.retry_3)
    dlq_q = await broker.declare_queue(topology.dlq)

    await new_q.bind(payments_ex, routing_key=ROUTING_KEY_NEW)
    await retry2_q.bind(payments_ex, routing_key=ROUTING_KEY_RETRY_2)
    await retry3_q.bind(payments_ex, routing_key=ROUTING_KEY_RETRY_3)
    await dlq_q.bind(dlx_ex, routing_key=ROUTING_KEY_DLQ)

    return topology
