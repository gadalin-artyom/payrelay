"""Доменные перечисления статусов и валют."""

from __future__ import annotations

from enum import StrEnum


class Currency(StrEnum):
    """Допустимые валюты платежа."""

    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    """Бизнес-статус платежа."""

    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class WebhookDeliveryStatus(StrEnum):
    """Статус доставки webhook (независимо от бизнес-статуса)."""

    PENDING = "pending"
    DELIVERED = "delivered"
    EXHAUSTED = "exhausted"


class OutboxEventType(StrEnum):
    """Типы событий Outbox, используемые в v1."""

    PAYMENT_CREATED = "payment.created"
    PAYMENT_WEBHOOK_RETRY = "payment.webhook_retry"
