"""Канонический SHA-256 хэш тела запроса на создание платежа."""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Any


def format_amount(amount: Decimal) -> str:
    """Отформатировать сумму как строку с ровно двумя знаками.

    Args:
        amount: Сумма платежа.

    Returns:
        Строка вида ``"1500.50"``.
    """
    quantized = amount.quantize(Decimal("0.01"))
    return f"{quantized:.2f}"


def compute_request_hash(
    *,
    amount: Decimal,
    currency: str,
    description: str,
    metadata: dict[str, Any],
    webhook_url: str,
) -> str:
    """Вычислить стабильный SHA-256 от нормализованных бизнес-полей.

    Idempotency-Key в хэш не входит. Порядок ключей в ``metadata`` не влияет
    на результат за счёт ``sort_keys=True`` на всех уровнях JSON.

    Args:
        amount: Сумма платежа.
        currency: Код валюты.
        description: Описание (уже нормализованное / trim).
        metadata: Произвольный JSON-объект.
        webhook_url: URL webhook мерчанта.

    Returns:
        Hex-дайджест SHA-256 (64 символа).
    """
    canonical: dict[str, Any] = {
        "amount": format_amount(amount),
        "currency": currency,
        "description": description,
        "metadata": metadata,
        "webhook_url": webhook_url,
    }
    payload = json.dumps(
        canonical,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
