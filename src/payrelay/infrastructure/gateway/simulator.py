"""Детерминированный симулятор платёжного шлюза (90/10, задержка 2–5 с)."""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from payrelay.domain.enums import PaymentStatus

SleepFunc = Callable[[float], Awaitable[None]]


class PaymentGateway(Protocol):
    """Протокол шлюза для инъекции в application-слой."""

    async def charge(self, payment_id: uuid.UUID) -> PaymentStatus:
        """Выполнить (эмулировать) списание по платежу.

        Args:
            payment_id: Идентификатор платежа.

        Returns:
            Терминальный бизнес-статус ``succeeded`` или ``failed``.
        """
        ...


@dataclass(frozen=True, slots=True)
class GatewayOutcome:
    """Результат симуляции без побочных эффектов в БД."""

    status: PaymentStatus
    delay_seconds: float


def deterministic_outcome(payment_id: uuid.UUID) -> PaymentStatus:
    """Вычислить стабильный outcome по ``payment_id`` (доля ~90/10).

    Используется ``sha256(payment_id.bytes)``: значения ``0..8`` из
    ``value % 10`` дают ``succeeded``, ``9`` — ``failed``. Это распределение
    по UUID, а не гарантия ровно 9 из 10 на каждой выборке.

    Args:
        payment_id: UUID платежа.

    Returns:
        ``PaymentStatus.SUCCEEDED`` или ``PaymentStatus.FAILED``.
    """
    digest = hashlib.sha256(payment_id.bytes).digest()
    value = int.from_bytes(digest[:8], "big", signed=False)
    if value % 10 < 9:
        return PaymentStatus.SUCCEEDED
    return PaymentStatus.FAILED


def deterministic_delay_seconds(
    payment_id: uuid.UUID,
    *,
    min_seconds: float = 2.0,
    max_seconds: float = 5.0,
) -> float:
    """Вычислить стабильную задержку в ``[min_seconds, max_seconds]``.

    Args:
        payment_id: UUID платежа.
        min_seconds: Нижняя граница задержки.
        max_seconds: Верхняя граница задержки.

    Returns:
        Длительность ожидания в секундах.

    Raises:
        ValueError: если границы некорректны.
    """
    if min_seconds <= 0 or max_seconds < min_seconds:
        msg = "gateway delay bounds must satisfy 0 < min <= max"
        raise ValueError(msg)
    digest = hashlib.sha256(payment_id.bytes).digest()
    value = int.from_bytes(digest[8:16], "big", signed=False)
    span = max_seconds - min_seconds
    if span == 0:
        return float(min_seconds)
    # Равномерное отображение в [0, 1) без float-сумм денег.
    fraction = (value % 1_000_000) / 1_000_000.0
    return float(min_seconds + span * fraction)


class GatewaySimulator:
    """Тестируемый эмулятор шлюза: не мутирует БД, outcome стабилен."""

    def __init__(
        self,
        *,
        sleep: SleepFunc,
        min_delay_seconds: float = 2.0,
        max_delay_seconds: float = 5.0,
        forced_outcome: PaymentStatus | None = None,
    ) -> None:
        """Инициализировать симулятор.

        Args:
            sleep: Async sleep (инъекция для тестов / ``asyncio.sleep``).
            min_delay_seconds: Минимальная задержка.
            max_delay_seconds: Максимальная задержка.
            forced_outcome: Принудительный статус (только для тестов).
        """
        self._sleep = sleep
        self._min_delay_seconds = min_delay_seconds
        self._max_delay_seconds = max_delay_seconds
        self._forced_outcome = forced_outcome

    def preview(self, payment_id: uuid.UUID) -> GatewayOutcome:
        """Показать outcome и delay без ожидания.

        Args:
            payment_id: UUID платежа.

        Returns:
            Структура ``GatewayOutcome``.
        """
        status = self._forced_outcome or deterministic_outcome(payment_id)
        delay = deterministic_delay_seconds(
            payment_id,
            min_seconds=self._min_delay_seconds,
            max_seconds=self._max_delay_seconds,
        )
        return GatewayOutcome(status=status, delay_seconds=delay)

    async def charge(self, payment_id: uuid.UUID) -> PaymentStatus:
        """Эмулировать вызов шлюза с задержкой.

        Args:
            payment_id: UUID платежа.

        Returns:
            Терминальный бизнес-статус.

        Raises:
            Exception: техническая ошибка симулятора (не бизнес-``failed``).
        """
        outcome = self.preview(payment_id)
        await self._sleep(outcome.delay_seconds)
        return outcome.status
