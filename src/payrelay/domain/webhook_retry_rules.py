"""Чистые правила webhook/system retry (без I/O)."""

from __future__ import annotations

from payrelay.domain.enums import OutboxEventType

MAX_WEBHOOK_ATTEMPTS: int = 3
MAX_SYSTEM_ATTEMPTS: int = 3
SYSTEM_ATTEMPT_HEADER: str = "system_attempt"

# Задержка перед следующей попыткой webhook после неуспеха attempt N.
# Ключ — номер завершившейся неуспешной попытки; значение — секунды.
WEBHOOK_RETRY_DELAY_SECONDS: dict[int, int] = {
    1: 2,
    2: 4,
}


def webhook_retry_delay_seconds(*, failed_attempt: int) -> int | None:
    """Вернуть задержку Outbox перед следующей попыткой webhook.

    Args:
        failed_attempt: Номер только что завершившейся HTTP-попытки (1..3).

    Returns:
        Секунды задержки для следующей попытки либо ``None``, если retry нет.
    """
    if failed_attempt < 1 or failed_attempt >= MAX_WEBHOOK_ATTEMPTS:
        return None
    return WEBHOOK_RETRY_DELAY_SECONDS.get(failed_attempt)


def next_webhook_attempt(*, failed_attempt: int) -> int | None:
    """Вычислить номер следующей логической попытки webhook.

    Args:
        failed_attempt: Номер завершившейся неуспешной попытки.

    Returns:
        Следующий attempt (2 или 3) либо ``None`` после третьей.
    """
    if failed_attempt < 1 or failed_attempt >= MAX_WEBHOOK_ATTEMPTS:
        return None
    return failed_attempt + 1


def is_valid_event_attempt(*, event_type: str, attempt: int) -> bool:
    """Проверить согласованность ``event_type`` и ``attempt``.

    Args:
        event_type: Тип события из payload.
        attempt: Номер логической попытки webhook.

    Returns:
        ``True``, если комбинация допустима для consumer.
    """
    if attempt < 1 or attempt > MAX_WEBHOOK_ATTEMPTS:
        return False
    if event_type == OutboxEventType.PAYMENT_CREATED.value:
        return attempt == 1
    if event_type == OutboxEventType.PAYMENT_WEBHOOK_RETRY.value:
        return attempt in (2, 3)
    return False


def parse_system_attempt(raw: object | None) -> int | None:
    """Разобрать заголовок ``system_attempt``.

    Отсутствие заголовка трактуется как 1 (сообщения этапов до 005).
    Невалидное значение — ``None`` (poison → DLQ).

    Args:
        raw: Значение из AMQP headers или ``None``.

    Returns:
        Целое 1..MAX_SYSTEM_ATTEMPTS либо ``None`` при ошибке.
    """
    if raw is None:
        return 1
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        value = raw
    elif isinstance(raw, str) and raw.strip().isdigit():
        value = int(raw.strip())
    else:
        return None
    if value < 1 or value > MAX_SYSTEM_ATTEMPTS:
        return None
    return value


def system_retry_routing_key(*, current_system_attempt: int) -> str | None:
    """Выбрать routing key TTL-очереди для следующей системной попытки.

    Args:
        current_system_attempt: Текущий ``system_attempt`` упавшего сообщения.

    Returns:
        ``payments.retry.2`` / ``payments.retry.3`` либо ``None`` (пора в DLQ).
    """
    if current_system_attempt == 1:
        return "payments.retry.2"
    if current_system_attempt == 2:
        return "payments.retry.3"
    return None


def next_system_attempt(*, current_system_attempt: int) -> int | None:
    """Следующее значение заголовка ``system_attempt``.

    Args:
        current_system_attempt: Текущая системная попытка.

    Returns:
        Увеличенное значение либо ``None``, если лимит исчерпан.
    """
    nxt = current_system_attempt + 1
    if nxt > MAX_SYSTEM_ATTEMPTS:
        return None
    return nxt
