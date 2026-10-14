from __future__ import annotations

from datetime import datetime


def is_outbox_claimable(
    *,
    published_at: datetime | None,
    available_at: datetime,
    claimed_until: datetime | None,
    now: datetime,
) -> bool:
    """Проверить, доступна ли запись Outbox для claim.

    Args:
        published_at: Момент успешной публикации или ``None``.
        available_at: Момент, с которого запись снова доступна.
        claimed_until: Конец активного lease или ``None``.
        now: Текущий момент (обычно UTC).

    Returns:
        ``True``, если запись можно захватывать.
    """
    if published_at is not None:
        return False
    if available_at > now:
        return False
    if claimed_until is not None and claimed_until > now:
        return False
    return True


def publish_backoff_seconds(*, attempts: int, max_delay_seconds: float) -> float:
    """Вычислить задержку повторной публикации в брокер.

    Не путать с ``payload.attempt`` (webhook/business delivery).
    ``attempts`` — уже увеличенный счётчик publish-попыток Outbox.

    Args:
        attempts: Значение ``outbox.attempts`` после claim (>= 1).
        max_delay_seconds: Верхняя граница backoff.

    Returns:
        Число секунд до следующей попытки (неотрицательное).

    Raises:
        ValueError: если ``attempts < 1`` или ``max_delay_seconds <= 0``.
    """
    if attempts < 1:
        msg = "attempts must be >= 1"
        raise ValueError(msg)
    if max_delay_seconds <= 0:
        msg = "max_delay_seconds must be > 0"
        raise ValueError(msg)
    return float(min(2 ** (attempts - 1), max_delay_seconds))
