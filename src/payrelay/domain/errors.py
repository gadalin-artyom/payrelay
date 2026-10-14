"""Доменные ошибки PayRelay."""

from __future__ import annotations


class DomainError(Exception):
    """Базовая ошибка домена."""


class IdempotencyConflictError(DomainError):
    """Тот же Idempotency-Key при несовпадающем теле запроса."""

    def __init__(
        self,
        message: str = "Idempotency-Key reuse with different body",
    ) -> None:
        """Инициализировать ошибку конфликта идемпотентности.

        Args:
            message: Человекочитаемое описание.
        """
        super().__init__(message)


class PaymentNotFoundError(DomainError):
    """Платёж с указанным идентификатором не найден."""

    def __init__(self, payment_id: str) -> None:
        """Инициализировать ошибку отсутствия платежа.

        Args:
            payment_id: Идентификатор запрошенного платежа.
        """
        self.payment_id = payment_id
        super().__init__(f"Payment not found: {payment_id}")
