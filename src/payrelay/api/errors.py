from __future__ import annotations


class InvalidAPIKeyError(Exception):
    """Отсутствует или неверен заголовок X-API-Key."""


class InvalidRequestHeaderError(Exception):
    """Некорректный или отсутствующий обязательный HTTP-заголовок запроса."""

    def __init__(
        self,
        *,
        header_name: str,
        error_type: str,
        message: str,
    ) -> None:
        """Инициализировать ошибку заголовка.

        Args:
            header_name: Имя заголовка (как в HTTP, например ``Idempotency-Key``).
            error_type: Стабильный тип ошибки (``missing``, ``value_error`` и т.п.).
            message: Локализованное сообщение для клиента.
        """
        self.header_name = header_name
        self.error_type = error_type
        self.message = message
        super().__init__(message)
