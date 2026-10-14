from __future__ import annotations

import hashlib
import hmac
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Header, Request, Security
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payrelay.api.constants import API_KEY_HEADER_NAME, IDEMPOTENCY_KEY_HEADER_NAME
from payrelay.api.errors import InvalidAPIKeyError, InvalidRequestHeaderError
from payrelay.api.messages import IDEMPOTENCY_KEY_MISSING
from payrelay.api.schemas import validate_idempotency_key_value
from payrelay.core.config import Settings, get_settings

api_key_header = APIKeyHeader(
    name=API_KEY_HEADER_NAME,
    auto_error=False,
    description="Статический API-ключ клиента. Вводится в Swagger через Authorize.",
)

_IDEMPOTENCY_KEY_HEADER = Header(
    alias=IDEMPOTENCY_KEY_HEADER_NAME,
    description=(
        "Ключ идемпотентности, который генерирует клиент. "
        "Рекомендуется UUID v4, но допустима любая непрозрачная строка "
        "без пробелов по краям (1–255 символов)."
    ),
    examples=["11111111-1111-4111-8111-111111111111"],
)


def _api_keys_match(provided: str, expected: str) -> bool:
    """Сравнить API-ключи в constant-time через SHA-256 дайджесты.

    Args:
        provided: Значение из заголовка запроса.
        expected: Ожидаемый ключ из настроек.

    Returns:
        True, если ключи совпадают.
    """
    provided_digest = hashlib.sha256(provided.encode("utf-8")).digest()
    expected_digest = hashlib.sha256(expected.encode("utf-8")).digest()
    return hmac.compare_digest(provided_digest, expected_digest)


async def require_api_key(
    settings: Annotated[Settings, Depends(get_settings)],
    x_api_key: Annotated[str | None, Security(api_key_header)] = None,
) -> None:
    """Проверить заголовок `X-API-Key` с constant-time сравнением.

    Args:
        settings: Настройки приложения.
        x_api_key: Значение заголовка X-API-Key.

    Raises:
        InvalidAPIKeyError: при отсутствии или неверном ключе.
    """
    expected = settings.api_key.get_secret_value()
    if x_api_key is None or not _api_keys_match(x_api_key, expected):
        raise InvalidAPIKeyError()


def require_idempotency_key(
    idempotency_key: Annotated[str | None, _IDEMPOTENCY_KEY_HEADER] = None,
) -> str:
    """Извлечь и провалидировать заголовок Idempotency-Key.

    Args:
        idempotency_key: Сырое значение заголовка.

    Returns:
        Валидный ключ.

    Raises:
        InvalidRequestHeaderError: при отсутствии или невалидном ключе.
    """
    if idempotency_key is None:
        raise InvalidRequestHeaderError(
            header_name=IDEMPOTENCY_KEY_HEADER_NAME,
            error_type="missing",
            message=IDEMPOTENCY_KEY_MISSING,
        )
    try:
        return validate_idempotency_key_value(idempotency_key)
    except ValueError as exc:
        raise InvalidRequestHeaderError(
            header_name=IDEMPOTENCY_KEY_HEADER_NAME,
            error_type="value_error",
            message=str(exc),
        ) from exc


async def get_db_session(request: Request) -> AsyncIterator[AsyncSession]:
    """Выдать AsyncSession на время HTTP-запроса.

    Args:
        request: Текущий FastAPI Request (берёт session_factory из app.state).

    Yields:
        Сессия SQLAlchemy.

    Raises:
        RuntimeError: если приложение запущено без инициализации БД.
    """
    session_factory = getattr(request.app.state, "session_factory", None)
    if session_factory is None:
        msg = "Database session factory is not initialized"
        raise RuntimeError(msg)
    factory: async_sessionmaker[AsyncSession] = session_factory
    async with factory() as session:
        yield session
