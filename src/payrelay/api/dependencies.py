"""Зависимости FastAPI: аутентификация и доступ к настройкам."""

from __future__ import annotations

import hashlib
import hmac
from typing import Annotated

from fastapi import Depends, Header, HTTPException, status

from payrelay.core.config import Settings, get_settings


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
    x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
) -> None:
    """Проверить заголовок `X-API-Key` с constant-time сравнением.

    Args:
        settings: Настройки приложения.
        x_api_key: Значение заголовка X-API-Key.

    Raises:
        HTTPException: 401 при отсутствии или неверном ключе.
    """
    expected = settings.api_key.get_secret_value()
    if x_api_key is None or not _api_keys_match(x_api_key, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
        )
