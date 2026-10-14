from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from payrelay.api.errors import InvalidAPIKeyError, InvalidRequestHeaderError
from payrelay.api.messages import (
    AMOUNT_INVALID,
    API_KEY_INVALID,
    CURRENCY_ALLOWED,
    FALLBACK_VALIDATION,
    IDEMPOTENCY_CONFLICT,
    INTERNAL_SERVER_ERROR,
    PAYMENT_NOT_FOUND,
    VALIDATION_TYPE_MESSAGES,
)
from payrelay.domain.errors import IdempotencyConflictError, PaymentNotFoundError

logger = logging.getLogger(__name__)


def _strip_pydantic_value_error_prefix(msg: str) -> str:
    """Убрать стандартный префикс Pydantic у value_error.

    Args:
        msg: Исходное сообщение ошибки.

    Returns:
        Сообщение без префикса ``Value error, ``.
    """
    prefixes = ("Value error, ", "Value error,")
    for prefix in prefixes:
        if msg.startswith(prefix):
            return msg[len(prefix) :].lstrip()
    return msg


def _localize_validation_item(error: dict[str, Any]) -> dict[str, Any]:
    """Сформировать элемент 422 detail с русским msg.

    Args:
        error: Элемент из ``RequestValidationError.errors()``.

    Returns:
        Словарь с ``loc``, ``msg``, ``type`` без сырого ``input``.
    """
    err_type = str(error.get("type") or "value_error")
    loc = list(error.get("loc") or ())
    loc_names = {str(part) for part in loc}

    if err_type == "literal_error" and "currency" in loc_names:
        msg = CURRENCY_ALLOWED
    elif err_type in VALIDATION_TYPE_MESSAGES:
        msg = VALIDATION_TYPE_MESSAGES[err_type]
    elif err_type == "value_error":
        raw = _strip_pydantic_value_error_prefix(str(error.get("msg") or ""))
        msg = raw or FALLBACK_VALIDATION
    elif err_type.startswith("decimal") or (
        "amount" in loc_names and err_type.endswith("_parsing")
    ):
        msg = AMOUNT_INVALID
    else:
        msg = FALLBACK_VALIDATION

    return {
        "loc": loc,
        "msg": msg,
        "type": err_type,
    }


async def request_validation_exception_handler(
    _request: Request,
    exc: RequestValidationError,
) -> JSONResponse:
    """Вернуть 422 с локализованными сообщениями валидации.

    Args:
        _request: Текущий HTTP-запрос (не используется).
        exc: Ошибка валидации FastAPI/Pydantic.

    Returns:
        JSON ``{"detail": [...]}`` со статусом 422.
    """
    detail = [_localize_validation_item(dict(item)) for item in exc.errors()]
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content={"detail": detail},
    )


async def invalid_api_key_exception_handler(
    _request: Request,
    _exc: InvalidAPIKeyError,
) -> JSONResponse:
    """Вернуть 401 при неверном или отсутствующем API-ключе.

    Args:
        _request: Текущий HTTP-запрос (не используется).
        _exc: Ошибка авторизации по API-ключу.

    Returns:
        JSON 401 с русским ``detail``.
    """
    return JSONResponse(
        status_code=status.HTTP_401_UNAUTHORIZED,
        content={"detail": API_KEY_INVALID},
    )


async def invalid_request_header_exception_handler(
    _request: Request,
    exc: InvalidRequestHeaderError,
) -> JSONResponse:
    """Вернуть 422 для ошибок обязательных заголовков.

    Args:
        _request: Текущий HTTP-запрос (не используется).
        exc: Ошибка валидации заголовка.

    Returns:
        JSON 422 со списком ``detail`` в формате FastAPI.
    """
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content={
            "detail": [
                {
                    "loc": ["header", exc.header_name],
                    "msg": exc.message,
                    "type": exc.error_type,
                }
            ]
        },
    )


async def payment_not_found_exception_handler(
    _request: Request,
    _exc: PaymentNotFoundError,
) -> JSONResponse:
    """Вернуть 404, если платёж не найден.

    Args:
        _request: Текущий HTTP-запрос (не используется).
        _exc: Доменное исключение отсутствия платежа.

    Returns:
        JSON 404 с русским ``detail``.
    """
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"detail": PAYMENT_NOT_FOUND},
    )


async def idempotency_conflict_exception_handler(
    _request: Request,
    _exc: IdempotencyConflictError,
) -> JSONResponse:
    """Вернуть 409 при конфликте Idempotency-Key.

    Args:
        _request: Текущий HTTP-запрос (не используется).
        _exc: Доменное исключение конфликта идемпотентности.

    Returns:
        JSON 409 с русским ``detail``.
    """
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={"detail": IDEMPOTENCY_CONFLICT},
    )


async def http_exception_handler(
    _request: Request,
    exc: StarletteHTTPException,
) -> JSONResponse:
    """Проксировать HTTPException без раскрытия инфраструктуры.

    Args:
        _request: Текущий HTTP-запрос (не используется).
        exc: HTTP-исключение.

    Returns:
        JSON с полем ``detail`` и исходными заголовками (например ``Allow``).
    """
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
        headers=exc.headers,
    )


async def unhandled_exception_handler(
    request: Request,
    exc: Exception,
) -> JSONResponse:
    """Скрыть детали непредвиденных ошибок от клиента.

    Args:
        request: Текущий HTTP-запрос.
        exc: Неперехваченное исключение.

    Returns:
        JSON 500 с безопасным русским ``detail``.
    """
    logger.exception(
        "Unhandled API error path=%s",
        request.url.path,
        exc_info=exc,
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": INTERNAL_SERVER_ERROR},
    )


async def _as_request_validation_handler(
    request: Request,
    exc: Exception,
) -> JSONResponse:
    """Адаптер сигнатуры Starlette для RequestValidationError.

    Args:
        request: HTTP-запрос.
        exc: Ожидается RequestValidationError.

    Returns:
        Локализованный JSON 422.
    """
    if not isinstance(exc, RequestValidationError):
        raise TypeError("expected RequestValidationError") from exc
    return await request_validation_exception_handler(request, exc)


async def _as_http_exception_handler(
    request: Request,
    exc: Exception,
) -> JSONResponse:
    """Адаптер сигнатуры Starlette для HTTPException.

    Args:
        request: HTTP-запрос.
        exc: Ожидается StarletteHTTPException.

    Returns:
        JSON с исходным detail и headers.
    """
    if not isinstance(exc, StarletteHTTPException):
        raise TypeError("expected HTTPException") from exc
    return await http_exception_handler(request, exc)


async def _as_invalid_api_key_handler(
    request: Request,
    exc: Exception,
) -> JSONResponse:
    """Адаптер сигнатуры Starlette для InvalidAPIKeyError.

    Args:
        request: HTTP-запрос.
        exc: Ожидается InvalidAPIKeyError.

    Returns:
        JSON 401.
    """
    if not isinstance(exc, InvalidAPIKeyError):
        raise TypeError("expected InvalidAPIKeyError") from exc
    return await invalid_api_key_exception_handler(request, exc)


async def _as_invalid_request_header_handler(
    request: Request,
    exc: Exception,
) -> JSONResponse:
    """Адаптер сигнатуры Starlette для InvalidRequestHeaderError.

    Args:
        request: HTTP-запрос.
        exc: Ожидается InvalidRequestHeaderError.

    Returns:
        JSON 422.
    """
    if not isinstance(exc, InvalidRequestHeaderError):
        raise TypeError("expected InvalidRequestHeaderError") from exc
    return await invalid_request_header_exception_handler(request, exc)


async def _as_payment_not_found_handler(
    request: Request,
    exc: Exception,
) -> JSONResponse:
    """Адаптер сигнатуры Starlette для PaymentNotFoundError.

    Args:
        request: HTTP-запрос.
        exc: Ожидается PaymentNotFoundError.

    Returns:
        JSON 404.
    """
    if not isinstance(exc, PaymentNotFoundError):
        raise TypeError("expected PaymentNotFoundError") from exc
    return await payment_not_found_exception_handler(request, exc)


async def _as_idempotency_conflict_handler(
    request: Request,
    exc: Exception,
) -> JSONResponse:
    """Адаптер сигнатуры Starlette для IdempotencyConflictError.

    Args:
        request: HTTP-запрос.
        exc: Ожидается IdempotencyConflictError.

    Returns:
        JSON 409.
    """
    if not isinstance(exc, IdempotencyConflictError):
        raise TypeError("expected IdempotencyConflictError") from exc
    return await idempotency_conflict_exception_handler(request, exc)


def register_exception_handlers(app: FastAPI) -> None:
    """Зарегистрировать централизованные HTTP exception handlers.

    Args:
        app: Экземпляр FastAPI.
    """
    app.add_exception_handler(InvalidAPIKeyError, _as_invalid_api_key_handler)
    app.add_exception_handler(
        InvalidRequestHeaderError,
        _as_invalid_request_header_handler,
    )
    app.add_exception_handler(PaymentNotFoundError, _as_payment_not_found_handler)
    app.add_exception_handler(
        IdempotencyConflictError,
        _as_idempotency_conflict_handler,
    )
    app.add_exception_handler(RequestValidationError, _as_request_validation_handler)
    app.add_exception_handler(StarletteHTTPException, _as_http_exception_handler)
    app.add_exception_handler(Exception, unhandled_exception_handler)
