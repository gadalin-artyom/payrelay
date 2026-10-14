"""Асинхронный HTTP-клиент webhook с SSRF-фильтрацией и стабильным event id."""

from __future__ import annotations

import ipaddress
import logging
import socket
import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)

# Неизменный namespace проекта для UUIDv5 webhook_event_id.
WEBHOOK_EVENT_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
WEBHOOK_EVENT_TYPE = "payment.processed"
_AMOUNT_QUANT = Decimal("0.01")

_BLOCKED_NETWORKS = (
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("100.64.0.0/10"),
    ipaddress.ip_network("::/128"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
    ipaddress.ip_network("ff00::/8"),
)


class WebhookUrlBlockedError(ValueError):
    """URL webhook отклонён политикой SSRF."""


@dataclass(frozen=True, slots=True)
class WebhookPayload:
    """Тело POST webhook мерчанту."""

    webhook_event_id: uuid.UUID
    event_type: str
    payment_id: uuid.UUID
    status: str
    amount: Decimal
    currency: str
    processed_at: datetime

    def to_json_dict(self) -> dict[str, str]:
        """Сериализовать payload в JSON-совместимый словарь.

        Returns:
            Словарь с amount в два десятичных знака и RFC3339 ``processed_at``.
        """
        return {
            "webhook_event_id": str(self.webhook_event_id),
            "event_type": self.event_type,
            "payment_id": str(self.payment_id),
            "status": self.status,
            "amount": format(self.amount.quantize(_AMOUNT_QUANT), "f"),
            "currency": self.currency,
            "processed_at": _to_rfc3339_z(self.processed_at),
        }


@dataclass(frozen=True, slots=True)
class WebhookDeliveryResult:
    """Исход одной HTTP-попытки доставки."""

    ok: bool
    status_code: int | None
    error_class: str | None


def build_webhook_event_id(payment_id: uuid.UUID) -> uuid.UUID:
    """Построить стабильный ``webhook_event_id`` (UUIDv5 от payment_id).

    Args:
        payment_id: UUID платежа.

    Returns:
        Неизменный UUID уведомления для всех попыток/повторов.
    """
    return uuid.uuid5(WEBHOOK_EVENT_NAMESPACE, f"payrelay:webhook:{payment_id}")


def build_webhook_payload(
    *,
    payment_id: uuid.UUID,
    status: str,
    amount: Decimal,
    currency: str,
    processed_at: datetime,
) -> WebhookPayload:
    """Собрать контрактный payload webhook.

    Args:
        payment_id: UUID платежа.
        status: Бизнес-статус ``succeeded``/``failed``.
        amount: Сумма.
        currency: Валюта.
        processed_at: Время терминального статуса.

    Returns:
        Готовый ``WebhookPayload``.
    """
    return WebhookPayload(
        webhook_event_id=build_webhook_event_id(payment_id),
        event_type=WEBHOOK_EVENT_TYPE,
        payment_id=payment_id,
        status=status,
        amount=amount,
        currency=currency,
        processed_at=processed_at,
    )


def _to_rfc3339_z(value: datetime) -> str:
    """Форматировать datetime в RFC3339 UTC с суффиксом Z.

    Args:
        value: Момент времени.

    Returns:
        Строка вида ``2026-10-14T10:00:03Z``.
    """
    from datetime import UTC

    if value.tzinfo is None:
        aware = value.replace(tzinfo=UTC)
    else:
        aware = value.astimezone(UTC)
    return aware.strftime("%Y-%m-%dT%H:%M:%SZ")


def _is_blocked_ip(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """Проверить, что IP относится к запрещённым диапазонам.

    Args:
        ip: Адрес.

    Returns:
        ``True``, если адрес запрещён для production egress.
    """
    if ip.is_multicast or ip.is_unspecified or ip.is_loopback:
        return True
    if ip.is_private or ip.is_link_local or ip.is_reserved:
        return True
    for network in _BLOCKED_NETWORKS:
        if ip in network:
            return True
    return False


def validate_webhook_url_for_delivery(
    url: str,
    *,
    allow_private: bool,
) -> tuple[str, str, int | None]:
    """Статическая проверка URL до DNS/connect.

    Args:
        url: Целевой URL.
        allow_private: Разрешить private/loopback (только test/dev).

    Returns:
        Кортеж ``(hostname, scheme, port)``.

    Raises:
        WebhookUrlBlockedError: при нарушении политики.
    """
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        msg = "webhook URL scheme must be http or https"
        raise WebhookUrlBlockedError(msg)
    if parsed.username is not None or parsed.password is not None:
        msg = "webhook URL must not contain userinfo"
        raise WebhookUrlBlockedError(msg)
    hostname = parsed.hostname
    if not hostname:
        msg = "webhook URL must include a host"
        raise WebhookUrlBlockedError(msg)
    blocked_hosts = {"localhost", "metadata.google.internal"}
    if not allow_private and hostname.lower() in blocked_hosts:
        msg = "webhook host is forbidden"
        raise WebhookUrlBlockedError(msg)
    # Литеральный IP в URL.
    try:
        literal_ip = ipaddress.ip_address(hostname)
    except ValueError:
        literal_ip = None
    if literal_ip is not None and not allow_private and _is_blocked_ip(literal_ip):
        msg = "webhook literal IP is forbidden"
        raise WebhookUrlBlockedError(msg)
    return hostname, parsed.scheme, parsed.port


def resolve_and_validate_host(
    hostname: str,
    *,
    allow_private: bool,
) -> list[str]:
    """Разрешить DNS и проверить все A/AAAA записи.

    Args:
        hostname: Имя хоста.
        allow_private: Разрешить private адреса.

    Returns:
        Список разрешённых IP (строки).

    Raises:
        WebhookUrlBlockedError: если DNS пуст или все/любые адреса запрещены.
    """
    try:
        infos = socket.getaddrinfo(hostname, None, type=socket.SOCK_STREAM)
    except OSError as exc:
        msg = "webhook DNS resolution failed"
        raise WebhookUrlBlockedError(msg) from exc
    addresses: list[str] = []
    for info in infos:
        sockaddr = info[4]
        ip_str = str(sockaddr[0])
        try:
            ip = ipaddress.ip_address(ip_str)
        except ValueError:
            continue
        if not allow_private and _is_blocked_ip(ip):
            msg = "webhook DNS resolved to forbidden address"
            raise WebhookUrlBlockedError(msg)
        if ip_str not in addresses:
            addresses.append(ip_str)
    if not addresses:
        msg = "webhook DNS returned no usable addresses"
        raise WebhookUrlBlockedError(msg)
    return addresses


class _SsrfGuardTransport(httpx.AsyncBaseTransport):
    """Transport, валидирующий URL и DNS перед делегированием connect.

    DNS precheck + повторная проверка непосредственно перед запросом снижают
    риск rebinding, но не заменяют сетевой egress-фильтр. При невозможности
    гарантировать pin IP на уровне TLS SNI/HTTP2 политика fail-closed для
    запрещённых адресов; private URL допускаются только при явном флаге.
    """

    def __init__(
        self,
        *,
        allow_private: bool,
        inner: httpx.AsyncBaseTransport,
    ) -> None:
        """Инициализировать обёртку.

        Args:
            allow_private: Разрешить private/loopback цели.
            inner: Вложенный transport (обычно ``AsyncHTTPTransport``).
        """
        self._allow_private = allow_private
        self._inner = inner

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        """Проверить цель и выполнить запрос без follow redirects.

        Args:
            request: Исходящий HTTP-запрос.

        Returns:
            Ответ httpx.

        Raises:
            WebhookUrlBlockedError: при SSRF-нарушении.
        """
        url = str(request.url)
        hostname, _scheme, _port = validate_webhook_url_for_delivery(
            url,
            allow_private=self._allow_private,
        )
        resolve_and_validate_host(hostname, allow_private=self._allow_private)
        return await self._inner.handle_async_request(request)

    async def aclose(self) -> None:
        """Закрыть вложенный transport."""
        await self._inner.aclose()


class WebhookHttpClient:
    """Клиент доставки webhook с таймаутами и SSRF-guard."""

    def __init__(
        self,
        *,
        timeout_seconds: float,
        allow_private_urls: bool,
        connect_timeout_seconds: float | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        """Инициализировать клиент.

        Args:
            timeout_seconds: Read/write/pool timeout HTTP.
            allow_private_urls: Dev/test флаг private URL.
            connect_timeout_seconds: Таймаут connect (по умолчанию = timeout).
            transport: Опциональный transport (тесты / кастомный egress).
            client: Готовый ``AsyncClient`` (владение внешнее, ``aclose`` не зовём).
        """
        self._timeout_seconds = timeout_seconds
        self._allow_private_urls = allow_private_urls
        self._owns_client = client is None
        connect_timeout = (
            connect_timeout_seconds
            if connect_timeout_seconds is not None
            else timeout_seconds
        )
        if client is not None:
            self._client = client
        else:
            inner = transport or httpx.AsyncHTTPTransport(
                retries=0,
                limits=httpx.Limits(
                    max_connections=20,
                    max_keepalive_connections=10,
                ),
            )
            guard = _SsrfGuardTransport(
                allow_private=allow_private_urls,
                inner=inner,
            )
            timeout = httpx.Timeout(
                connect=connect_timeout,
                read=timeout_seconds,
                write=timeout_seconds,
                pool=timeout_seconds,
            )
            self._client = httpx.AsyncClient(
                transport=guard,
                timeout=timeout,
                follow_redirects=False,
                trust_env=False,
                max_redirects=0,
            )

    async def aclose(self) -> None:
        """Закрыть владеемый HTTP-клиент."""
        if self._owns_client:
            await self._client.aclose()

    async def deliver(
        self,
        *,
        url: str,
        payload: WebhookPayload,
    ) -> WebhookDeliveryResult:
        """Отправить POST webhook.

        Args:
            url: URL мерчанта.
            payload: Контрактное тело.

        Returns:
            Результат попытки (успех = любой HTTP 2xx).
        """
        body = payload.to_json_dict()
        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Event-ID": str(payload.webhook_event_id),
        }
        try:
            validate_webhook_url_for_delivery(
                url,
                allow_private=self._allow_private_urls,
            )
            response = await self._client.post(url, json=body, headers=headers)
        except WebhookUrlBlockedError as exc:
            logger.warning(
                "Webhook URL blocked",
                extra={
                    "payment_id": str(payload.payment_id),
                    "webhook_event_id": str(payload.webhook_event_id),
                    "error_class": type(exc).__name__,
                },
            )
            return WebhookDeliveryResult(
                ok=False,
                status_code=None,
                error_class=type(exc).__name__,
            )
        except httpx.HTTPError as exc:
            logger.warning(
                "Webhook HTTP failure",
                extra={
                    "payment_id": str(payload.payment_id),
                    "webhook_event_id": str(payload.webhook_event_id),
                    "error_class": type(exc).__name__,
                },
            )
            return WebhookDeliveryResult(
                ok=False,
                status_code=None,
                error_class=type(exc).__name__,
            )

        # Не читаем полный body ответа — ограничиваем потребление.
        try:
            await response.aread()
        except httpx.HTTPError:
            pass
        ok = 200 <= response.status_code < 300
        if not ok:
            logger.warning(
                "Webhook non-2xx",
                extra={
                    "payment_id": str(payload.payment_id),
                    "webhook_event_id": str(payload.webhook_event_id),
                    "status_code": response.status_code,
                },
            )
        return WebhookDeliveryResult(
            ok=ok,
            status_code=response.status_code,
            error_class=None if ok else "Non2xxStatus",
        )

    # Для mypy/тестов: доступ к низкоуровневым проверкам без сети.
    @staticmethod
    def assert_url_allowed(url: str, *, allow_private: bool) -> None:
        """Публичная обёртка статической проверки URL.

        Args:
            url: URL.
            allow_private: Флаг private.

        Raises:
            WebhookUrlBlockedError: при запрете.
        """
        validate_webhook_url_for_delivery(url, allow_private=allow_private)
