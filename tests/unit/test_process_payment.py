"""U10/U11/lease/HTTP: ProcessPaymentService с фейками без реальной сети."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock

import httpx
import pytest
from pydantic import SecretStr

from payrelay.application.process_payment import (
    ProcessOutcome,
    ProcessPaymentService,
    _LeaseAcquireKind,
    _LeaseAcquireResult,
)
from payrelay.core.config import Settings
from payrelay.domain.enums import PaymentStatus, WebhookDeliveryStatus
from payrelay.infrastructure.db.models import PaymentModel
from payrelay.infrastructure.gateway.simulator import GatewaySimulator
from payrelay.infrastructure.webhooks.http_client import WebhookHttpClient

pytestmark = pytest.mark.unit


def _settings() -> Settings:
    return Settings(
        database_url=SecretStr(
            "postgresql+asyncpg://payrelay:payrelay@localhost:5432/payrelay_test"
        ),
        rabbitmq_url=SecretStr("amqp://payrelay:payrelay@localhost:5672/"),
        api_key=SecretStr("test-api-key-not-for-production"),
        app_env="test",
        webhook_allow_private_urls=True,
        processing_lease_seconds=60.0,
        gateway_min_delay_seconds=2.0,
        gateway_max_delay_seconds=5.0,
        webhook_timeout_seconds=5.0,
    )


def _payment(
    *,
    payment_id: uuid.UUID | None = None,
    status: str = PaymentStatus.PENDING.value,
    webhook_status: str = WebhookDeliveryStatus.PENDING.value,
    lease_until: datetime | None = None,
    webhook_attempts: int = 0,
) -> PaymentModel:
    return PaymentModel(
        id=payment_id or uuid.uuid4(),
        amount=Decimal("1500.50"),
        currency="RUB",
        description="test",
        metadata_={},
        status=status,
        idempotency_key=str(uuid.uuid4()),
        request_hash="a" * 64,
        webhook_url="http://127.0.0.1/hook",
        webhook_delivery_status=webhook_status,
        webhook_attempts=webhook_attempts,
        webhook_delivered_at=None,
        processing_lease_until=lease_until,
        created_at=datetime.now(UTC),
        processed_at=(
            datetime.now(UTC) if status != PaymentStatus.PENDING.value else None
        ),
    )


class _FakeRepo:
    def __init__(self, payment: PaymentModel | None) -> None:
        self.payment = payment

    async def get_by_id(self, payment_id: uuid.UUID) -> PaymentModel | None:
        if self.payment is None or self.payment.id != payment_id:
            return None
        return self.payment

    async def claim_pending_for_processing(
        self,
        *,
        payment_id: uuid.UUID,
        lease_seconds: float,
    ) -> datetime | None:
        assert self.payment is not None
        if self.payment.status != PaymentStatus.PENDING.value:
            return None
        now = datetime.now(UTC)
        if (
            self.payment.processing_lease_until is not None
            and self.payment.processing_lease_until > now
        ):
            return None
        token = now + timedelta(seconds=lease_seconds)
        self.payment.processing_lease_until = token
        return token

    async def claim_terminal_for_webhook(
        self,
        *,
        payment_id: uuid.UUID,
        lease_seconds: float,
    ) -> datetime | None:
        assert self.payment is not None
        if self.payment.status == PaymentStatus.PENDING.value:
            return None
        if self.payment.webhook_delivery_status != WebhookDeliveryStatus.PENDING.value:
            return None
        now = datetime.now(UTC)
        if (
            self.payment.processing_lease_until is not None
            and self.payment.processing_lease_until > now
        ):
            return None
        token = now + timedelta(seconds=lease_seconds)
        self.payment.processing_lease_until = token
        return token

    async def commit_terminal_status(
        self,
        *,
        payment_id: uuid.UUID,
        lease_token: datetime,
        status: PaymentStatus,
    ) -> bool:
        assert self.payment is not None
        if self.payment.status != PaymentStatus.PENDING.value:
            return False
        if self.payment.processing_lease_until != lease_token:
            return False
        self.payment.status = status.value
        self.payment.processed_at = datetime.now(UTC)
        return True

    async def mark_webhook_attempt_started(
        self,
        *,
        payment_id: uuid.UUID,
        lease_token: datetime,
        attempt: int = 1,
    ) -> bool:
        assert self.payment is not None
        if self.payment.processing_lease_until != lease_token:
            return False
        if self.payment.webhook_attempts < attempt:
            self.payment.webhook_attempts = attempt
        return True

    async def mark_webhook_delivered(
        self,
        *,
        payment_id: uuid.UUID,
        lease_token: datetime,
    ) -> bool:
        assert self.payment is not None
        if self.payment.processing_lease_until != lease_token:
            return False
        self.payment.webhook_delivery_status = WebhookDeliveryStatus.DELIVERED.value
        self.payment.webhook_delivered_at = datetime.now(UTC)
        self.payment.processing_lease_until = None
        return True

    async def release_processing_lease(
        self,
        *,
        payment_id: uuid.UUID,
        lease_token: datetime,
    ) -> bool:
        assert self.payment is not None
        if self.payment.processing_lease_until != lease_token:
            return False
        self.payment.processing_lease_until = None
        return True


class _Harness(ProcessPaymentService):
    """Сервис с in-memory repo вместо PostgreSQL."""

    def __init__(
        self,
        *,
        repo: _FakeRepo,
        gateway: GatewaySimulator,
        webhook_client: WebhookHttpClient,
        settings: Settings,
        sleep: AsyncMock,
    ) -> None:
        super().__init__(
            session_factory=AsyncMock(),
            gateway=gateway,
            webhook_client=webhook_client,
            settings=settings,
            sleep=sleep,
        )
        self.repo = repo

    async def _load_payment(self, payment_id: uuid.UUID) -> PaymentModel | None:
        return await self.repo.get_by_id(payment_id)

    async def _acquire_lease(
        self,
        *,
        payment_id: uuid.UUID,
        stop: object = None,
    ) -> _LeaseAcquireResult:
        if self.repo.payment is None:
            return _LeaseAcquireResult(kind=_LeaseAcquireKind.NOT_FOUND)
        if (
            self.repo.payment.status != PaymentStatus.PENDING.value
            and self.repo.payment.webhook_delivery_status
            == WebhookDeliveryStatus.DELIVERED.value
        ):
            return _LeaseAcquireResult(kind=_LeaseAcquireKind.ALREADY_DELIVERED)
        if self.repo.payment.status == PaymentStatus.PENDING.value:
            token = await self.repo.claim_pending_for_processing(
                payment_id=payment_id,
                lease_seconds=60.0,
            )
        else:
            token = await self.repo.claim_terminal_for_webhook(
                payment_id=payment_id,
                lease_seconds=60.0,
            )
        if token is None:
            return _LeaseAcquireResult(kind=_LeaseAcquireKind.BUSY)
        return _LeaseAcquireResult(kind=_LeaseAcquireKind.CLAIMED, token=token)

    async def _commit_terminal(
        self,
        *,
        payment_id: uuid.UUID,
        lease_token: datetime,
        status: PaymentStatus,
    ) -> bool:
        return await self.repo.commit_terminal_status(
            payment_id=payment_id,
            lease_token=lease_token,
            status=status,
        )

    async def _mark_attempt_started(
        self,
        payment_id: uuid.UUID,
        lease_token: datetime,
    ) -> None:
        ok = await self.repo.mark_webhook_attempt_started(
            payment_id=payment_id,
            lease_token=lease_token,
            attempt=1,
        )
        if not ok:
            raise RuntimeError("webhook attempt CAS failed")

    async def _mark_delivered(
        self,
        payment_id: uuid.UUID,
        lease_token: datetime,
    ) -> bool:
        return await self.repo.mark_webhook_delivered(
            payment_id=payment_id,
            lease_token=lease_token,
        )

    async def _release_lease(
        self,
        payment_id: uuid.UUID,
        lease_token: datetime,
    ) -> None:
        await self.repo.release_processing_lease(
            payment_id=payment_id,
            lease_token=lease_token,
        )


def _build_service(
    payment: PaymentModel | None,
    *,
    gateway: GatewaySimulator | None = None,
    webhook_status_code: int = 200,
) -> tuple[_Harness, _FakeRepo, AsyncMock]:
    repo = _FakeRepo(payment)
    sleep = AsyncMock()
    transport = httpx.MockTransport(lambda _r: httpx.Response(webhook_status_code))
    webhook = WebhookHttpClient(
        timeout_seconds=1.0,
        allow_private_urls=True,
        transport=transport,
    )
    gw = gateway or GatewaySimulator(
        sleep=sleep,
        forced_outcome=PaymentStatus.SUCCEEDED,
        min_delay_seconds=2.0,
        max_delay_seconds=5.0,
    )
    service = _Harness(
        repo=repo,
        gateway=gw,
        webhook_client=webhook,
        settings=_settings(),
        sleep=sleep,
    )
    return service, repo, sleep


@pytest.mark.asyncio
async def test_u10_pending_to_succeeded_and_second_terminal_forbidden() -> None:
    """U10: pending→succeeded; повторный terminal не перезаписывает."""
    payment = _payment()
    service, repo, _sleep = _build_service(payment)
    event_id = uuid.uuid4()
    result = await service.process_payment_created(
        payment_id=payment.id,
        event_id=event_id,
    )
    assert result.outcome is ProcessOutcome.ACK
    assert repo.payment is not None
    assert repo.payment.status == PaymentStatus.SUCCEEDED.value
    assert repo.payment.webhook_delivery_status == WebhookDeliveryStatus.DELIVERED.value

    processed_at = repo.payment.processed_at
    gateway = GatewaySimulator(sleep=AsyncMock(), forced_outcome=PaymentStatus.FAILED)
    service2, repo2, _ = _build_service(repo.payment, gateway=gateway)
    result2 = await service2.process_payment_created(
        payment_id=payment.id,
        event_id=event_id,
    )
    assert result2.outcome is ProcessOutcome.ACK
    assert result2.detail == "already_delivered"
    assert repo2.payment is not None
    assert repo2.payment.status == PaymentStatus.SUCCEEDED.value
    assert repo2.payment.processed_at == processed_at


@pytest.mark.asyncio
async def test_u10_gateway_technical_exception_does_not_fail_payment() -> None:
    """U10: техническая ошибка шлюза не переводит платёж в business failed."""

    class _BoomGateway:
        async def charge(self, payment_id: uuid.UUID) -> PaymentStatus:
            raise RuntimeError("gateway down")

    payment = _payment()
    service, repo, _ = _build_service(payment)
    service.gateway = _BoomGateway()  # type: ignore[assignment]
    with pytest.raises(RuntimeError, match="gateway down"):
        await service.process_payment_created(
            payment_id=payment.id,
            event_id=uuid.uuid4(),
        )
    assert repo.payment is not None
    assert repo.payment.status == PaymentStatus.PENDING.value


@pytest.mark.asyncio
async def test_u11_terminal_skips_gateway_on_redelivery() -> None:
    """U11: terminal payment + повторный event не вызывает simulator повторно."""
    payment = _payment(
        status=PaymentStatus.SUCCEEDED.value,
        webhook_status=WebhookDeliveryStatus.PENDING.value,
    )
    sleep = AsyncMock()
    gateway = GatewaySimulator(sleep=sleep, forced_outcome=PaymentStatus.FAILED)
    service, repo, _ = _build_service(payment, gateway=gateway)
    result = await service.process_payment_created(
        payment_id=payment.id,
        event_id=uuid.uuid4(),
    )
    assert result.outcome is ProcessOutcome.ACK
    sleep.assert_not_awaited()
    assert repo.payment is not None
    assert repo.payment.status == PaymentStatus.SUCCEEDED.value
    assert repo.payment.webhook_delivery_status == WebhookDeliveryStatus.DELIVERED.value


@pytest.mark.asyncio
async def test_active_lease_returns_retry_later_without_ack_path() -> None:
    """Активный чужой lease → RETRY_LATER (не ACK с потерей сообщения)."""
    payment = _payment(lease_until=datetime.now(UTC) + timedelta(seconds=30))
    service, _repo, _ = _build_service(payment)
    result = await service.process_payment_created(
        payment_id=payment.id,
        event_id=uuid.uuid4(),
    )
    assert result.outcome is ProcessOutcome.RETRY_LATER


@pytest.mark.asyncio
async def test_http_error_keeps_terminal_and_pending_webhook_attempt_one() -> None:
    """HTTP ошибка: business terminal + webhook pending + attempts=1."""
    payment = _payment()
    service, repo, _ = _build_service(payment, webhook_status_code=500)
    result = await service.process_payment_created(
        payment_id=payment.id,
        event_id=uuid.uuid4(),
    )
    assert result.outcome is ProcessOutcome.ACK
    assert result.detail == "webhook_failed_deferred_stage_005"
    assert repo.payment is not None
    assert repo.payment.status == PaymentStatus.SUCCEEDED.value
    assert repo.payment.webhook_delivery_status == WebhookDeliveryStatus.PENDING.value
    assert repo.payment.webhook_attempts == 1
