"""Минимальный test-only HTTP webhook receiver для process-level E2E.

Не является production-компонентом: только журнал безопасных метаданных
и управляемые ответы (200 / 500).
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any

import uvicorn
from fastapi import FastAPI, Request, Response


@dataclass
class WebhookCallRecord:
    """Безопасная запись об одном HTTP-вызове webhook.

    Attributes:
        payment_id: Идентификатор платежа из тела (если есть).
        webhook_event_id: Значение заголовка X-Webhook-Event-ID.
        status_code: Отправленный HTTP status.
        received_at: Монотонное время приёма.
    """

    payment_id: str | None
    webhook_event_id: str | None
    status_code: int
    received_at: float


@dataclass
class WebhookReceiver:
    """Управляемый HTTP-приёмник webhook для E2E.

    Attributes:
        host: Адрес bind.
        port: Порт bind.
        calls: Журнал вызовов (без секретов и полных metadata).
    """

    host: str = "127.0.0.1"
    port: int = 8199
    calls: list[WebhookCallRecord] = field(default_factory=list)
    _status_sequence: list[int] = field(default_factory=lambda: [200])
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _server: uvicorn.Server | None = field(default=None, init=False, repr=False)
    _thread: threading.Thread | None = field(default=None, init=False, repr=False)

    @property
    def base_url(self) -> str:
        """Базовый URL receiver.

        Returns:
            URL вида ``http://host:port``.
        """
        return f"http://{self.host}:{self.port}"

    @property
    def hook_url(self) -> str:
        """URL endpoint webhook.

        Returns:
            Полный URL ``/hook``.
        """
        return f"{self.base_url}/hook"

    def set_status_sequence(self, statuses: list[int]) -> None:
        """Задать последовательность HTTP-ответов.

        После исчерпания списка повторяется последний код.

        Args:
            statuses: Коды ответа (например ``[500, 500, 200]``).
        """
        if not statuses:
            msg = "status sequence must not be empty"
            raise ValueError(msg)
        with self._lock:
            self._status_sequence = list(statuses)
            self.calls.clear()

    def reset_calls(self) -> None:
        """Очистить журнал вызовов."""
        with self._lock:
            self.calls.clear()

    def _next_status(self) -> int:
        """Выбрать следующий status code.

        Returns:
            HTTP status для текущего запроса.
        """
        with self._lock:
            if len(self._status_sequence) == 1:
                return self._status_sequence[0]
            return self._status_sequence.pop(0)

    def _record(
        self,
        *,
        payment_id: str | None,
        webhook_event_id: str | None,
        status_code: int,
    ) -> None:
        """Добавить запись в журнал.

        Args:
            payment_id: ID платежа.
            webhook_event_id: Стабильный webhook event id.
            status_code: Отправленный статус.
        """
        with self._lock:
            self.calls.append(
                WebhookCallRecord(
                    payment_id=payment_id,
                    webhook_event_id=webhook_event_id,
                    status_code=status_code,
                    received_at=time.monotonic(),
                )
            )

    def snapshot_calls(self) -> list[WebhookCallRecord]:
        """Снимок журнала вызовов.

        Returns:
            Копия списка записей.
        """
        with self._lock:
            return list(self.calls)

    def _build_app(self) -> FastAPI:
        """Собрать FastAPI-приложение receiver.

        Returns:
            Приложение с ``GET /health`` и ``POST /hook``.
        """
        app = FastAPI(title="PayRelay E2E Webhook Receiver", docs_url=None)

        @app.get("/health")
        async def health() -> dict[str, str]:
            """Liveness receiver.

            Returns:
                Простой статус.
            """
            return {"status": "ok"}

        @app.post("/hook")
        async def hook(request: Request) -> Response:
            """Принять webhook и вернуть управляемый статус.

            Args:
                request: Входящий HTTP-запрос.

            Returns:
                Ответ с очередным status code.
            """
            payload: dict[str, Any]
            try:
                body = await request.json()
                payload = body if isinstance(body, dict) else {}
            except Exception:
                payload = {}
            payment_id = payload.get("payment_id")
            if payment_id is not None:
                payment_id = str(payment_id)
            event_id = request.headers.get("X-Webhook-Event-ID")
            status = self._next_status()
            self._record(
                payment_id=payment_id,
                webhook_event_id=event_id,
                status_code=status,
            )
            return Response(status_code=status, content=b"")

        return app

    def start(self) -> None:
        """Запустить uvicorn в фоновом потоке."""
        if self._thread is not None and self._thread.is_alive():
            return
        config = uvicorn.Config(
            self._build_app(),
            host=self.host,
            port=self.port,
            log_level="warning",
            access_log=False,
        )
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(
            target=self._server.run,
            name="e2e-webhook-receiver",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Остановить uvicorn и дождаться завершения потока."""
        if self._server is not None:
            self._server.should_exit = True
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None
        self._server = None
