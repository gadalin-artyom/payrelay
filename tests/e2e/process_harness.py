"""Orchestration реальных entrypoints для process-level E2E.

Запускает ``api``, ``outbox-publisher`` и ``consumer`` как отдельные
subprocess в том же контейнере/хосте, что и pytest. Не заменяет бизнес-логику
прямыми вызовами ``process_payment`` / ``outbox_dispatcher``.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

import httpx

RoleName = Literal["api", "consumer", "outbox-publisher"]

_ROLE_MODULE: dict[RoleName, str] = {
    "api": "payrelay.entrypoints.api",
    "consumer": "payrelay.entrypoints.consumer",
    "outbox-publisher": "payrelay.entrypoints.outbox_publisher",
}


@dataclass
class ProcessHarness:
    """Менеджер subprocess-ролей PayRelay для E2E.

    Attributes:
        base_env: Базовое окружение (test DSN/RabbitMQ/API key).
        api_base_url: Базовый URL API-процесса.
        api_key: Значение X-API-Key.
        _procs: Запущенные процессы по имени роли.
    """

    base_env: dict[str, str]
    api_base_url: str = "http://127.0.0.1:8000"
    api_key: str = "test-api-key-not-for-production"
    _procs: dict[RoleName, subprocess.Popen[bytes]] = field(default_factory=dict)

    def _role_env(self, overrides: Mapping[str, str] | None = None) -> dict[str, str]:
        """Собрать env для роли.

        Args:
            overrides: Дополнительные переменные.

        Returns:
            Полный словарь окружения.
        """
        env = dict(self.base_env)
        if overrides:
            env.update(overrides)
        return env

    def start_role(
        self,
        role: RoleName,
        *,
        overrides: Mapping[str, str] | None = None,
    ) -> None:
        """Запустить роль, если ещё не запущена.

        Args:
            role: Имя роли.
            overrides: Env-переопределения.

        Raises:
            RuntimeError: если процесс сразу завершился.
        """
        existing = self._procs.get(role)
        if existing is not None and existing.poll() is None:
            return
        module = _ROLE_MODULE[role]
        proc = subprocess.Popen(
            [sys.executable, "-m", module],
            env=self._role_env(overrides),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        self._procs[role] = proc
        time.sleep(0.3)
        if proc.poll() is not None:
            output = b""
            if proc.stderr is not None:
                output = proc.stderr.read()
                proc.stderr.close()
            msg = (
                f"Role {role!r} exited early with code {proc.returncode}: "
                f"{output[-2000:].decode('utf-8', errors='replace')}"
            )
            raise RuntimeError(msg)

    def stop_role(self, role: RoleName, *, timeout: float = 10.0) -> None:
        """Остановить роль graceful (SIGTERM → SIGKILL).

        Args:
            role: Имя роли.
            timeout: Секунды ожидания завершения.
        """
        proc = self._procs.pop(role, None)
        if proc is None:
            return
        try:
            if proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
                try:
                    proc.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5.0)
        finally:
            if proc.stderr is not None:
                try:
                    proc.stderr.close()
                except OSError:
                    pass

    def stop_all(self, *, timeout: float = 10.0) -> None:
        """Остановить все запущенные роли.

        Args:
            timeout: Таймаут на каждую роль.
        """
        for role in list(self._procs):
            self.stop_role(role, timeout=timeout)

    def start_all_roles(self) -> None:
        """Запустить api, outbox-publisher и consumer."""
        self.start_role("api")
        self.start_role("outbox-publisher")
        self.start_role("consumer")

    def wait_api_ready(self, *, timeout: float = 30.0) -> None:
        """Дождаться liveness API.

        Args:
            timeout: Максимальное ожидание.

        Raises:
            TimeoutError: если API не ответил вовремя.
        """
        deadline = time.monotonic() + timeout
        url = f"{self.api_base_url}/health/live"
        headers = {"X-API-Key": self.api_key}
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                response = httpx.get(url, headers=headers, timeout=1.0)
                if response.status_code == 200:
                    return
            except (httpx.HTTPError, OSError) as exc:
                last_error = exc
            api_proc = self._procs.get("api")
            if api_proc is not None and api_proc.poll() is not None:
                output = b""
                if api_proc.stdout is not None:
                    output = api_proc.stdout.read()
                msg = (
                    "API process died while waiting for readiness: "
                    f"{output[-2000:].decode('utf-8', errors='replace')}"
                )
                raise RuntimeError(msg)
            time.sleep(0.2)
        msg = f"API not ready within {timeout}s (last_error={last_error!r})"
        raise TimeoutError(msg)


def build_e2e_env(
    *,
    database_url: str,
    rabbitmq_url: str,
    api_key: str,
    extra: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Собрать окружение для subprocess-ролей E2E.

    Ускоряет gateway delay, сохраняет реальные webhook retry 2/4 с.
    Production SSRF-политику не ослабляет глобально: private URL только
    при ``WEBHOOK_ALLOW_PRIVATE_URLS=true`` в test env.

    Args:
        database_url: DSN test PostgreSQL.
        rabbitmq_url: AMQP URL test RabbitMQ.
        api_key: Тестовый API-ключ.
        extra: Дополнительные переменные.

    Returns:
        Полный env для дочерних процессов.
    """
    env = os.environ.copy()
    env.update(
        {
            "APP_ENV": "test",
            "LOG_LEVEL": "INFO",
            "LOG_JSON": "false",
            "ENABLE_DOCS": "false",
            "DATABASE_URL": database_url,
            "RABBITMQ_URL": rabbitmq_url,
            "API_KEY": api_key,
            "WEBHOOK_ALLOW_PRIVATE_URLS": "true",
            "WEBHOOK_TIMEOUT_SECONDS": "5",
            "WEBHOOK_CONNECT_TIMEOUT_SECONDS": "2",
            "WEBHOOK_MAX_ATTEMPTS": "3",
            "WEBHOOK_RETRY_DELAY_2_SECONDS": "2",
            "WEBHOOK_RETRY_DELAY_4_SECONDS": "4",
            "OUTBOX_POLL_INTERVAL_SECONDS": "0.1",
            "OUTBOX_BATCH_SIZE": "50",
            "OUTBOX_LEASE_SECONDS": "30",
            "OUTBOX_PUBLISH_TIMEOUT_SECONDS": "5",
            "OUTBOX_RETRY_MAX_DELAY_SECONDS": "30",
            "OUTBOX_SHUTDOWN_GRACE_SECONDS": "10",
            "OUTBOX_BROKER_RECONNECT_DELAY_SECONDS": "1",
            "OUTBOX_DB_RETRY_DELAY_SECONDS": "1",
            # Ускорение E2E: реальный sleep gateway, но без 2–5 с.
            "GATEWAY_MIN_DELAY_SECONDS": "0.05",
            "GATEWAY_MAX_DELAY_SECONDS": "0.05",
            "PROCESSING_LEASE_SECONDS": "20",
            "PROCESSING_PREFETCH_COUNT": "1",
            "CONSUMER_SHUTDOWN_GRACE_SECONDS": "10",
        }
    )
    if extra:
        env.update(extra)
    return env
