"""Small retry + circuit-breaker primitives for scraper adapters.

Designed so source failures do not silently become empty lead batches.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Callable, TypeVar

T = TypeVar("T")


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(RuntimeError):
    pass


@dataclass
class RetryPolicy:
    max_attempts: int = 3
    base_delay_seconds: float = 0.25
    max_delay_seconds: float = 2.0


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 3, cooldown_seconds: float = 30.0,
                 *, clock: Callable[[], float] | None = None):
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self.clock = clock or time.monotonic
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.opened_at: float | None = None

    def _refresh(self) -> None:
        if self.state == CircuitState.OPEN and self.opened_at is not None:
            if self.clock() - self.opened_at >= self.cooldown_seconds:
                self.state = CircuitState.HALF_OPEN

    def allow(self) -> None:
        self._refresh()
        if self.state == CircuitState.OPEN:
            raise CircuitOpenError("circuit_open")

    def record_success(self) -> None:
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.opened_at = None

    def record_failure(self) -> None:
        self.failure_count += 1
        if self.state == CircuitState.HALF_OPEN or self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN
            self.opened_at = self.clock()


def retry_call(fn: Callable[[], T], *, policy: RetryPolicy | None = None,
               is_transient: Callable[[BaseException], bool] | None = None,
               sleep: Callable[[float], None] | None = None,
               breaker: CircuitBreaker | None = None) -> tuple[T, dict]:
    policy = policy or RetryPolicy()
    is_transient = is_transient or (lambda exc: isinstance(exc, (TimeoutError, ConnectionError)))
    sleep = sleep or time.sleep
    attempts = 0
    errors: list[str] = []

    while attempts < policy.max_attempts:
        if breaker:
            breaker.allow()
        attempts += 1
        try:
            value = fn()
            if breaker:
                breaker.record_success()
            return value, {"attempts": attempts, "errors": errors, "recovered": attempts > 1}
        except BaseException as exc:
            errors.append(f"{type(exc).__name__}:{exc}")
            transient = is_transient(exc)
            if breaker:
                breaker.record_failure()
            if not transient or attempts >= policy.max_attempts:
                raise
            delay = min(policy.max_delay_seconds,
                        policy.base_delay_seconds * (2 ** (attempts - 1)))
            sleep(delay)

    raise RuntimeError("retry_exhausted")
