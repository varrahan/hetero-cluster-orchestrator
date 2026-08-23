"""Application-owned periodic and USR1 checkpoint scheduling."""

from __future__ import annotations

import signal
import time
from collections.abc import Callable
from typing import Any


class CheckpointLifecycle:
    def __init__(self, interval_seconds: float = 300) -> None:
        if interval_seconds <= 0:
            raise ValueError("checkpoint interval must be positive")
        self.interval_seconds = interval_seconds
        self._requested = False
        self._next = time.monotonic() + interval_seconds
        self._previous_handler: Any = None

    def __enter__(self) -> "CheckpointLifecycle":
        self._previous_handler = signal.signal(signal.SIGUSR1, self._signal)
        return self

    def __exit__(self, *_: object) -> None:
        signal.signal(signal.SIGUSR1, self._previous_handler)

    def _signal(self, _signal: int, _frame: Any) -> None:
        self._requested = True

    def restore_or_initialize(self, restore: Callable[[], int | None], save: Callable[[int, str], None]) -> int:
        step = restore()
        if step is None:
            save(0, "step-zero")
            return 0
        return step

    def checkpoint_if_due(self, step: int, save: Callable[[int, str], None]) -> bool:
        now = time.monotonic()
        if not self._requested and now < self._next:
            return False
        reason = "usr1" if self._requested else "periodic"
        save(step, reason)
        self._requested = False
        self._next = time.monotonic() + self.interval_seconds
        return True
