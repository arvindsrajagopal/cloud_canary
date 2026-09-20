"""Controllable monotonic and wall clocks for deterministic tests."""

from __future__ import annotations

import threading


class FakeClock:
    def __init__(self, monotonic: float = 0.0, wall: float = 1_700_000_000.0):
        self._monotonic = float(monotonic)
        self._wall = float(wall)
        self._lock = threading.Lock()

    def monotonic(self) -> float:
        with self._lock:
            return self._monotonic

    def time(self) -> float:
        with self._lock:
            return self._wall

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("FakeClock cannot move backwards")
        with self._lock:
            self._monotonic += seconds
            self._wall += seconds

    def jump_wall(self, seconds: float) -> None:
        """Move wall time independently to test monotonic deadline behavior."""
        with self._lock:
            self._wall += seconds
