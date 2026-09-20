"""Scriptable Schema Registry fake."""

from __future__ import annotations

from collections import deque
from typing import Any, Deque, Iterable, List


class FakeSchemaRegistryClient:
    def __init__(self, outcomes: Iterable[Any] = ()):
        self.outcomes: Deque[Any] = deque(outcomes)
        self.calls = 0

    def queue(self, outcome: Any) -> None:
        self.outcomes.append(outcome)

    def get_subjects(self) -> List[str]:
        self.calls += 1
        outcome = self.outcomes.popleft() if self.outcomes else []
        if isinstance(outcome, Exception):
            raise outcome
        return list(outcome)
