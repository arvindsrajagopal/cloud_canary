"""Bounded, fair scheduling for per-partition Kafka checks."""

from __future__ import annotations

import heapq
import math
import time
from concurrent.futures import Future
from dataclasses import dataclass
from threading import RLock
from typing import Callable, Iterable, TypeVar

from src.bounded_executor import DaemonThreadPoolExecutor


_Result = TypeVar("_Result")


class OperationDeadlineExceeded(TimeoutError):
    """A scheduled operation did not finish before its overall deadline."""

    def __init__(self, timeout: float) -> None:
        self.timeout = timeout
        super().__init__(f"operation exceeded its {timeout:g}s deadline")


class ScheduledOperationLane:
    """A dedicated, single-flight lane for one periodic blocking operation."""

    def __init__(
        self,
        interval: float,
        *,
        initial_delay: float = 0.0,
        timeout: float | None = None,
        monotonic_clock: Callable[[], float] = time.monotonic,
        executor=None,
        thread_name_prefix: str = "cloud-canary-operation",
    ) -> None:
        if interval <= 0:
            raise ValueError("interval must be positive")
        if initial_delay < 0:
            raise ValueError("initial_delay must not be negative")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive")
        self.interval = float(interval)
        self.timeout = None if timeout is None else float(timeout)
        self._monotonic = monotonic_clock
        self._next_due = self._monotonic() + initial_delay
        self._executor = (
            executor
            if executor is not None
            else DaemonThreadPoolExecutor(
                max_workers=1, thread_name_prefix=thread_name_prefix
            )
        )
        self._owns_executor = executor is None
        self._future: Future | None = None
        self._deadline: float | None = None
        self._completed_at: float | None = None
        self._timeout_reported = False
        self._closed = False
        self._lock = RLock()

    @property
    def in_flight(self) -> bool:
        with self._lock:
            return self._future is not None

    def seconds_until_deadline(self, *, now: float | None = None) -> float | None:
        """Return the remaining unreported deadline delay, if one is active."""
        current = self._monotonic() if now is None else now
        with self._lock:
            if (
                self._future is None
                or self._deadline is None
                or self._timeout_reported
            ):
                return None
            return max(0.0, self._deadline - current)

    def seconds_until_next_due(self, *, now: float | None = None) -> float | None:
        """Return when this lane can next dispatch, or None while occupied."""
        current = self._monotonic() if now is None else now
        with self._lock:
            if self._future is not None:
                return None
            return max(0.0, self._next_due - current)

    def dispatch_if_due(
        self, operation: Callable[[], _Result], *, now: float | None = None
    ) -> bool:
        """Submit one due occurrence, dropping any missed while it is in flight."""
        current = self._monotonic() if now is None else now
        with self._lock:
            if (
                self._closed
                or self._future is not None
                or current < self._next_due
            ):
                return False
            periods = max(1, math.floor((current - self._next_due) / self.interval) + 1)
            self._next_due += periods * self.interval
            self._completed_at = None
            self._future = self._executor.submit(operation)
            self._future.add_done_callback(self._record_completion)
            self._deadline = (
                None if self.timeout is None else current + self.timeout
            )
            self._timeout_reported = False
            return True

    def _record_completion(self, completed: Future) -> None:
        """Publish completion time atomically for the matching invocation."""
        completed_at = self._monotonic()
        with self._lock:
            if self._future is completed:
                self._completed_at = completed_at

    def take_completed(self, *, now: float | None = None) -> Future | None:
        """Return completion or one deadline event without permitting overlap.

        A timed-out worker remains in flight until it exits.  This lets callers
        classify the probe at its overall deadline while preserving the lane's
        single-flight guarantee and discarding any late result.
        """
        current = self._monotonic() if now is None else now
        with self._lock:
            if self._future is None:
                return None
            if self._future.done():
                # Future completion becomes visible before callbacks are
                # guaranteed to finish.  Do not retire this invocation until
                # its callback has atomically published the completion time.
                if self._completed_at is None:
                    return None
                completed = self._future
                completed_at = self._completed_at
                if (
                    self._deadline is not None
                    and completed_at > self._deadline
                    and not self._timeout_reported
                ):
                    self._timeout_reported = True
                    timed_out = Future()
                    timed_out.set_exception(
                        OperationDeadlineExceeded(self.timeout)
                    )
                    return timed_out
                self._future = None
                self._deadline = None
                self._completed_at = None
                if self._timeout_reported:
                    # Consume a late exception and suppress the late result: the
                    # deadline event was already the authoritative outcome.
                    if not completed.cancelled():
                        completed.exception()
                    return None
                return completed
            if (
                self._deadline is not None
                and current >= self._deadline
                and not self._timeout_reported
            ):
                self._timeout_reported = True
                timed_out = Future()
                timed_out.set_exception(OperationDeadlineExceeded(self.timeout))
                return timed_out
            return None

    def close(self, *, wait: bool = True) -> None:
        with self._lock:
            self._closed = True
        if self._owns_executor:
            self._executor.shutdown(wait=wait, cancel_futures=True)


@dataclass
class _PartitionSchedule:
    next_due: float
    in_flight: bool = False


@dataclass(frozen=True)
class SchedulerSnapshot:
    pending: int
    in_flight: int
    oldest_overdue_seconds: float
    coverage_duration_seconds: float


class PartitionScheduler:
    """Own exactly one monotonic scheduling record per expected partition."""

    def __init__(
        self,
        partitions: Iterable[int],
        interval: float,
        *,
        monotonic_clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if interval <= 0:
            raise ValueError("interval must be positive")
        self.interval = float(interval)
        self._monotonic = monotonic_clock
        self._lock = RLock()
        self._records: dict[int, _PartitionSchedule] = {}
        self._future: list[tuple[float, int]] = []
        self._due: list[tuple[float, int]] = []
        self._deadline_versions: dict[int, int] = {}
        self._minimum_deadlines: list[tuple[float, int, int]] = []
        self._maximum_deadlines: list[tuple[float, int, int]] = []
        self._in_flight = 0
        self.replace_partitions(partitions)

    @staticmethod
    def _validated(partitions: Iterable[int]) -> list[int]:
        result = sorted(set(partitions))
        if any(not isinstance(partition, int) or partition < 0 for partition in result):
            raise ValueError("partitions must be non-negative integers")
        return result

    def replace_partitions(self, partitions: Iterable[int]) -> None:
        """Replace a generation and spread its first starts over one interval."""
        expected = self._validated(partitions)
        now = self._monotonic()
        spacing = self.interval / len(expected) if expected else 0.0
        with self._lock:
            if any(record.in_flight for record in self._records.values()):
                raise RuntimeError("cannot replace partitions while checks are in flight")
            self._records = {
                partition: _PartitionSchedule(now + index * spacing)
                for index, partition in enumerate(expected)
            }
            self._rebuild_indexes(now)

    def _rebuild_indexes(self, now: float) -> None:
        """Rebuild bounded deadline indexes after a topology change."""
        available = [
            (record.next_due, partition)
            for partition, record in self._records.items()
            if not record.in_flight
        ]
        self._due = [item for item in available if item[0] <= now]
        self._future = [item for item in available if item[0] > now]
        heapq.heapify(self._due)
        heapq.heapify(self._future)
        self._deadline_versions = {partition: 0 for partition in self._records}
        self._minimum_deadlines = [
            (record.next_due, partition, 0)
            for partition, record in self._records.items()
        ]
        self._maximum_deadlines = [
            (-record.next_due, partition, 0)
            for partition, record in self._records.items()
        ]
        heapq.heapify(self._minimum_deadlines)
        heapq.heapify(self._maximum_deadlines)
        self._in_flight = sum(record.in_flight for record in self._records.values())

    def _promote_due(self, now: float) -> None:
        while self._future and self._future[0][0] <= now:
            heapq.heappush(self._due, heapq.heappop(self._future))

    def _deadline_is_current(self, partition: int, version: int) -> bool:
        return self._deadline_versions.get(partition) == version

    def _deadline_bounds(self) -> tuple[float, float] | None:
        while self._minimum_deadlines and not self._deadline_is_current(
            self._minimum_deadlines[0][1], self._minimum_deadlines[0][2]
        ):
            heapq.heappop(self._minimum_deadlines)
        while self._maximum_deadlines and not self._deadline_is_current(
            self._maximum_deadlines[0][1], self._maximum_deadlines[0][2]
        ):
            heapq.heappop(self._maximum_deadlines)
        if not self._minimum_deadlines:
            return None
        return self._minimum_deadlines[0][0], -self._maximum_deadlines[0][0]

    def _update_deadline_index(self, partition: int, deadline: float) -> None:
        version = self._deadline_versions[partition] + 1
        self._deadline_versions[partition] = version
        heapq.heappush(self._minimum_deadlines, (deadline, partition, version))
        heapq.heappush(self._maximum_deadlines, (-deadline, partition, version))
        # Lazy stale entries keep updates logarithmic. Rebuild before either
        # index can grow beyond a constant multiple of topology size.
        bound = max(1, len(self._records) * 2)
        if len(self._minimum_deadlines) > bound or len(self._maximum_deadlines) > bound:
            self._rebuild_deadline_bounds()

    def _rebuild_deadline_bounds(self) -> None:
        self._minimum_deadlines = []
        self._maximum_deadlines = []
        for partition, record in self._records.items():
            version = self._deadline_versions[partition]
            self._minimum_deadlines.append((record.next_due, partition, version))
            self._maximum_deadlines.append((-record.next_due, partition, version))
        heapq.heapify(self._minimum_deadlines)
        heapq.heapify(self._maximum_deadlines)

    def reconcile_partitions(self, partitions: Iterable[int]) -> None:
        """Preserve existing deadlines and evenly stagger newly added records."""
        expected = self._validated(partitions)
        expected_set = set(expected)
        now = self._monotonic()
        with self._lock:
            removed = set(self._records) - expected_set
            if any(self._records[partition].in_flight for partition in removed):
                raise RuntimeError("cannot remove a partition while its check is in flight")
            for partition in removed:
                del self._records[partition]
            added = [partition for partition in expected if partition not in self._records]
            spacing = self.interval / len(added) if added else 0.0
            for index, partition in enumerate(added):
                self._records[partition] = _PartitionSchedule(now + index * spacing)
            self._rebuild_indexes(now)

    def acquire_due(self, limit: int, *, now: float | None = None) -> tuple[int, ...]:
        """Mark and return the oldest eligible records, using partition as tie-breaker."""
        if limit < 0:
            raise ValueError("limit must be non-negative")
        current = self._monotonic() if now is None else now
        with self._lock:
            self._promote_due(current)
            selected = []
            while self._due and len(selected) < limit:
                _, partition = heapq.heappop(self._due)
                self._records[partition].in_flight = True
                self._in_flight += 1
                selected.append(partition)
            return tuple(selected)

    def complete(self, partition: int, *, now: float | None = None) -> None:
        """Complete one check and coalesce every cadence missed while it ran."""
        current = self._monotonic() if now is None else now
        with self._lock:
            try:
                record = self._records[partition]
            except KeyError as exc:
                raise ValueError(f"partition {partition} is not scheduled") from exc
            if not record.in_flight:
                raise RuntimeError(f"partition {partition} is not in flight")
            periods = max(1, math.floor((current - record.next_due) / self.interval) + 1)
            record.next_due += periods * self.interval
            record.in_flight = False
            self._in_flight -= 1
            self._update_deadline_index(partition, record.next_due)
            if record.next_due <= current:
                heapq.heappush(self._due, (record.next_due, partition))
            else:
                heapq.heappush(self._future, (record.next_due, partition))

    def seconds_until_next_due(self, *, now: float | None = None) -> float | None:
        current = self._monotonic() if now is None else now
        with self._lock:
            self._promote_due(current)
            if self._due:
                return 0.0
            if not self._future:
                return None
            return max(0.0, self._future[0][0] - current)

    def snapshot(self, *, now: float | None = None) -> SchedulerSnapshot:
        current = self._monotonic() if now is None else now
        with self._lock:
            self._promote_due(current)
            bounds = self._deadline_bounds()
            oldest_overdue = max(0.0, current - bounds[0]) if bounds else 0.0
            coverage_duration = (
                max(bounds[1], current) - bounds[0] if bounds else 0.0
            )
            return SchedulerSnapshot(
                pending=len(self._due),
                in_flight=self._in_flight,
                oldest_overdue_seconds=oldest_overdue,
                coverage_duration_seconds=max(0.0, coverage_duration),
            )
