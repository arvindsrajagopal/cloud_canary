"""Bounded Kafka workers with exclusive, replaceable consumers."""

from __future__ import annotations

from dataclasses import dataclass
from queue import Empty, Queue
from threading import Lock
from typing import Callable, Generic, TypeVar

from confluent_kafka import TopicPartition


T = TypeVar("T")


class ConsumerInvalidError(Exception):
    """Signal that a check left its borrowed consumer unsafe for reuse."""

    def __init__(self, cause: Exception) -> None:
        self.cause = cause
        super().__init__(str(cause))


class WorkerPoolInvariantError(RuntimeError):
    """Raised when the pool can no longer provide its configured capacity."""


@dataclass
class _Worker:
    worker_id: int
    consumer: object | None
    deserializer: object | None
    borrowed: bool = False
    closing: bool = False


class WorkerPool(Generic[T]):
    """A fixed set of workers, each owning exactly one consumer at a time."""

    def __init__(
        self,
        *,
        size: int,
        topic: str,
        factory: Callable[[int], tuple[object, object]],
    ) -> None:
        if size < 1:
            raise ValueError("worker pool size must be positive")
        self._topic = topic
        self._factory = factory
        self._workers: list[_Worker] = []
        self._available: Queue[_Worker] = Queue(maxsize=size)
        self._closed = False
        self._fatal_error: WorkerPoolInvariantError | None = None
        self._state_lock = Lock()
        # Serialize capacity changes without making shutdown wait for consumer
        # construction, which may block inside native client code.
        self._growth_lock = Lock()

        try:
            for worker_id in range(size):
                consumer, deserializer = factory(worker_id)
                worker = _Worker(worker_id, consumer, deserializer)
                self._workers.append(worker)
                self._available.put_nowait(worker)
        except Exception:
            try:
                self.close()
            except WorkerPoolInvariantError:
                pass
            raise

    def __len__(self) -> int:
        return len(self._workers)

    @property
    def consumers(self) -> tuple[object, ...]:
        return tuple(
            worker.consumer
            for worker in self._workers
            if worker.consumer is not None
        )

    def grow(self, size: int) -> None:
        """Atomically add worker-owned consumers without disturbing borrowers."""
        with self._growth_lock:
            with self._state_lock:
                if self._closed:
                    raise WorkerPoolInvariantError("worker pool is closed")
                if self._fatal_error is not None:
                    raise self._fatal_error
                current_size = len(self._workers)
                if size < current_size:
                    raise ValueError("worker pool cannot shrink in place")
                if size == current_size:
                    return

            additions: list[_Worker] = []
            try:
                for worker_id in range(current_size, size):
                    consumer, deserializer = self._factory(worker_id)
                    additions.append(_Worker(worker_id, consumer, deserializer))
            except Exception:
                for worker in additions:
                    try:
                        worker.consumer.close()
                    except Exception:
                        pass
                raise

            publication_error = None
            with self._state_lock:
                if self._closed:
                    publication_error = WorkerPoolInvariantError(
                        "worker pool is closed"
                    )
                elif self._fatal_error is not None:
                    publication_error = self._fatal_error
                else:
                    # Increase the queue bound before atomically publishing the
                    # additions. Existing borrowers may concurrently return.
                    with self._available.mutex:
                        self._available.maxsize = size
                    self._workers.extend(additions)
                    for worker in additions:
                        self._available.put_nowait(worker)

            if publication_error is not None:
                for worker in additions:
                    try:
                        worker.consumer.close()
                    except Exception:
                        pass
                raise publication_error

    def run(self, partition: int, operation: Callable[[object, object], T]) -> T:
        """Run one check with one exclusively borrowed worker-owned consumer."""
        with self._state_lock:
            if self._closed:
                raise WorkerPoolInvariantError("worker pool is closed")
            if self._fatal_error is not None:
                raise self._fatal_error
            try:
                worker = self._available.get_nowait()
            except Empty as exc:
                raise WorkerPoolInvariantError(
                    "worker pool dispatch exceeded configured capacity"
                ) from exc
            worker.borrowed = True
        return_to_pool = True
        try:
            if worker.consumer is None:
                raise self._fail_pool(worker, "worker has no owned consumer")
            consumer = worker.consumer
            deserializer = worker.deserializer
            try:
                consumer.assign([TopicPartition(self._topic, partition)])
            except Exception as exc:
                self._replace_invalid(worker)
                raise exc

            try:
                return operation(consumer, deserializer)
            except ConsumerInvalidError as exc:
                self._replace_invalid(worker)
                raise exc.cause from exc
        except WorkerPoolInvariantError:
            return_to_pool = False
            raise
        finally:
            with self._state_lock:
                worker.borrowed = False
                shutting_down = self._closed
                if shutting_down:
                    worker.closing = True
                if return_to_pool and not shutting_down:
                    self._available.put_nowait(worker)
            if shutting_down:
                if not self._close_owned_worker(worker):
                    raise self._fail_pool(
                        worker, "could not close consumer during shutdown"
                    )

    def _replace_invalid(self, worker: _Worker) -> None:
        """Retire then replace one explicitly invalid consumer."""
        invalid = worker.consumer
        try:
            if invalid is not None:
                invalid.close()
        except Exception as exc:
            raise self._fail_pool(
                worker, "could not retire invalid consumer"
            ) from exc

        worker.consumer = None
        worker.deserializer = None
        try:
            worker.consumer, worker.deserializer = self._factory(worker.worker_id)
        except Exception as exc:
            raise self._fail_pool(
                worker, "could not replace invalid consumer"
            ) from exc

    def _fail_pool(
        self, worker: _Worker, reason: str
    ) -> WorkerPoolInvariantError:
        error = WorkerPoolInvariantError(
            f"worker pool invariant failed for worker {worker.worker_id}: {reason}"
        )
        with self._state_lock:
            if self._fatal_error is None:
                self._fatal_error = error
            return self._fatal_error

    def _close_owned_worker(self, worker: _Worker) -> bool:
        """Close one consumer from its current owner thread when possible."""
        consumer = worker.consumer
        if consumer is None:
            with self._state_lock:
                worker.closing = False
            return True
        try:
            consumer.close()
        except Exception:
            with self._state_lock:
                worker.closing = False
            return False
        with self._state_lock:
            worker.consumer = None
            worker.deserializer = None
            worker.closing = False
        return True

    def begin_shutdown(self) -> None:
        """Atomically reject new work without performing blocking cleanup."""
        with self._state_lock:
            self._closed = True

    def close(self) -> None:
        """Start shutdown and close every currently released consumer."""
        self.begin_shutdown()
        with self._state_lock:
            idle = [
                worker
                for worker in self._workers
                if not worker.borrowed
                and not worker.closing
                and worker.consumer is not None
            ]
            for worker in idle:
                worker.closing = True
        failures = sum(not self._close_owned_worker(worker) for worker in idle)
        if failures:
            raise WorkerPoolInvariantError(
                f"failed to close {failures} worker-owned consumer(s)"
            )
