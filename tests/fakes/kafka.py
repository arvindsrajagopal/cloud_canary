"""Small Kafka fakes that never import a Kafka client or use a network."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple


@dataclass(frozen=True)
class FakeTopicMetadata:
    partitions: Tuple[int, ...]


@dataclass(frozen=True)
class FakeClusterMetadata:
    brokers: Tuple[int, ...]
    topics: Dict[str, FakeTopicMetadata]


class FakeFuture:
    def __init__(self, value: Any = None, error: Optional[Exception] = None):
        self._value = value
        self._error = error

    def result(self, timeout: Optional[float] = None) -> Any:
        del timeout
        if self._error is not None:
            raise self._error
        return self._value


class FakeAdminClient:
    def __init__(self, broker_count: int = 1, topics: Optional[Dict[str, int]] = None):
        self.broker_count = broker_count
        self.topics = dict(topics or {})
        self.calls: List[Tuple[str, Any]] = []
        self.failures: Dict[str, Deque[Exception]] = {}

    def fail_next(self, operation: str, error: Exception) -> None:
        self.failures.setdefault(operation, deque()).append(error)

    def _error(self, operation: str) -> Optional[Exception]:
        failures = self.failures.get(operation)
        return failures.popleft() if failures else None

    def list_topics(self, timeout: Optional[float] = None) -> FakeClusterMetadata:
        self.calls.append(("list_topics", timeout))
        error = self._error("list_topics")
        if error:
            raise error
        return FakeClusterMetadata(
            brokers=tuple(range(self.broker_count)),
            topics={
                name: FakeTopicMetadata(tuple(range(count)))
                for name, count in self.topics.items()
            },
        )

    def create_topics(self, topics: Iterable[Any]) -> Dict[str, FakeFuture]:
        results = {}
        for topic in topics:
            name = getattr(topic, "topic", str(topic))
            count = int(getattr(topic, "num_partitions", 1))
            self.calls.append(("create_topic", (name, count)))
            error = self._error("create_topic")
            if error is None:
                self.topics[name] = count
            results[name] = FakeFuture(error=error)
        return results

    def create_partitions(self, requests: Iterable[Any]) -> Dict[str, FakeFuture]:
        results = {}
        for request in requests:
            name = getattr(request, "topic", str(request))
            count = int(getattr(request, "new_total_count", self.topics.get(name, 0)))
            self.calls.append(("create_partitions", (name, count)))
            error = self._error("create_partitions")
            if error is None:
                self.topics[name] = count
            results[name] = FakeFuture(error=error)
        return results

    def delete_topics(self, names: Iterable[str]) -> Dict[str, FakeFuture]:
        results = {}
        for name in names:
            self.calls.append(("delete_topic", name))
            error = self._error("delete_topic")
            if error is None:
                self.topics.pop(name, None)
            results[name] = FakeFuture(error=error)
        return results


class FakeProducer:
    def __init__(self, max_messages: int = 100):
        if max_messages <= 0:
            raise ValueError("max_messages must be positive")
        self.max_messages = max_messages
        self.pending: Deque[Dict[str, Any]] = deque()
        self.delivered: List[Dict[str, Any]] = []

    def produce(self, **record: Any) -> None:
        if len(self.pending) >= self.max_messages:
            raise BufferError("fake producer queue is full")
        self.pending.append(dict(record))

    def poll(self, timeout: float = 0.0) -> int:
        del timeout
        if not self.pending:
            return 0
        record = self.pending.popleft()
        self.delivered.append(record)
        callback = record.get("on_delivery")
        if callback:
            callback(None, FakeMessage(record))
        return 1

    def flush(self, timeout: Optional[float] = None) -> int:
        del timeout
        while self.pending:
            self.poll()
        return 0


class FakeMessage:
    def __init__(self, record: Dict[str, Any]):
        self._record = record

    def topic(self) -> str:
        return str(self._record.get("topic", ""))

    def partition(self) -> int:
        return int(self._record.get("partition", 0))

    def offset(self) -> int:
        return 0

    def value(self) -> Any:
        return self._record.get("value")


class FakeConsumer:
    def __init__(self, records: Optional[Iterable[Any]] = None):
        self.records: Deque[Any] = deque(records or [])
        self.assignment: Optional[Any] = None
        self.closed = False
        self.calls: List[Tuple[str, Any]] = []

    def assign(self, partitions: Iterable[Any]) -> None:
        values = tuple(partitions)
        self.assignment = values
        self.calls.append(("assign", values))

    def poll(self, timeout: Optional[float] = None) -> Any:
        self.calls.append(("poll", timeout))
        return self.records.popleft() if self.records else None

    def get_watermark_offsets(self, partition: Any, timeout: Optional[float] = None):
        self.calls.append(("get_watermark_offsets", (partition, timeout)))
        return (0, len(self.records))

    def seek(self, partition: Any) -> None:
        self.calls.append(("seek", partition))

    def close(self) -> None:
        self.closed = True
        self.calls.append(("close", None))
