"""Deterministic fakes shared by unit and in-process integration tests."""

from tests.fakes.clock import FakeClock
from tests.fakes.kafka import FakeAdminClient, FakeConsumer, FakeProducer
from tests.fakes.schema_registry import FakeSchemaRegistryClient

__all__ = [
    "FakeAdminClient",
    "FakeClock",
    "FakeConsumer",
    "FakeProducer",
    "FakeSchemaRegistryClient",
]
