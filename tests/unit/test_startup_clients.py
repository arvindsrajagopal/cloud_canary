"""Regression tests for validation through retained startup clients."""

import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from src.main import (
    validate_kafka_startup_client,
    validate_schema_registry_startup_client,
)


class StartupClientValidationTests(unittest.TestCase):
    def test_kafka_validation_uses_supplied_admin_client(self):
        metadata = SimpleNamespace(brokers={1: object(), 2: object()})
        admin = Mock()
        admin.list_topics.return_value = metadata

        validate_kafka_startup_client(admin)

        admin.list_topics.assert_called_once_with(timeout=15)

    def test_schema_registry_validation_uses_supplied_client(self):
        schema_registry = Mock()

        validate_schema_registry_startup_client(schema_registry)

        schema_registry.get_subjects.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
