"""Contract tests for the exact bounded metric-label vocabularies."""

import unittest

from src import metrics
from src.error_classifier import ErrorCategory, Phase, Recoverability
from src.main import INSTANCE_ROLE_VALUES


class MetricEnumTests(unittest.TestCase):
    def test_metric_label_vocabularies_are_closed_and_state_has_no_unknown(self):
        self.assertEqual({"success", "failure"}, set(metrics.RESULT_VALUES))
        self.assertEqual(
            {"healthy", "degraded", "unhealthy"}, set(metrics.STATE_VALUES)
        )
        self.assertEqual({"manage", "observe"}, set(INSTANCE_ROLE_VALUES))
        self.assertEqual(
            {member.value for member in Phase}, set(metrics.FAILURE_PHASE_VALUES)
        )
        self.assertEqual(
            {member.value for member in ErrorCategory},
            set(metrics.FAILURE_CATEGORY_VALUES),
        )
        self.assertEqual(
            {member.value for member in Recoverability},
            set(metrics.RECOVERABILITY_VALUES),
        )
        with self.assertRaises(ValueError):
            metrics.bounded_label("unknown", metrics.STATE_VALUES)


if __name__ == "__main__":
    unittest.main()
