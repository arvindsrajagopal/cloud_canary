"""Unit tests for Ralph orchestration and review gates."""

from __future__ import annotations

import inspect
import json
import unittest
from unittest import mock

from scripts import ralph


def _category(status="PASS", findings=None):
    return {"status": status, "findings": list(findings or [])}


def _review(verdict="PASS", human=False):
    return {
        "verdict": verdict,
        "summary": "reviewed",
        "human_intervention": human,
        "categories": {name: _category() for name in ralph.REVIEW_CATEGORIES},
    }


class ScopeTests(unittest.TestCase):
    def test_allowed_paths_support_files_and_directories(self):
        self.assertTrue(ralph._path_allowed("src/main.py", ["src/"]))
        self.assertTrue(ralph._path_allowed("README.md", ["README.md"]))
        self.assertFalse(ralph._path_allowed("Dockerfile", ["src/", "tests/"]))

    def test_content_snapshots_detect_add_change_and_delete(self):
        before = {"same": "1", "changed": "1", "deleted": "1"}
        after = {"same": "1", "changed": "2", "added": "1"}
        self.assertEqual(
            {"changed", "deleted", "added"},
            ralph._changed_since(before, after),
        )

    def test_snapshot_inventory_prohibits_secret_bearing_paths(self):
        for path in (
            "config/config.ini",
            ".env.local",
            "secrets/token",
            "tls/client.p12",
            "tls/private.pem",
        ):
            self.assertTrue(ralph._is_prohibited_path(path), path)
        self.assertFalse(ralph._is_prohibited_path("src/config.py"))


class ReviewGateTests(unittest.TestCase):
    def test_clean_review_passes(self):
        passed, detail = ralph._validate_review(_review())
        self.assertTrue(passed)
        self.assertEqual("reviewed", detail)

    def test_failed_category_blocks_review(self):
        review = _review()
        review["verdict"] = "FAIL"
        review["categories"]["security"] = _category("FAIL")
        passed, detail = ralph._validate_review(review)
        self.assertFalse(passed)
        self.assertIn("security", detail)

    def test_high_finding_blocks_even_if_category_is_warning(self):
        finding = {
            "severity": "HIGH",
            "file": "src/main.py",
            "line": 1,
            "message": "unsafe",
            "recommendation": "fix it",
        }
        review = _review()
        review["categories"]["security"] = _category("WARN", [finding])
        passed, detail = ralph._validate_review(review)
        self.assertFalse(passed)
        self.assertIn("HIGH", detail)

    def test_human_review_stops_loop(self):
        with self.assertRaises(ralph.HumanIntervention):
            ralph._validate_review(_review("HUMAN_REQUIRED", human=True))

    def test_missing_review_category_is_invalid(self):
        review = _review()
        del review["categories"]["performance"]
        with self.assertRaises(ralph.RalphError):
            ralph._validate_review(review)


class ContextControlTests(unittest.TestCase):
    def test_bounded_text_retains_head_and_tail(self):
        bounded = ralph._bounded_text("A" * 100 + "Z" * 100, 100)
        self.assertLessEqual(len(bounded), 100)
        self.assertTrue(bounded.startswith("A"))
        self.assertTrue(bounded.endswith("Z"))
        self.assertIn("omitted", bounded)

    def test_task_criteria_are_extracted_without_whole_spec(self):
        task = ralph._load_plan()["tasks"][0]
        criteria = ralph._criteria_text(task)
        self.assertEqual(5, len(criteria.splitlines()))
        self.assertTrue(criteria.startswith("1. "))
        self.assertIn("5. ", criteria)
        self.assertNotIn("6. ", criteria)

    def test_implementation_report_requires_exact_structure(self):
        report = {
            "status": "COMPLETE",
            "summary": "done",
            "changed_files": [],
            "tests_added_or_updated": [],
            "tests_run": [],
            "assumptions": [],
            "blockers": [],
            "remaining_work": [],
        }
        ralph._validate_implementation_report(report)
        report["extra"] = []
        with self.assertRaises(ralph.RalphError):
            ralph._validate_implementation_report(report)

    def test_snapshot_fingerprint_is_order_independent(self):
        self.assertEqual(
            ralph._snapshot_fingerprint({"a": "1", "b": "2"}),
            ralph._snapshot_fingerprint({"b": "2", "a": "1"}),
        )

    def test_repository_state_rejects_head_drift(self):
        state = {
            "head": "expected",
            "repository_fingerprint": "files",
            "working_tree_fingerprint": "status",
        }
        with mock.patch.object(ralph, "_head", return_value="unexpected"):
            with self.assertRaises(ralph.HumanIntervention):
                ralph._assert_repository_state(state, "test")

    def test_repository_state_rejects_working_tree_drift(self):
        state = {
            "head": "head",
            "repository_fingerprint": "files",
            "working_tree_fingerprint": "expected",
        }
        with mock.patch.object(ralph, "_head", return_value="head"), mock.patch.object(
            ralph, "_snapshot_fingerprint", return_value="files"
        ), mock.patch.object(
            ralph, "_working_tree_fingerprint", return_value="unexpected"
        ):
            with self.assertRaises(ralph.HumanIntervention):
                ralph._assert_repository_state(state, "test")

    def test_codex_approval_flag_precedes_exec_subcommand(self):
        source = inspect.getsource(ralph._invoke_codex)
        self.assertLess(source.index('"-a"'), source.index('"exec"'))


class PlanAndPromptTests(unittest.TestCase):
    def test_plan_selects_first_incomplete_task(self):
        plan = ralph._load_plan()
        state = {"completed_tasks": ["R01"]}
        self.assertEqual("R02", ralph._select_task(plan, state, None)["id"])

    def test_requested_completed_task_is_rejected(self):
        plan = ralph._load_plan()
        state = {"completed_tasks": ["R01"]}
        with self.assertRaises(ralph.RalphError):
            ralph._select_task(plan, state, "R01")

    def test_requested_task_cannot_skip_the_next_task(self):
        plan = ralph._load_plan()
        state = {"completed_tasks": []}
        with self.assertRaises(ralph.HumanIntervention):
            ralph._select_task(plan, state, "R02")

    def test_state_completion_must_be_an_execution_order_prefix(self):
        plan = ralph._load_plan()
        state = {
            "completed_tasks": ["R02"],
            "current_task": None,
            "repository_fingerprint": "fingerprint",
            "attempts": {},
        }
        with self.assertRaises(ralph.HumanIntervention):
            ralph._validate_state(plan, state)

    def test_state_current_task_must_be_next_in_sequence(self):
        plan = ralph._load_plan()
        state = {
            "completed_tasks": ["R01"],
            "current_task": "R03",
            "repository_fingerprint": "fingerprint",
            "attempts": {},
        }
        with self.assertRaises(ralph.HumanIntervention):
            ralph._validate_state(plan, state)

    def test_prompt_rendering_allows_braces_in_feedback(self):
        rendered = ralph._render_prompt(
            ralph.IMPLEMENT_PROMPT_PATH,
            {
                "TASK_JSON": json.dumps({"id": "R01"}),
                "CRITERIA_TEXT": "1. example",
                "ATTEMPT": "1",
                "MAX_ATTEMPTS": "3",
                "FEEDBACK": "dictionary was {'bounded': False}",
            },
        )
        self.assertIn("'bounded': False", rendered)
        self.assertNotIn("{{TASK_JSON}}", rendered)

    def test_task_cannot_pass_before_declared_tests_exist(self):
        task = ralph._load_plan()["tasks"][0]
        passed, detail = ralph._declared_tests_exist(task)
        self.assertFalse(passed)
        self.assertIn("tests/unit/test_partition_health.py", detail)


if __name__ == "__main__":
    unittest.main()
