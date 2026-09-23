"""Unit tests for Ralph orchestration and review gates."""

from __future__ import annotations

import inspect
import json
import tempfile
import unittest
from pathlib import Path
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


def _finding(classification="BLOCKER_CURRENT_TASK", severity="HIGH", **overrides):
    finding = {
        "classification": classification,
        "severity": severity,
        "file": "src/main.py",
        "line": 1,
        "message": "unsafe",
        "recommendation": "fix it",
        "future_task": None,
        "novelty_explanation": None,
    }
    finding.update(overrides)
    return finding


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

    def test_task_change_limits_use_stricter_task_budget(self):
        limits = {"max_changed_files": 4, "max_diff_lines": 500}
        self.assertEqual(
            (2, 220),
            ralph._task_change_limits(
                {"budget": {"max_changed_files": 2, "max_diff_lines": 220}},
                limits,
            ),
        )

    def test_task_change_limits_fall_back_to_global_ceiling(self):
        limits = {"max_changed_files": 4, "max_diff_lines": 500}
        self.assertEqual((4, 500), ralph._task_change_limits({}, limits))


class ReviewGateTests(unittest.TestCase):
    def test_clean_review_passes(self):
        passed, detail = ralph._validate_review(_review())
        self.assertTrue(passed)
        self.assertEqual("reviewed", detail)

    def test_failed_category_blocks_review(self):
        review = _review()
        review["verdict"] = "FAIL"
        review["categories"]["security"] = _category("FAIL", [_finding()])
        passed, detail = ralph._validate_review(review)
        self.assertFalse(passed)
        self.assertIn("security", detail)

    def test_high_finding_blocks_even_if_category_is_warning(self):
        finding = _finding()
        review = _review()
        review["categories"]["security"] = _category("WARN", [finding])
        passed, detail = ralph._validate_review(review)
        self.assertFalse(passed)
        self.assertIn("HIGH", detail)

    def test_runner_stops_after_major_review_instead_of_retrying(self):
        source = inspect.getsource(ralph.run)
        marker = "major review finding requires approval"
        self.assertIn(marker, source)
        self.assertLess(source.index(marker), source.index("milestone_result = None"))

    def test_medium_current_task_finding_is_auto_accepted(self):
        review = _review("FAIL")
        review["categories"]["performance"] = _category(
            "FAIL", [_finding(severity="MEDIUM")]
        )
        passed, detail = ralph._validate_review(review)
        self.assertTrue(passed)
        self.assertIn("Auto-accepted minor findings", detail)

    def test_future_task_finding_does_not_block(self):
        review = _review()
        review["categories"]["architecture"] = _category(
            "WARN",
            [_finding("FUTURE_TASK", future_task="R28")],
        )
        passed, _detail = ralph._validate_review(review)
        self.assertTrue(passed)

    def test_specification_gap_requires_human(self):
        review = _review("HUMAN_REQUIRED", human=True)
        review["categories"]["architecture"] = _category(
            "WARN", [_finding("SPEC_GAP")]
        )
        with self.assertRaises(ralph.HumanIntervention):
            ralph._validate_review(review)

    def test_second_review_blocker_requires_novelty_explanation(self):
        review = _review("FAIL")
        review["categories"]["architecture"] = _category("FAIL", [_finding()])
        with self.assertRaises(ralph.HumanIntervention):
            ralph._validate_review(review, review_cycle=2)

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

    def test_support_task_uses_explicit_acceptance_conditions(self):
        task = next(
            task for task in ralph._load_plan()["tasks"] if task["id"] == "R29"
        )
        criteria = ralph._criteria_text(task)
        self.assertTrue(criteria.startswith("Support acceptance: "))
        self.assertIn("first state-mutating operation", criteria)
        self.assertIn("preallocated state", criteria)

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

    def test_next_broad_phase_is_an_explicit_replan_gate(self):
        plan = ralph._load_plan()
        task = next(task for task in plan["tasks"] if task["id"] == "R15")
        self.assertTrue(task["requires_replan"])

    def test_r13_migration_selects_r53_after_completed_prefix(self):
        plan = ralph._load_plan()
        r53_index = plan["execution_order"].index("R53")
        state = {"completed_tasks": plan["execution_order"][:r53_index]}

        self.assertEqual("R52", state["completed_tasks"][-1])
        self.assertEqual("R53", ralph._select_task(plan, state, None)["id"])

    def test_r14_migration_selects_r57_after_completed_prefix(self):
        plan = ralph._load_plan()
        completed = [
            "R01", "R02", "R03", "R25", "R04", "R05", "R26", "R06", "R27",
            "R31", "R32", "R29", "R30", "R33", "R34", "R07", "R85", "R86",
            "R87", "R88", "R28", "R35", "R36", "R37", "R38", "R39", "R40",
            "R41", "R89", "R42", "R43", "R90", "R44", "R45", "R46", "R91",
            "R92", "R94", "R95", "R96", "R97", "R98", "R93", "R99", "R49",
            "R47", "R50", "R48", "R51", "R52", "R53", "R54", "R55", "R56",
        ]
        state = {
            "completed_tasks": completed,
            "current_task": None,
            "repository_fingerprint": "recorded",
            "working_tree_fingerprint": "clean",
            "attempts": {},
            "feedback": {},
            "plan_schema_version": 42,
        }

        self.assertEqual(42, plan["schema_version"])
        ralph._validate_state(plan, state)
        self.assertEqual("R56", state["completed_tasks"][-1])
        self.assertEqual("R57", ralph._select_task(plan, state, None)["id"])

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
        missing_test = "tests/unit/definitely_missing_declared_test.py"
        task = {"test_files": [missing_test]}
        passed, detail = ralph._declared_tests_exist(task)
        self.assertFalse(passed)
        self.assertIn(missing_test, detail)

    def test_task_cannot_pass_with_placeholder_declared_test(self):
        runtime_directory = ralph.ROOT / ".ralph-state" / "test-runtime"
        runtime_directory.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=runtime_directory) as temporary:
            root = Path(temporary)
            test_path = root / "tests" / "test_placeholder.py"
            test_path.parent.mkdir(parents=True)
            test_path.write_text('"""Scaffold only."""\n', encoding="utf-8")
            with mock.patch.object(ralph, "ROOT", root):
                passed, detail = ralph._declared_tests_exist(
                    {"test_files": ["tests/test_placeholder.py"]}
                )
        self.assertFalse(passed)
        self.assertIn("contain no tests", detail)

    def test_declared_test_requires_a_discoverable_test_symbol(self):
        runtime_directory = ralph.ROOT / ".ralph-state" / "test-runtime"
        runtime_directory.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=runtime_directory) as temporary:
            root = Path(temporary)
            test_path = root / "tests" / "test_real.py"
            test_path.parent.mkdir(parents=True)
            test_path.write_text(
                "import unittest\n\n"
                "class ExampleTests(unittest.TestCase):\n"
                "    def test_behavior(self):\n"
                "        self.assertTrue(True)\n",
                encoding="utf-8",
            )
            with mock.patch.object(ralph, "ROOT", root):
                passed, detail = ralph._declared_tests_exist(
                    {"test_files": ["tests/test_real.py"]}
                )
        self.assertTrue(passed)
        self.assertIn("discoverable tests", detail)

    def test_declared_test_execution_rejects_zero_tests(self):
        completed = mock.Mock(returncode=0, stdout="Ran 0 tests in 0.000s\nOK")
        with mock.patch.object(ralph, "_run", return_value=completed):
            passed, detail = ralph._run_declared_tests(
                {"test_files": ["tests/test_placeholder.py"]}, "python", 30
            )
        self.assertFalse(passed)
        self.assertIn("did not execute tests", detail)

    def test_declared_test_execution_reports_each_file(self):
        completed = mock.Mock(returncode=0, stdout="Ran 2 tests in 0.001s\nOK")
        task = {"test_files": ["tests/test_one.py", "tests/test_two.py"]}
        with mock.patch.object(ralph, "_run", return_value=completed) as run:
            passed, detail = ralph._run_declared_tests(task, "python", 30)
        self.assertTrue(passed)
        self.assertEqual(2, run.call_count)
        self.assertIn("tests/test_one.py (2)", detail)
        self.assertIn("tests/test_two.py (2)", detail)


if __name__ == "__main__":
    unittest.main()
