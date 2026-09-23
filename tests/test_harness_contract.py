"""Self-tests for the Ralph plan and its repository contracts."""

from __future__ import annotations

import json
import re
import unittest
from pathlib import Path, PurePosixPath


ROOT = Path(__file__).resolve().parents[1]
CRITERION_PATTERN = re.compile(r"^(\d+)\.\s", re.MULTILINE)
CATEGORY_NAMES = {
    "consistency",
    "security",
    "architecture",
    "performance",
    "best_practices",
    "specification_deviation",
}


def _load_json(relative_path: str):
    with (ROOT / relative_path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


class SpecificationCoverageTests(unittest.TestCase):
    def test_specification_has_sequential_regression_criteria(self):
        specification = (ROOT / "SPEC.md").read_text(encoding="utf-8")
        section = specification.split("## 14. Testing Requirements", 1)[1]
        section = section.split("## 15. Acceptance Criteria", 1)[0]
        identifiers = [int(value) for value in CRITERION_PATTERN.findall(section)]
        self.assertEqual(list(range(1, 120)), identifiers)

    def test_task_plan_covers_every_criterion_exactly_once(self):
        plan = _load_json("ralph/tasks.json")
        self.assertEqual(56, len(plan["tasks"]))
        self.assertEqual("active", plan["execution_status"])
        self.assertEqual(2, plan["review_policy"]["max_review_cycles_per_task"])
        self.assertTrue(
            plan["review_policy"]["major_findings_require_human_approval"]
        )
        expected_count = plan["completion_criteria_count"]
        covered = []
        for task in plan["tasks"]:
            criteria_range = task["criteria_range"]
            if criteria_range is None:
                conditions = task.get("acceptance_conditions")
                self.assertIsInstance(conditions, list, task["id"])
                self.assertTrue(conditions, task["id"])
                self.assertTrue(
                    all(isinstance(condition, str) and condition.strip() for condition in conditions),
                    task["id"],
                )
            else:
                start, end = criteria_range
                self.assertLessEqual(start, end, task["id"])
                self.assertLessEqual(end - start + 1, 5, task["id"])
                covered.extend(range(start, end + 1))
            self.assertTrue(task["spec_sections"], task["id"])
            self.assertIn("development_reference_sections", task, task["id"])
            self.assertTrue(
                all(
                    isinstance(section, str) and section in set("234567")
                    for section in task["development_reference_sections"]
                ),
                task["id"],
            )
        self.assertEqual(list(range(1, expected_count + 1)), sorted(covered))
        self.assertEqual(len(covered), len(set(covered)))

    def test_tasks_have_unique_ids_safe_paths_and_declared_tests(self):
        plan = _load_json("ralph/tasks.json")
        identifiers = [task["id"] for task in plan["tasks"]]
        self.assertEqual(identifiers, plan["execution_order"])
        self.assertEqual(len(identifiers), len(set(identifiers)))
        self.assertTrue(any(task["milestone"] for task in plan["tasks"]))
        for index, task in enumerate(plan["tasks"]):
            self.assertRegex(task["id"], r"^R\d{2}$")
            expected_dependency = [] if index == 0 else [identifiers[index - 1]]
            self.assertEqual(expected_dependency, task["depends_on"], task["id"])
            self.assertTrue(task["test_files"], task["id"])
            self.assertTrue(task["allowed_paths"], task["id"])
            self.assertNotIn("SPEC.md", task["allowed_paths"], task["id"])
            for raw_path in task["test_files"] + task["allowed_paths"]:
                path = PurePosixPath(raw_path)
                self.assertFalse(path.is_absolute(), raw_path)
                self.assertNotIn("..", path.parts, raw_path)
            for test_path in task["test_files"]:
                self.assertTrue(test_path.startswith("tests/"), test_path)

    def test_context_limits_are_explicit_and_positive(self):
        limits = _load_json("ralph/tasks.json")["context_limits"]
        self.assertEqual(
            {
                "max_feedback_characters",
                "max_changed_files",
                "max_diff_lines",
                "max_review_findings_forwarded",
            },
            set(limits),
        )
        self.assertTrue(all(isinstance(value, int) and value > 0 for value in limits.values()))

    def test_development_reference_is_non_normative_and_source_linked(self):
        plan = _load_json("ralph/tasks.json")
        self.assertEqual(
            "ralph/confluent-development-reference.md",
            plan["development_reference"],
        )
        reference = (ROOT / "ralph/confluent-development-reference.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("non-normative", reference)
        self.assertIn("does not add or change Cloud Canary requirements", reference)
        self.assertIn("https://docs.confluent.io/", reference)

    def test_shutdown_policy_distinguishes_cooperative_and_wedged_workers(self):
        specification = (ROOT / "SPEC.md").read_text(encoding="utf-8")
        normalized_specification = " ".join(specification.split())
        self.assertIn(
            "releases ownership by the shutdown deadline", normalized_specification
        )
        self.assertIn(
            "must not have its consumer closed concurrently", normalized_specification
        )
        self.assertIn("daemon execution boundary", normalized_specification)

    def test_shutdown_publication_defines_atomic_claim_semantics(self):
        specification = " ".join((ROOT / "SPEC.md").read_text(encoding="utf-8").split())
        self.assertIn("atomic, first-write-wins publication claim", specification)
        self.assertIn("state allocated before signal delivery", specification)
        self.assertIn("is not considered published", specification)

    def test_shutdown_task_defers_endpoint_availability_to_r28(self):
        plan = _load_json("ralph/tasks.json")
        r07 = next(task for task in plan["tasks"] if task["id"] == "R07")
        r28 = next(task for task in plan["tasks"] if task["id"] == "R28")
        self.assertNotIn("6.1", r07["spec_sections"])
        self.assertIn("6.1", r28["spec_sections"])

    def test_shutdown_integration_owns_two_phase_cleanup_regressions(self):
        plan = _load_json("ralph/tasks.json")
        r07 = next(task for task in plan["tasks"] if task["id"] == "R07")
        self.assertEqual(
            {
                "src/main.py",
                "tests/integration/test_bounded_shutdown.py",
                "tests/unit/test_execution_lanes.py",
                "tests/unit/test_topic_reconciliation.py",
            },
            set(r07["allowed_paths"]),
        )

    def test_liveness_phase_is_split_by_ownership_boundary(self):
        plan = _load_json("ralph/tasks.json")
        tasks = {task["id"]: task for task in plan["tasks"]}
        self.assertIsNone(tasks["R85"]["criteria_range"])
        self.assertIsNone(tasks["R86"]["criteria_range"])
        self.assertIsNone(tasks["R87"]["criteria_range"])
        self.assertIsNone(tasks["R88"]["criteria_range"])
        self.assertEqual(["R88"], tasks["R28"]["depends_on"])
        self.assertEqual([33, 34], tasks["R28"]["criteria_range"])
        self.assertEqual([35, 35], tasks["R35"]["criteria_range"])
        self.assertEqual([36, 37], tasks["R36"]["criteria_range"])
        self.assertEqual([38, 38], tasks["R37"]["criteria_range"])
        self.assertEqual([39, 39], tasks["R38"]["criteria_range"])
        self.assertEqual([40, 40], tasks["R39"]["criteria_range"])
        self.assertEqual([41, 42], tasks["R40"]["criteria_range"])
        self.assertEqual([43, 43], tasks["R41"]["criteria_range"])
        self.assertIsNone(tasks["R89"]["criteria_range"])
        self.assertEqual([44, 44], tasks["R42"]["criteria_range"])
        self.assertEqual([45, 45], tasks["R43"]["criteria_range"])
        self.assertIsNone(tasks["R90"]["criteria_range"])
        self.assertEqual([46, 47], tasks["R44"]["criteria_range"])
        self.assertEqual([48, 48], tasks["R45"]["criteria_range"])
        self.assertEqual([49, 50], tasks["R46"]["criteria_range"])
        self.assertIsNone(tasks["R91"]["criteria_range"])
        self.assertIsNone(tasks["R92"]["criteria_range"])
        self.assertIsNone(tasks["R93"]["criteria_range"])
        self.assertIsNone(tasks["R94"]["criteria_range"])
        self.assertIsNone(tasks["R95"]["criteria_range"])
        self.assertIsNone(tasks["R96"]["criteria_range"])
        self.assertIsNone(tasks["R97"]["criteria_range"])
        self.assertEqual(["R95"], tasks["R96"]["depends_on"])
        self.assertEqual(["R96"], tasks["R97"]["depends_on"])
        self.assertEqual(["R97"], tasks["R93"]["depends_on"])
        self.assertEqual(["R93"], tasks["R11"]["depends_on"])
        self.assertTrue(tasks["R11"]["requires_replan"])


class PromptAndReviewContractTests(unittest.TestCase):
    def test_implementation_prompt_has_required_tokens_and_safety_stops(self):
        prompt = (ROOT / "ralph/implement-prompt.md").read_text(encoding="utf-8")
        normalized_prompt = " ".join(prompt.split())
        for token in (
            "{{TASK_JSON}}",
            "{{CRITERIA_TEXT}}",
            "{{ATTEMPT}}",
            "{{MAX_ATTEMPTS}}",
            "{{FEEDBACK}}",
            "{{DECISION_LEDGER}}",
        ):
            self.assertIn(token, prompt)
        for phrase in (
            "`HUMAN_REQUIRED`",
            "Do not access secret files",
            "Do not install or upgrade dependencies",
        ):
            self.assertIn(phrase, normalized_prompt)

    def test_review_prompt_covers_all_required_review_dimensions(self):
        raw_prompt = (ROOT / "ralph/review-prompt.md").read_text(encoding="utf-8")
        prompt = raw_prompt.lower()
        for category in CATEGORY_NAMES:
            self.assertIn(category.replace("_", " "), prompt)
        for token in (
            "{{CRITERIA_TEXT}}",
            "{{CHANGED_FILES}}",
            "{{PATCH_PATH}}",
            "{{QUALITY_EVIDENCE}}",
            "{{DECISION_LEDGER}}",
            "{{PRIOR_FINDINGS}}",
            "{{REVIEW_CYCLE}}",
        ):
            self.assertIn(token, raw_prompt)

    def test_review_prompts_enforce_full_patch_spec_and_ownership_review(self):
        plan = _load_json("ralph/tasks.json")
        self.assertEqual("active", plan["execution_status"])
        active_task_ids = set(plan["execution_order"])
        self.assertEqual(
            active_task_ids,
            {task["id"] for task in plan["tasks"]},
        )

        expected_patch_phrases = {
            "ralph/review-prompt.md": "complete isolated patch",
            "ralph/milestone-review-prompt.md": "complete cumulative patch",
        }
        for relative_path, patch_phrase in expected_patch_phrases.items():
            prompt = " ".join((ROOT / relative_path).read_text(encoding="utf-8").split())
            with self.subTest(prompt=relative_path):
                self.assertIn(patch_phrase, prompt)
                self.assertIn("latest correction", prompt)
                self.assertIn("trace it to production callers", prompt)
                self.assertIn("specific pending", prompt)
                self.assertIn("active execution plan in `ralph/tasks.json`", prompt)
                self.assertIn("independently", prompt)
                self.assertIn("do not prove", prompt)
                self.assertIn("`FUTURE_TASK`", prompt)
                self.assertIn("pending task identifier", prompt)

    def test_milestone_review_prompt_requires_cumulative_integration_review(self):
        prompt = (ROOT / "ralph/milestone-review-prompt.md").read_text(
            encoding="utf-8"
        )
        for token in (
            "{{MILESTONE_TASKS_JSON}}",
            "{{CHANGED_FILES}}",
            "{{PATCH_PATH}}",
            "{{QUALITY_EVIDENCE}}",
        ):
            self.assertIn(token, prompt)
        self.assertIn("conflicts between tasks", prompt)
        self.assertIn("strictly limited", prompt)
        self.assertIn("future acceptance criteria must not block", prompt)
        self.assertIn("directly prevents a future criterion", prompt)

    def test_review_schema_requires_all_categories(self):
        schema = _load_json("ralph/review-schema.json")
        categories = schema["properties"]["categories"]
        self.assertEqual(CATEGORY_NAMES, set(categories["required"]))
        self.assertEqual(CATEGORY_NAMES, set(categories["properties"]))
        self.assertEqual(
            ["PASS", "FAIL", "HUMAN_REQUIRED"],
            schema["properties"]["verdict"]["enum"],
        )
        finding = schema["$defs"]["category"]["properties"]["findings"]["items"]
        self.assertIn("classification", finding["required"])
        self.assertEqual(
            {
                "BLOCKER_CURRENT_TASK",
                "INTRODUCED_REGRESSION",
                "FUTURE_TASK",
                "SPEC_GAP",
                "NON_BLOCKING_IMPROVEMENT",
            },
            set(finding["properties"]["classification"]["enum"]),
        )

    def test_implementation_schema_requires_bounded_structured_output(self):
        schema = _load_json("ralph/implementation-schema.json")
        self.assertFalse(schema["additionalProperties"])
        self.assertEqual(
            {"COMPLETE", "HUMAN_REQUIRED", "BLOCKED"},
            set(schema["properties"]["status"]["enum"]),
        )
        self.assertEqual(4, schema["properties"]["changed_files"]["maxItems"])


if __name__ == "__main__":
    unittest.main()
