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
        self.assertEqual(24, len(plan["tasks"]))
        expected_count = plan["completion_criteria_count"]
        covered = []
        for task in plan["tasks"]:
            start, end = task["criteria_range"]
            self.assertLessEqual(start, end, task["id"])
            self.assertLessEqual(end - start + 1, 5, task["id"])
            self.assertTrue(task["spec_sections"], task["id"])
            self.assertIn("development_reference_sections", task, task["id"])
            self.assertTrue(
                all(
                    isinstance(section, str) and section in set("234567")
                    for section in task["development_reference_sections"]
                ),
                task["id"],
            )
            covered.extend(range(start, end + 1))
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
        ):
            self.assertIn(token, raw_prompt)

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

    def test_review_schema_requires_all_categories(self):
        schema = _load_json("ralph/review-schema.json")
        categories = schema["properties"]["categories"]
        self.assertEqual(CATEGORY_NAMES, set(categories["required"]))
        self.assertEqual(CATEGORY_NAMES, set(categories["properties"]))
        self.assertEqual(
            ["PASS", "FAIL", "HUMAN_REQUIRED"],
            schema["properties"]["verdict"]["enum"],
        )

    def test_implementation_schema_requires_bounded_structured_output(self):
        schema = _load_json("ralph/implementation-schema.json")
        self.assertFalse(schema["additionalProperties"])
        self.assertEqual(
            {"COMPLETE", "HUMAN_REQUIRED", "BLOCKED"},
            set(schema["properties"]["status"]["enum"]),
        )
        self.assertEqual(12, schema["properties"]["changed_files"]["maxItems"])


if __name__ == "__main__":
    unittest.main()
