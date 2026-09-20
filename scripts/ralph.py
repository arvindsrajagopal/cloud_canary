#!/usr/bin/env python3
"""Bounded, review-gated Ralph loop for the Cloud Canary remediation."""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple


ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = ROOT / "ralph" / "tasks.json"
IMPLEMENT_PROMPT_PATH = ROOT / "ralph" / "implement-prompt.md"
REVIEW_PROMPT_PATH = ROOT / "ralph" / "review-prompt.md"
MILESTONE_REVIEW_PROMPT_PATH = ROOT / "ralph" / "milestone-review-prompt.md"
REVIEW_SCHEMA_PATH = ROOT / "ralph" / "review-schema.json"
IMPLEMENTATION_SCHEMA_PATH = ROOT / "ralph" / "implementation-schema.json"
DEVELOPMENT_REFERENCE_PATH = ROOT / "ralph" / "confluent-development-reference.md"
STATE_DIR = ROOT / ".ralph-state"
STATE_PATH = STATE_DIR / "state.json"
LOCK_PATH = STATE_DIR / "lock"
QUALITY_REPORT_PATH = STATE_DIR / "quality-report.json"
TASK_BASELINE_PATH = STATE_DIR / "task-baseline.json"
MILESTONE_BASELINE_PATH = STATE_DIR / "milestone-baseline.json"
MAIN_BRANCHES = {"main", "master"}
REVIEW_CATEGORIES = {
    "consistency",
    "security",
    "architecture",
    "performance",
    "best_practices",
    "specification_deviation",
}
PROHIBITED_PATHS = {"config/config.ini"}
PROHIBITED_SUFFIXES = {".jks", ".key", ".p12", ".pem", ".pfx"}
CRITERION_PATTERN = re.compile(r"^(\d+)\.\s(.*)$")


class RalphError(RuntimeError):
    pass


class HumanIntervention(RalphError):
    pass


def _run(
    command: Sequence[str],
    *,
    input_text: Optional[str] = None,
    timeout_seconds: int = 300,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        list(command),
        cwd=str(ROOT),
        input=input_text,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout_seconds,
        check=False,
    )


def _git(*args: str, timeout_seconds: int = 60) -> subprocess.CompletedProcess:
    return _run(["git", *args], timeout_seconds=timeout_seconds)


def _current_branch() -> str:
    result = _git("branch", "--show-current")
    if result.returncode != 0 or not result.stdout.strip():
        raise RalphError(result.stdout.strip() or "unable to determine Git branch")
    return result.stdout.strip()


def _head() -> str:
    result = _git("rev-parse", "HEAD")
    if result.returncode != 0:
        raise RalphError(result.stdout.strip() or "unable to determine HEAD")
    return result.stdout.strip()


def _status() -> str:
    result = _git("status", "--short", "--untracked-files=all")
    if result.returncode != 0:
        raise RalphError(result.stdout.strip() or "git status failed")
    return result.stdout


def _repository_paths() -> List[str]:
    result = _git("ls-files", "--cached", "--others", "--exclude-standard")
    if result.returncode != 0:
        raise RalphError(result.stdout.strip() or "git ls-files failed")
    return sorted(
        line
        for line in result.stdout.splitlines()
        if line and not _is_prohibited_path(line)
    )


def _is_prohibited_path(relative: str) -> bool:
    path = Path(relative)
    return (
        relative in PROHIBITED_PATHS
        or any(part == "secrets" for part in path.parts)
        or any(part.startswith(".env") for part in path.parts)
        or path.suffix.lower() in PROHIBITED_SUFFIXES
    )


def _snapshot() -> Dict[str, str]:
    snapshot = {}
    for relative in _repository_paths():
        path = ROOT / relative
        if not path.is_file():
            continue
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        snapshot[relative] = digest
    return snapshot


def _snapshot_fingerprint(snapshot: Optional[Dict[str, str]] = None) -> str:
    payload = snapshot if snapshot is not None else _snapshot()
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _working_tree_fingerprint() -> str:
    return hashlib.sha256(_status().encode("utf-8")).hexdigest()


def _working_tree_paths() -> Set[str]:
    paths: Set[str] = set()
    commands = (
        ("diff", "--name-only"),
        ("diff", "--cached", "--name-only"),
        ("ls-files", "--others", "--exclude-standard"),
    )
    for command in commands:
        result = _git(*command)
        if result.returncode != 0:
            raise RalphError(result.stdout.strip() or "unable to inspect working tree")
        paths.update(line for line in result.stdout.splitlines() if line)
    return paths


def _changed_since(before: Dict[str, str], after: Dict[str, str]) -> Set[str]:
    return {
        path
        for path in set(before) | set(after)
        if before.get(path) != after.get(path)
    }


def _bounded_text(text: str, limit: int) -> str:
    if limit <= 0:
        raise RalphError("text limit must be positive")
    if len(text) <= limit:
        return text
    marker = "\n... {} characters omitted; full output is in .ralph-state ...\n"
    omitted = len(text) - limit
    rendered_marker = marker.format(omitted)
    available = max(0, limit - len(rendered_marker))
    head = available // 2
    tail = available - head
    return text[:head] + rendered_marker + (text[-tail:] if tail else "")


def _spec_criteria() -> Dict[int, str]:
    specification = (ROOT / "SPEC.md").read_text(encoding="utf-8")
    try:
        testing = specification.split("## 14. Testing Requirements", 1)[1]
        testing = testing.split("## 15. Acceptance Criteria", 1)[0]
    except IndexError as exc:
        raise RalphError("unable to locate SPEC.md testing criteria") from exc
    criteria: Dict[int, List[str]] = {}
    current: Optional[int] = None
    for line in testing.splitlines():
        match = CRITERION_PATTERN.match(line)
        if match:
            current = int(match.group(1))
            criteria[current] = [match.group(2).strip()]
        elif current is not None and (line.startswith("    ") or line.strip() == ""):
            if line.strip():
                criteria[current].append(line.strip())
        elif current is not None:
            current = None
    return {
        identifier: "{}. {}".format(identifier, " ".join(parts))
        for identifier, parts in criteria.items()
    }


def _criteria_text(task: Dict[str, Any]) -> str:
    criteria = _spec_criteria()
    start, end = task["criteria_range"]
    missing = [identifier for identifier in range(start, end + 1) if identifier not in criteria]
    if missing:
        raise RalphError("missing SPEC.md criteria: {}".format(missing))
    return "\n".join(criteria[identifier] for identifier in range(start, end + 1))


def _text_snapshot(allowed_paths: Iterable[str]) -> Dict[str, Optional[str]]:
    snapshot: Dict[str, Optional[str]] = {}
    for relative in _repository_paths():
        if not _path_allowed(relative, allowed_paths):
            continue
        path = ROOT / relative
        try:
            snapshot[relative] = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            snapshot[relative] = None
    return snapshot


def _all_text_snapshot() -> Dict[str, Optional[str]]:
    snapshot: Dict[str, Optional[str]] = {}
    for relative in _repository_paths():
        path = ROOT / relative
        if not path.is_file():
            continue
        try:
            snapshot[relative] = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            snapshot[relative] = None
    return snapshot


def _iteration_patch(
    before: Dict[str, Optional[str]], changed_paths: Iterable[str]
) -> str:
    output: List[str] = []
    for relative in sorted(changed_paths):
        existed_before = relative in before
        old_text = before.get(relative, "")
        path = ROOT / relative
        if path.is_file() and not _is_prohibited_path(relative):
            try:
                new_text: Optional[str] = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                new_text = None
        else:
            new_text = "" if not path.exists() else None
        if (existed_before and old_text is None) or new_text is None:
            output.append("Binary or non-UTF-8 change: {}\n".format(relative))
            continue
        output.extend(
            difflib.unified_diff(
                old_text.splitlines(keepends=True),
                new_text.splitlines(keepends=True),
                fromfile="before/" + relative,
                tofile="after/" + relative,
            )
        )
    return "".join(output)


def _path_allowed(path: str, allowed_paths: Iterable[str]) -> bool:
    for allowed in allowed_paths:
        normalized = allowed.rstrip("/")
        if path == normalized or path.startswith(normalized + "/"):
            return True
    return False


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def _load_plan() -> Dict[str, Any]:
    plan = _load_json(PLAN_PATH)
    if plan.get("development_reference") != str(
        DEVELOPMENT_REFERENCE_PATH.relative_to(ROOT)
    ):
        raise RalphError("task plan has an invalid development reference")
    reference = DEVELOPMENT_REFERENCE_PATH.read_text(encoding="utf-8")
    reference_sections = set(re.findall(r"^## ([2-7])\.", reference, re.MULTILINE))
    if reference_sections != set("234567"):
        raise RalphError("development reference sections are incomplete")
    limits = plan.get("context_limits", {})
    required_limits = {
        "max_feedback_characters",
        "max_changed_files",
        "max_diff_lines",
        "max_review_findings_forwarded",
    }
    if set(limits) != required_limits or any(
        not isinstance(limits[name], int) or limits[name] <= 0 for name in required_limits
    ):
        raise RalphError("task plan has invalid context limits")
    identifiers = [task["id"] for task in plan["tasks"]]
    if len(identifiers) != len(set(identifiers)):
        raise RalphError("task plan contains duplicate task identifiers")
    execution_order = plan.get("execution_order")
    if execution_order != identifiers:
        raise RalphError("task array must match the declared execution order")
    covered = []
    for index, task in enumerate(plan["tasks"]):
        start, end = task["criteria_range"]
        if end - start + 1 > 5:
            raise RalphError("task {} exceeds five acceptance criteria".format(task["id"]))
        if not task.get("spec_sections"):
            raise RalphError("task {} has no targeted specification sections".format(task["id"]))
        development_sections = task.get("development_reference_sections")
        if (
            not isinstance(development_sections, list)
            or len(development_sections) != len(set(development_sections))
            or not set(development_sections).issubset(reference_sections)
        ):
            raise RalphError(
                "task {} has invalid development reference sections".format(task["id"])
            )
        dependencies = task.get("depends_on")
        if (
            not isinstance(dependencies, list)
            or len(dependencies) != len(set(dependencies))
            or not set(dependencies).issubset(set(identifiers[:index]))
        ):
            raise RalphError("task {} has invalid dependencies".format(task["id"]))
        expected_dependency = [] if index == 0 else [identifiers[index - 1]]
        if dependencies != expected_dependency:
            raise RalphError(
                "task {} must depend on its immediate predecessor".format(task["id"])
            )
        covered.extend(range(start, end + 1))
    expected = list(range(1, int(plan["completion_criteria_count"]) + 1))
    if sorted(covered) != expected or len(covered) != len(set(covered)):
        raise RalphError("task plan does not cover every criterion exactly once")
    return plan


def _new_state(plan: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "schema_version": 1,
        "branch": _current_branch(),
        "head": _head(),
        "repository_fingerprint": _snapshot_fingerprint(),
        "working_tree_fingerprint": _working_tree_fingerprint(),
        "completed_tasks": [],
        "attempts": {},
        "feedback": {},
        "history": [],
        "status": "ready",
        "current_task": None,
        "plan_schema_version": plan["schema_version"],
    }


def _load_or_create_state(plan: Dict[str, Any]) -> Dict[str, Any]:
    if STATE_PATH.exists():
        state = _load_json(STATE_PATH)
        if state.get("branch") != _current_branch():
            raise HumanIntervention(
                "Ralph state belongs to branch {!r}; current branch is {!r}.".format(
                    state.get("branch"), _current_branch()
                )
            )
        if state.get("plan_schema_version") != plan["schema_version"]:
            raise HumanIntervention("task plan changed; review or archive existing Ralph state")
        _validate_state(plan, state)
        if not MILESTONE_BASELINE_PATH.exists():
            raise HumanIntervention("Ralph milestone baseline is missing")
        return state
    if _status().strip():
        raise HumanIntervention(
            "The first Ralph run requires a clean working tree. Review and checkpoint "
            "the current specification and harness before starting."
        )
    state = _new_state(plan)
    _write_json(STATE_PATH, state)
    _write_json(
        MILESTONE_BASELINE_PATH,
        {"completed_count": 0, "files": _all_text_snapshot()},
    )
    return state


def _validate_state(plan: Dict[str, Any], state: Dict[str, Any]) -> None:
    order = plan["execution_order"]
    completed = state.get("completed_tasks")
    if (
        not isinstance(completed, list)
        or len(completed) != len(set(completed))
        or completed != order[: len(completed)]
    ):
        raise HumanIntervention(
            "Ralph completion state is not an exact prefix of the execution order."
        )
    expected_current = order[len(completed)] if len(completed) < len(order) else None
    if state.get("current_task") not in {None, expected_current}:
        raise HumanIntervention(
            "Ralph current task is inconsistent with the execution order."
        )
    if not isinstance(state.get("repository_fingerprint"), str):
        raise HumanIntervention("Ralph state has no repository fingerprint.")
    if not isinstance(state.get("working_tree_fingerprint"), str):
        raise HumanIntervention("Ralph state has no working-tree fingerprint.")
    unknown_attempts = set(state.get("attempts", {})) - set(order)
    if unknown_attempts:
        raise HumanIntervention(
            "Ralph state contains attempts for unknown tasks: "
            + ", ".join(sorted(unknown_attempts))
        )
    if any(
        not isinstance(value, int) or value < 0
        for value in state.get("attempts", {}).values()
    ):
        raise HumanIntervention("Ralph state contains an invalid attempt count.")
    if set(state.get("feedback", {})) - set(order):
        raise HumanIntervention("Ralph state contains feedback for an unknown task.")


def _assert_repository_state(state: Dict[str, Any], context: str) -> None:
    current_head = _head()
    if current_head != state["head"]:
        raise HumanIntervention(
            "Git HEAD changed outside the Ralph checkpoint workflow during {}. "
            "Expected {}, found {}.".format(context, state["head"], current_head)
        )
    current_fingerprint = _snapshot_fingerprint()
    if current_fingerprint != state["repository_fingerprint"]:
        raise HumanIntervention(
            "Repository content changed outside the recorded Ralph transition during {}."
            .format(context)
        )
    if _working_tree_fingerprint() != state["working_tree_fingerprint"]:
        raise HumanIntervention(
            "Working-tree status changed outside the recorded Ralph transition during {}."
            .format(context)
        )


def _record_repository_state(
    state: Dict[str, Any], snapshot: Optional[Dict[str, str]] = None
) -> None:
    if _head() != state["head"]:
        raise HumanIntervention("Codex or another writer changed Git HEAD.")
    state["repository_fingerprint"] = _snapshot_fingerprint(snapshot)
    state["working_tree_fingerprint"] = _working_tree_fingerprint()
    _write_json(STATE_PATH, state)


def _load_or_create_task_baseline(
    task: Dict[str, Any], state: Dict[str, Any]
) -> Dict[str, Any]:
    if state.get("current_task") is None:
        baseline = {
            "task": task["id"],
            "hashes": _snapshot(),
            "texts": _text_snapshot(task["allowed_paths"]),
            "worktree_paths": sorted(_working_tree_paths()),
        }
        _write_json(TASK_BASELINE_PATH, baseline)
        state["current_task"] = task["id"]
        state["status"] = "running"
        _write_json(STATE_PATH, state)
        return baseline
    if state["current_task"] != task["id"] or not TASK_BASELINE_PATH.exists():
        raise HumanIntervention("task baseline is missing or belongs to another task")
    baseline = _load_json(TASK_BASELINE_PATH)
    if baseline.get("task") != task["id"]:
        raise HumanIntervention("persisted task baseline has the wrong task identifier")
    if not isinstance(baseline.get("hashes"), dict) or not isinstance(
        baseline.get("texts"), dict
    ) or not isinstance(baseline.get("worktree_paths"), list):
        raise HumanIntervention("persisted task baseline is malformed")
    return baseline


def _acquire_lock() -> int:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(str(LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as exc:
        raise HumanIntervention(
            "Ralph lock already exists. Confirm no loop is active before removing it."
        ) from exc
    os.write(descriptor, str(os.getpid()).encode("ascii"))
    return descriptor


def _release_lock(descriptor: int) -> None:
    os.close(descriptor)
    try:
        LOCK_PATH.unlink()
    except FileNotFoundError:
        pass


def _select_task(
    plan: Dict[str, Any], state: Dict[str, Any], requested_task: Optional[str]
) -> Optional[Dict[str, Any]]:
    completed = set(state["completed_tasks"])
    next_task = next(
        (task for task in plan["tasks"] if task["id"] not in completed), None
    )
    if requested_task:
        matching = [task for task in plan["tasks"] if task["id"] == requested_task]
        if not matching:
            raise RalphError("unknown task {}".format(requested_task))
        if requested_task in completed:
            raise RalphError("task {} is already complete".format(requested_task))
        if next_task is None or requested_task != next_task["id"]:
            raise HumanIntervention(
                "task {} is out of sequence; the next permitted task is {}".format(
                    requested_task, next_task["id"] if next_task else "none"
                )
            )
    if next_task is not None:
        unmet = set(next_task["depends_on"]) - completed
        if unmet:
            raise HumanIntervention(
                "task {} has unmet dependencies: {}".format(
                    next_task["id"], ", ".join(sorted(unmet))
                )
            )
    return next_task


def _render_prompt(path: Path, replacements: Dict[str, str]) -> str:
    prompt = path.read_text(encoding="utf-8")
    for token, value in replacements.items():
        prompt = prompt.replace("{{" + token + "}}", value)
    unresolved = [token for token in replacements if "{{" + token + "}}" in prompt]
    if unresolved:
        raise RalphError(
            "unresolved prompt tokens in {}: {}".format(path, ", ".join(unresolved))
        )
    return prompt


def _quality_gate(
    python_executable: str, timeout_seconds: int, feedback_limit: int
) -> Tuple[bool, str]:
    result = _run(
        [
            python_executable,
            "scripts/quality_gate.py",
            "--python",
            python_executable,
            "--test-timeout-seconds",
            str(timeout_seconds),
            "--report",
            str(QUALITY_REPORT_PATH.relative_to(ROOT)),
        ],
        timeout_seconds=timeout_seconds + 60,
    )
    full_output = result.stdout.strip()
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    quality_log = STATE_DIR / "quality-gate-{}.log".format(time.time_ns())
    quality_log.write_text(full_output + "\n", encoding="utf-8")
    (STATE_DIR / "quality-gate-last.log").write_text(
        full_output + "\n", encoding="utf-8"
    )
    return result.returncode == 0, _bounded_text(full_output, feedback_limit)


def _invoke_codex(
    codex_executable: str,
    prompt: str,
    output_path: Path,
    log_path: Path,
    timeout_seconds: int,
    *,
    read_only: bool,
    output_schema: Optional[Path] = None,
) -> str:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    command = [
        codex_executable,
        "-a",
        "never",
        "exec",
        "--ephemeral",
        "-C",
        str(ROOT),
        "-s",
        "read-only" if read_only else "workspace-write",
        "-o",
        str(output_path),
    ]
    if output_schema is not None:
        command.extend(["--output-schema", str(output_schema)])
    command.append("-")
    try:
        result = _run(command, input_text=prompt, timeout_seconds=timeout_seconds)
    except subprocess.TimeoutExpired as exc:
        raise HumanIntervention(
            "Codex invocation exceeded {} seconds; inspect the task before retrying.".format(
                timeout_seconds
            )
        ) from exc
    log_path.write_text(result.stdout, encoding="utf-8")
    if result.returncode != 0:
        raise HumanIntervention(
            "Codex invocation failed with exit {}. See {}".format(
                result.returncode, log_path.relative_to(ROOT)
            )
        )
    if not output_path.exists():
        raise RalphError("Codex did not create {}".format(output_path.relative_to(ROOT)))
    return output_path.read_text(encoding="utf-8").strip()


def _validate_implementation_report(report: Dict[str, Any]) -> None:
    required = {
        "status",
        "summary",
        "changed_files",
        "tests_added_or_updated",
        "tests_run",
        "assumptions",
        "blockers",
        "remaining_work",
    }
    if set(report) != required:
        raise RalphError("implementation report does not match the required fields")
    if report["status"] not in {"COMPLETE", "HUMAN_REQUIRED", "BLOCKED"}:
        raise RalphError("implementation report has invalid status")
    if not isinstance(report["summary"], str):
        raise RalphError("implementation report summary must be a string")
    for name in required - {"status", "summary"}:
        if not isinstance(report[name], list) or not all(
            isinstance(value, str) for value in report[name]
        ):
            raise RalphError("implementation report field {} must be a string list".format(name))


def _validate_review(
    review: Dict[str, Any], max_findings: int = 20, feedback_limit: int = 8000
) -> Tuple[bool, str]:
    if review.get("verdict") not in {"PASS", "FAIL", "HUMAN_REQUIRED"}:
        raise RalphError("review has invalid verdict")
    categories = review.get("categories")
    if not isinstance(categories, dict) or set(categories) != REVIEW_CATEGORIES:
        raise RalphError("review does not contain the six required categories")
    blocking = []
    blocking_findings = []
    for name, category in categories.items():
        if category.get("status") not in {"PASS", "WARN", "FAIL"}:
            raise RalphError("review category {} has invalid status".format(name))
        if category["status"] == "FAIL":
            blocking.append(name)
        for finding in category.get("findings", []):
            if finding.get("severity") in {"HIGH", "CRITICAL"}:
                blocking.append("{}:{}".format(name, finding.get("severity")))
            if category["status"] == "FAIL" or finding.get("severity") in {"HIGH", "CRITICAL"}:
                if len(blocking_findings) < max_findings:
                    blocking_findings.append(
                        "{category} {severity} {file}:{line} - {message} Recommendation: {recommendation}".format(
                            category=name,
                            severity=finding.get("severity", "UNKNOWN"),
                            file=finding.get("file", "unknown"),
                            line=finding.get("line") or "?",
                            message=finding.get("message", ""),
                            recommendation=finding.get("recommendation", ""),
                        )
                    )
    if review.get("human_intervention") or review["verdict"] == "HUMAN_REQUIRED":
        raise HumanIntervention(review.get("summary", "review requires human intervention"))
    passed = review["verdict"] == "PASS" and not blocking
    detail = review.get("summary", "")
    if blocking:
        detail += "\nBlocking review categories: " + ", ".join(sorted(set(blocking)))
    if blocking_findings:
        detail += "\nBlocking findings:\n" + "\n".join(blocking_findings)
    return passed, _bounded_text(detail.strip(), feedback_limit)


def _declared_tests_exist(task: Dict[str, Any]) -> Tuple[bool, str]:
    missing = [path for path in task["test_files"] if not (ROOT / path).is_file()]
    if missing:
        return False, "Missing declared test files: " + ", ".join(missing)
    return True, "all declared task test files exist"


def _record_event(state: Dict[str, Any], event: Dict[str, Any]) -> None:
    event = dict(event)
    event["timestamp"] = int(time.time())
    state["history"].append(event)
    _write_json(STATE_PATH, state)


def _persist_feedback(
    state: Dict[str, Any], task_id: str, value: str, limit: int
) -> str:
    bounded = _bounded_text(value, limit)
    state.setdefault("feedback", {})[task_id] = bounded
    _write_json(STATE_PATH, state)
    return bounded


def _milestone_review(
    task: Dict[str, Any],
    plan: Dict[str, Any],
    state: Dict[str, Any],
    codex_executable: str,
    timeout_seconds: int,
    quality_detail: str,
    feedback_limit: int,
    max_findings: int,
) -> Dict[str, Any]:
    if not MILESTONE_BASELINE_PATH.exists():
        raise HumanIntervention("milestone baseline is missing")
    baseline = _load_json(MILESTONE_BASELINE_PATH)
    completed_count = baseline.get("completed_count")
    baseline_files = baseline.get("files")
    if (
        not isinstance(completed_count, int)
        or completed_count < 0
        or completed_count > len(state["completed_tasks"])
        or not isinstance(baseline_files, dict)
    ):
        raise HumanIntervention("milestone baseline is malformed")
    current_files = _all_text_snapshot()
    changed = {
        path
        for path in set(baseline_files) | set(current_files)
        if path not in baseline_files
        or path not in current_files
        or baseline_files[path] != current_files[path]
    }
    patch = _iteration_patch(baseline_files, changed)
    prefix = "milestone-{}".format(task["id"].lower())
    patch_path = STATE_DIR / (prefix + "-cumulative.patch")
    patch_path.write_text(patch, encoding="utf-8")
    milestone_tasks = plan["tasks"][completed_count : len(state["completed_tasks"]) + 1]
    prompt = _render_prompt(
        MILESTONE_REVIEW_PROMPT_PATH,
        {
            "MILESTONE_TASKS_JSON": json.dumps(milestone_tasks, indent=2),
            "CHANGED_FILES": "\n".join(sorted(changed)),
            "PATCH_PATH": str(patch_path.relative_to(ROOT)),
            "QUALITY_EVIDENCE": _bounded_text(quality_detail, feedback_limit),
        },
    )
    review_text = _invoke_codex(
        codex_executable,
        prompt,
        STATE_DIR / (prefix + "-review.json"),
        STATE_DIR / (prefix + "-review.log"),
        timeout_seconds,
        read_only=True,
        output_schema=REVIEW_SCHEMA_PATH,
    )
    try:
        review = json.loads(review_text)
    except json.JSONDecodeError as exc:
        raise HumanIntervention(
            "milestone review output was not valid JSON: {}".format(exc)
        ) from exc
    passed, detail = _validate_review(
        review, max_findings=max_findings, feedback_limit=feedback_limit
    )
    if not passed:
        raise HumanIntervention("cumulative milestone review failed:\n" + detail)
    return {
        "completed_count": len(state["completed_tasks"]) + 1,
        "files": current_files,
        "review": review,
        "changed_files": sorted(changed),
        "patch_lines": len(patch.splitlines()),
    }


def _checkpoint(task: Dict[str, Any], state: Dict[str, Any], non_interactive: bool) -> None:
    if not task["milestone"]:
        return
    changed = _status().strip()
    print("\nMilestone {} passed all gates.".format(task["id"]))
    print(changed or "Working tree is clean.")
    if not changed:
        return
    if non_interactive:
        print("Human checkpoint required: review, commit, and push the tested milestone.")
        return
    if _current_branch() in MAIN_BRANCHES:
        raise HumanIntervention(
            "Milestone is ready, but Ralph will not commit on main/master. "
            "Create an approved feature branch, then resume."
        )
    print("Type COMMIT to stage all milestone changes and create a tested checkpoint.")
    if input("> ").strip() != "COMMIT":
        print("Checkpoint skipped; tested changes remain in the working tree.")
        return
    message = "ralph: complete {} {}".format(task["id"], task["title"].lower())
    add = _git("add", "--all")
    if add.returncode != 0:
        raise HumanIntervention(add.stdout.strip() or "git add failed")
    commit = _git("commit", "-m", message, timeout_seconds=120)
    if commit.returncode != 0:
        raise HumanIntervention(commit.stdout.strip() or "git commit failed")
    state["head"] = _head()
    state["working_tree_fingerprint"] = _working_tree_fingerprint()
    _write_json(STATE_PATH, state)
    print(commit.stdout.strip())
    print("Type PUSH to push this branch to origin, or press Enter to keep it local.")
    if input("> ").strip() == "PUSH":
        branch = _current_branch()
        push = _git("push", "--set-upstream", "origin", branch, timeout_seconds=300)
        if push.returncode != 0:
            raise HumanIntervention(push.stdout.strip() or "git push failed")
        print(push.stdout.strip())


def _print_plan(plan: Dict[str, Any], state: Optional[Dict[str, Any]] = None) -> None:
    completed = set((state or {}).get("completed_tasks", []))
    for task in plan["tasks"]:
        status = "complete" if task["id"] in completed else "pending"
        marker = " milestone" if task["milestone"] else ""
        start, end = task["criteria_range"]
        print(
            "{} [{}] criteria {}-{}{}: {}".format(
                task["id"], status, start, end, marker, task["title"]
            )
        )


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", action="store_true", help="print the task plan and exit")
    parser.add_argument(
        "--task", help="run only the specified task when it is the next pending task"
    )
    parser.add_argument("--max-iterations", type=int)
    parser.add_argument("--max-attempts", type=int)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--codex", default=shutil.which("codex") or "codex")
    parser.add_argument("--agent-timeout-seconds", type=int, default=1800)
    parser.add_argument("--test-timeout-seconds", type=int, default=300)
    parser.add_argument("--non-interactive", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def run(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    plan = _load_plan()
    existing_state = _load_json(STATE_PATH) if STATE_PATH.exists() else None
    if args.plan:
        _print_plan(plan, existing_state)
        return 0

    max_iterations = args.max_iterations or int(plan["default_max_iterations"])
    max_attempts = args.max_attempts or int(plan["default_max_attempts_per_task"])
    limits = plan["context_limits"]
    feedback_limit = int(limits["max_feedback_characters"])
    max_changed_files = int(limits["max_changed_files"])
    max_diff_lines = int(limits["max_diff_lines"])
    max_review_findings = int(limits["max_review_findings_forwarded"])
    if max_iterations <= 0 or max_attempts <= 0:
        raise RalphError("iteration and attempt limits must be positive")
    if _current_branch() in MAIN_BRANCHES:
        raise HumanIntervention(
            "Ralph implementation must run on an explicitly approved feature branch, not main/master."
        )
    if not shutil.which(args.codex) and not Path(args.codex).is_file():
        raise HumanIntervention("Codex executable not found: {}".format(args.codex))

    lock_descriptor = _acquire_lock()
    try:
        state = _load_or_create_state(plan)
        state.setdefault("feedback", {})
        iterations = 0
        while iterations < max_iterations:
            _assert_repository_state(state, "task selection")
            task = _select_task(plan, state, args.task)
            if task is None:
                passed, detail = _quality_gate(
                    args.python, args.test_timeout_seconds, feedback_limit
                )
                if not passed:
                    raise HumanIntervention("final quality gate failed:\n" + detail)
                _assert_repository_state(state, "after final quality gate")
                state["status"] = "complete"
                state["current_task"] = None
                _record_event(state, {"event": "complete", "detail": detail})
                print("All Ralph tasks and the final quality gate are complete.")
                return 0

            feedback = state["feedback"].get(task["id"], "none")
            iterations += 1

            pre_passed, pre_detail = _quality_gate(
                args.python, args.test_timeout_seconds, feedback_limit
            )
            if not pre_passed:
                raise HumanIntervention(
                    "pre-change quality gate failed; repair the baseline before continuing:\n"
                    + pre_detail
                )
            _assert_repository_state(state, "after pre-change quality gate")

            task_baseline = _load_or_create_task_baseline(task, state)
            task_before = task_baseline["hashes"]
            task_before_text = task_baseline["texts"]
            task_before_worktree = set(task_baseline["worktree_paths"])
            prior_attempts = int(state["attempts"].get(task["id"], 0))
            if prior_attempts >= max_attempts:
                raise HumanIntervention(
                    "task {} has already exhausted its {} persisted attempts".format(
                        task["id"], max_attempts
                    )
                )
            for attempt in range(prior_attempts + 1, max_attempts + 1):
                _assert_repository_state(state, "before {} attempt {}".format(task["id"], attempt))
                state["attempts"][task["id"]] = attempt
                _write_json(STATE_PATH, state)
                attempt_before = _snapshot()
                task_json = json.dumps(task, indent=2)
                criteria_text = _criteria_text(task)
                prompt = _render_prompt(
                    IMPLEMENT_PROMPT_PATH,
                    {
                        "TASK_JSON": task_json,
                        "CRITERIA_TEXT": criteria_text,
                        "ATTEMPT": str(attempt),
                        "MAX_ATTEMPTS": str(max_attempts),
                        "FEEDBACK": _bounded_text(feedback or "none", feedback_limit),
                    },
                )
                if args.dry_run:
                    print(prompt)
                    return 0

                prefix = "{}-attempt-{}".format(task["id"].lower(), attempt)
                last_message = _invoke_codex(
                    args.codex,
                    prompt,
                    STATE_DIR / (prefix + "-implementation.txt"),
                    STATE_DIR / (prefix + "-implementation.log"),
                    args.agent_timeout_seconds,
                    read_only=False,
                    output_schema=IMPLEMENTATION_SCHEMA_PATH,
                )
                new_worktree_paths = _working_tree_paths() - task_before_worktree
                prohibited_changes = sorted(
                    path for path in new_worktree_paths if _is_prohibited_path(path)
                )
                if prohibited_changes:
                    raise HumanIntervention(
                        "task modified prohibited secret-bearing paths: "
                        + ", ".join(prohibited_changes)
                    )
                after = _snapshot()
                attempt_changed = _changed_since(attempt_before, after)
                changed = _changed_since(task_before, after)
                outside_scope = sorted(
                    path for path in changed if not _path_allowed(path, task["allowed_paths"])
                )
                if outside_scope:
                    raise HumanIntervention(
                        "task changed files outside its allowed scope: " + ", ".join(outside_scope)
                    )
                if len(changed) > max_changed_files:
                    raise HumanIntervention(
                        "task changed {} files; the per-iteration limit is {}".format(
                            len(changed), max_changed_files
                        )
                    )
                iteration_patch = _iteration_patch(task_before_text, changed)
                diff_lines = len(iteration_patch.splitlines())
                patch_path = STATE_DIR / (prefix + "-current.patch")
                patch_path.write_text(iteration_patch, encoding="utf-8")
                if diff_lines > max_diff_lines:
                    raise HumanIntervention(
                        "task produced {} diff lines; the per-iteration limit is {}. "
                        "Inspect {} and split the change.".format(
                            diff_lines, max_diff_lines, patch_path.relative_to(ROOT)
                        )
                    )
                _record_repository_state(state, after)
                try:
                    implementation = json.loads(last_message)
                    _validate_implementation_report(implementation)
                except (json.JSONDecodeError, RalphError) as exc:
                    feedback = _persist_feedback(
                        state,
                        task["id"],
                        "Implementation output was invalid: {}".format(exc),
                        feedback_limit,
                    )
                    continue
                declared_changed = set(implementation["changed_files"])
                if declared_changed != attempt_changed:
                    feedback = _persist_feedback(
                        state,
                        task["id"],
                        "Implementation changed-file report did not match the repository. "
                        "Declared: {}. Actual: {}.".format(
                            sorted(declared_changed), sorted(attempt_changed)
                        ),
                        feedback_limit,
                    )
                    continue
                if implementation["status"] == "HUMAN_REQUIRED":
                    raise HumanIntervention(
                        _bounded_text(
                            implementation["summary"]
                            + "\n"
                            + "\n".join(implementation["blockers"]),
                            feedback_limit,
                        )
                    )
                if implementation["status"] == "BLOCKED":
                    feedback = _persist_feedback(
                        state,
                        task["id"],
                        implementation["summary"]
                        + "\n"
                        + "\n".join(implementation["blockers"]),
                        feedback_limit,
                    )
                    continue
                if not changed:
                    feedback = _persist_feedback(
                        state,
                        task["id"],
                        "No repository files have changed for this task.",
                        feedback_limit,
                    )
                    continue

                tests_exist, test_detail = _declared_tests_exist(task)
                quality_passed, quality_detail = _quality_gate(
                    args.python, args.test_timeout_seconds, feedback_limit
                )
                if not tests_exist or not quality_passed:
                    feedback = _persist_feedback(
                        state,
                        task["id"],
                        "{}\n{}".format(test_detail, quality_detail), feedback_limit
                    )
                    _record_event(
                        state,
                        {
                            "event": "validation_failed",
                            "task": task["id"],
                            "attempt": attempt,
                            "changed_files": sorted(changed),
                            "diff_lines": diff_lines,
                            "detail": feedback,
                        },
                    )
                    continue

                _assert_repository_state(state, "before read-only task review")

                review_prompt = _render_prompt(
                    REVIEW_PROMPT_PATH,
                    {
                        "TASK_JSON": task_json,
                        "CRITERIA_TEXT": criteria_text,
                        "CHANGED_FILES": "\n".join(sorted(changed)),
                        "PATCH_PATH": str(patch_path.relative_to(ROOT)),
                        "QUALITY_EVIDENCE": _bounded_text(
                            quality_detail, feedback_limit
                        ),
                    },
                )
                review_text = _invoke_codex(
                    args.codex,
                    review_prompt,
                    STATE_DIR / (prefix + "-review.json"),
                    STATE_DIR / (prefix + "-review.log"),
                    args.agent_timeout_seconds,
                    read_only=True,
                    output_schema=REVIEW_SCHEMA_PATH,
                )
                try:
                    review = json.loads(review_text)
                except json.JSONDecodeError as exc:
                    feedback = _persist_feedback(
                        state,
                        task["id"],
                        "Review output was not valid JSON: {}".format(exc),
                        feedback_limit,
                    )
                    continue
                review_passed, review_detail = _validate_review(
                    review,
                    max_findings=max_review_findings,
                    feedback_limit=feedback_limit,
                )
                if not review_passed:
                    feedback = _persist_feedback(
                        state, task["id"], review_detail, feedback_limit
                    )
                    _record_event(
                        state,
                        {
                            "event": "review_failed",
                            "task": task["id"],
                            "attempt": attempt,
                            "changed_files": sorted(changed),
                            "diff_lines": diff_lines,
                            "review_verdict": review["verdict"],
                            "detail": review_detail,
                        },
                    )
                    continue

                _assert_repository_state(state, "after read-only task review")

                milestone_result = None
                if task["milestone"]:
                    milestone_result = _milestone_review(
                        task,
                        plan,
                        state,
                        args.codex,
                        args.agent_timeout_seconds,
                        quality_detail,
                        feedback_limit,
                        max_review_findings,
                    )
                    _assert_repository_state(state, "after cumulative milestone review")
                state["completed_tasks"].append(task["id"])
                state["feedback"].pop(task["id"], None)
                state["status"] = "ready"
                state["current_task"] = None
                _record_event(
                    state,
                    {
                        "event": "task_complete",
                        "task": task["id"],
                        "attempt": attempt,
                        "changed_files": sorted(changed),
                        "diff_lines": diff_lines,
                        "implementation_summary": implementation["summary"],
                        "quality": quality_detail,
                        "review_verdict": review["verdict"],
                        "review_summary": review.get("summary", ""),
                        "milestone_review": (
                            milestone_result["review"]["summary"]
                            if milestone_result is not None
                            else None
                        ),
                    },
                )
                if milestone_result is not None:
                    _write_json(
                        MILESTONE_BASELINE_PATH,
                        {
                            "completed_count": milestone_result["completed_count"],
                            "files": milestone_result["files"],
                        },
                    )
                _checkpoint(task, state, args.non_interactive)
                break
            else:
                raise HumanIntervention(
                    "task {} exhausted {} attempts. Last feedback:\n{}".format(
                        task["id"], max_attempts, feedback
                    )
                )

            if args.task:
                return 0

        raise HumanIntervention(
            "maximum iteration count {} reached before completion".format(max_iterations)
        )
    finally:
        _release_lock(lock_descriptor)


def main(argv: Optional[Sequence[str]] = None) -> int:
    try:
        return run(argv)
    except HumanIntervention as exc:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        intervention = {
            "status": "human_required",
            "reason": str(exc),
            "timestamp": int(time.time()),
        }
        _write_json(STATE_DIR / "human-intervention.json", intervention)
        print("HUMAN_REQUIRED: {}".format(exc), file=sys.stderr)
        return 2
    except (RalphError, subprocess.TimeoutExpired) as exc:
        print("RALPH_ERROR: {}".format(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
