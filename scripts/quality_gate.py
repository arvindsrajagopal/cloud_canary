#!/usr/bin/env python3
"""Deterministic, dependency-free quality gate for Cloud Canary."""

from __future__ import annotations

import argparse
import ast
import json
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence


ROOT = Path(__file__).resolve().parents[1]
TEXT_SUFFIXES = {
    ".ini",
    ".json",
    ".md",
    ".py",
    ".sh",
    ".txt",
    ".yml",
    ".yaml",
}
SECRET_PATHS = {"config/config.ini"}
SECRET_SUFFIXES = {".jks", ".key", ".p12", ".pem", ".pfx"}


@dataclass(frozen=True)
class GateResult:
    name: str
    passed: bool
    duration_seconds: float
    detail: str


def _is_prohibited_path(path: Path) -> bool:
    relative = path.as_posix()
    return (
        relative in SECRET_PATHS
        or any(part == "secrets" for part in path.parts)
        or any(part.startswith(".env") for part in path.parts)
        or path.suffix.lower() in SECRET_SUFFIXES
    )


def _run(
    command: Sequence[str], timeout_seconds: int = 300
) -> subprocess.CompletedProcess:
    return subprocess.run(
        list(command),
        cwd=str(ROOT),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout_seconds,
        check=False,
    )


def _repository_files() -> List[Path]:
    completed = _run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
        timeout_seconds=30,
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stdout.strip() or "git ls-files failed")
    files = []
    for raw in completed.stdout.splitlines():
        if not raw:
            continue
        path = Path(raw)
        if _is_prohibited_path(path):
            continue
        if any(part in {".git", ".venv", ".ralph-state", "secrets"} for part in path.parts):
            continue
        absolute = ROOT / path
        if absolute.is_file():
            files.append(absolute)
    return sorted(files)


def _timed_gate(name: str, operation) -> GateResult:
    started = time.monotonic()
    try:
        detail = operation()
        return GateResult(name, True, time.monotonic() - started, detail or "ok")
    except Exception as exc:  # Gate failures must be reported, not hide the rest.
        return GateResult(name, False, time.monotonic() - started, str(exc))


def _check_python(files: Iterable[Path]) -> str:
    checked = 0
    for path in files:
        if path.suffix != ".py":
            continue
        source = path.read_text(encoding="utf-8")
        ast.parse(source, filename=str(path))
        checked += 1
    return "parsed {} Python files".format(checked)


def _check_json(files: Iterable[Path]) -> str:
    checked = 0
    for path in files:
        if path.suffix != ".json":
            continue
        with path.open("r", encoding="utf-8") as handle:
            json.load(handle)
        checked += 1
    return "parsed {} JSON files".format(checked)


def _check_whitespace(files: Iterable[Path]) -> str:
    failures = []
    checked = 0
    for path in files:
        if path.suffix not in TEXT_SUFFIXES and path.name not in {"Dockerfile", ".gitignore"}:
            continue
        text = path.read_text(encoding="utf-8")
        checked += 1
        for number, line in enumerate(text.splitlines(), start=1):
            if line.endswith((" ", "\t")):
                failures.append("{}:{} trailing whitespace".format(path.relative_to(ROOT), number))
        if text and not text.endswith("\n"):
            failures.append("{} missing final newline".format(path.relative_to(ROOT)))
    if failures:
        raise RuntimeError("\n".join(failures[:100]))
    return "checked {} text files".format(checked)


def _check_git_diff() -> str:
    completed = _run(["git", "diff", "--check"], timeout_seconds=30)
    if completed.returncode != 0:
        raise RuntimeError(completed.stdout.strip() or "git diff --check failed")
    return "git diff --check passed"


def _run_tests(python_executable: str, timeout_seconds: int) -> str:
    completed = _run(
        [
            python_executable,
            "-m",
            "unittest",
            "discover",
            "-s",
            "tests",
            "-p",
            "test_*.py",
            "-v",
        ],
        timeout_seconds=timeout_seconds,
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stdout.strip() or "unit tests failed")
    lines = completed.stdout.strip().splitlines()
    return "\n".join(lines[-20:])


def _write_report(path: Path, results: Sequence[GateResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "passed": all(result.passed for result in results),
        "results": [asdict(result) for result in results],
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def run_quality_gate(
    python_executable: str,
    test_timeout_seconds: int,
    report_path: Optional[Path] = None,
) -> List[GateResult]:
    files = _repository_files()
    results = [
        _timed_gate("python-syntax", lambda: _check_python(files)),
        _timed_gate("json-syntax", lambda: _check_json(files)),
        _timed_gate("whitespace", lambda: _check_whitespace(files)),
        _timed_gate("git-diff", _check_git_diff),
        _timed_gate(
            "unit-tests",
            lambda: _run_tests(python_executable, test_timeout_seconds),
        ),
    ]
    if report_path is not None:
        _write_report(report_path, results)
    return results


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--test-timeout-seconds", type=int, default=300)
    parser.add_argument("--report", type=Path)
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    report = args.report
    if report is not None and not report.is_absolute():
        report = ROOT / report
    results = run_quality_gate(args.python, args.test_timeout_seconds, report)
    for result in results:
        marker = "PASS" if result.passed else "FAIL"
        print("[{}] {} ({:.3f}s)".format(marker, result.name, result.duration_seconds))
        if not result.passed:
            print(result.detail)
    return 0 if all(result.passed for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
