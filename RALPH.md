# Cloud Canary Ralph Harness

This repository includes a bounded Ralph loop for implementing `SPEC.md` in
small, tested, independently reviewed tasks. The loop is intentionally unable
to start from `main` or from an unreviewed dirty baseline.

## Safety Model

The runner:

- Works only inside this repository.
- Invokes Codex with a workspace-write sandbox for implementation and a
  read-only sandbox for review.
- Disables interactive tool approvals inside non-interactive Codex runs; an
  operation that needs additional authority must fail and become a human stop.
- Does not enable web search or authorize dependency installation.
- Never reads ignored configuration or secret files as part of its quality
  gate or repository snapshots.
- Treats `SPEC.md` as immutable task input and excludes it from implementation
  write scopes.
- Never creates a branch automatically.
- Never commits or pushes on `main` or `master`.
- Requires an exact interactive confirmation before each commit and push.
- Never force-pushes, tags, opens a pull request, releases, or deploys.

Running Codex can send task-relevant repository context to the configured Codex
service. Do not start the loop if the repository contains source or test data
that is not approved for that service.

## Bootstrap Validation

Run the deterministic local gate before starting Ralph:

```bash
python3 scripts/quality_gate.py
python3 scripts/ralph.py --plan
```

The quality gate uses only the Python standard library. It performs:

- Python syntax parsing without generating bytecode.
- JSON parsing.
- Repository text and whitespace checks.
- `git diff --check`.
- Full `unittest` discovery under `tests/`.

Use the Python interpreter from the intended project environment when later
tests require production dependencies:

```bash
python scripts/quality_gate.py --python python
```

## Starting a Loop

The checked-in plan is active through endpoint security and enterprise
monitoring. It uses invariant-sized tasks and stops deliberately at `R14`,
which is marked as the next replan gate. The
remaining broad phases must be split and reviewed before they can execute. See
`ralph/REPLAN.md`.

Before the first implementation run:

1. Review and checkpoint `SPEC.md`, this harness, and any other intended
   baseline files.
2. Obtain approval to create or select a feature branch.
3. Ensure `git status --short` is empty.
4. Confirm that the branch is not `main` or `master`.

Then run (select the project interpreter if production dependencies are not
available in the system Python):

```bash
.venv/bin/python scripts/ralph.py --max-iterations 36 --max-attempts 2
```

Useful modes:

```bash
# Render the next implementation prompt without invoking Codex.
python3 scripts/ralph.py --dry-run

# Run the next pending task only. Out-of-sequence task IDs are rejected.
python3 scripts/ralph.py --task R01

# Run automation without commit/push prompts. Milestones are left for a human.
python3 scripts/ralph.py --non-interactive
```

Runtime state and reports are written under `.ralph-state/`, which is ignored by
Git. `ralph/tasks.json` is the immutable versioned task plan; completion state
must not be written into that plan.

## Execution Order and Conflict Prevention

`ralph/tasks.json` declares one execution order. Every task depends on its
immediate predecessor, and completed tasks must be an exact prefix of that
order. The normal loop and `--task` use the same selector; `--task` cannot skip
ahead or reopen a completed task.

The ordered stages cover core health and scheduling through R43,
R44-R46 for failure classification and recovery, R91-R98/R93 for checkpoint
review integrity and recovery-boundary correction, R99/R49 for bounded HTTP
concurrency, R47/R50/R48/R51-R52 for bounded HTTP safety and lifecycle,
R53-R56 for endpoint and monitoring security, R14-R18 for startup,
portability, and secrets, and R19-R24 for supply-chain,
multi-instance ownership, topology, observability, and resource containment.
Each stage ends in a cumulative milestone review.

Only one Ralph runner may hold the repository lock. Ralph records the expected
branch, Git HEAD, and a content fingerprint of every non-ignored, non-secret
repository file. It verifies them before task selection and before every agent
attempt. A commit outside the milestone checkpoint workflow, a manual edit, or
another writer's change causes a human stop instead of being absorbed into the
next task.

Each task's original hashes, allowed-path text, and working-tree paths are
persisted in `.ralph-state/task-baseline.json`. Retries and process restarts
therefore continue to review the complete unaccepted task delta rather than
only the last incremental edit.

## Iteration Gates

For every task, Ralph:

1. Runs the complete existing quality gate before changes.
2. Captures a content-hash snapshot of non-ignored repository files.
3. Invokes one bounded implementation attempt.
4. Rejects changes outside the task's declared path scope.
5. Requires every declared task test file to exist.
6. Runs the complete quality gate again.
7. Invokes a separate read-only reviewer.
8. Requires structured review of consistency, security, architecture,
   performance, best practices, and specification deviation.
9. Rejects any failed category or high/critical finding.
10. Records the attempt, changed files, validation, and review result.

At R28, R12, R18, and R24, Ralph also builds a cumulative patch from the prior
milestone baseline and runs a separate read-only integration review. That gate
looks specifically for cross-task interface, ownership, configuration, state,
security, performance, portability, and architectural conflicts before the
milestone can be accepted.

An attempt may be retried only with the previous failure or review feedback in
the next prompt. The two-review ceiling is a hard policy maximum; a command-line
option cannot raise it. A second failed review stops for task splitting,
specification clarification, or reviewer-conflict resolution—not another
implementation attempt.

## Context and Quality Controls

The active phases use one invariant and ownership
boundary per task, with at most four changed files and 500 diff lines.
Execution stops at the `R14` replan gate before entering the remaining broad
phases. Every Codex
invocation is an ephemeral session. The runner
injects the exact assigned criteria and targeted specification section numbers;
it directs the agent to use targeted searches instead of loading the complete
specification.

Tasks also identify the exact numbered sections, if any, of
`ralph/confluent-development-reference.md` that may provide relevant Confluent
Cloud Python guidance. The reference is explicitly non-normative: it cannot
alter `SPEC.md`, expand task scope, authorize network access, or trigger tool or
skill installation.

Each implementation must return schema-validated JSON. The runner independently
compares its reported files with the repository snapshot and stops if an
iteration exceeds 4 changed files or 500 diff lines. It saves the isolated
iteration patch, complete Codex logs, and complete quality output under
`.ralph-state/`. Only bounded summaries are carried into later attempts:
4,000 feedback characters and at most 10 blocking review findings.

The read-only reviewer also receives `ralph/decisions.json`, the review cycle,
and prior findings. Every finding is scope-classified. Future-task findings and
optional improvements cannot fail the current task; specification gaps stop for
human resolution. A new blocker first raised in correction review requires a
novelty explanation so incomplete or contradictory reviews do not create an
unbounded retry loop.

Low- and medium-severity findings are retained in review artifacts but
automatically accepted. Only high/critical in-scope defects or introduced
regressions block progress; architecture/specification gaps always stop for a
human decision.

The read-only reviewer receives only the assigned task and criteria, the current
iteration's file list and isolated patch, and bounded quality evidence. It is
explicitly excluded from re-reviewing changes accepted in earlier tasks. This
keeps task context deterministic across fresh sessions and prevents a growing
conversation from being compacted at an arbitrary point. If a coherent change
does not fit the budgets, Ralph stops for a human to split the task instead of
silently truncating the implementation context.

## Test-Harness Growth

The bootstrap suite validates the Ralph machinery and deterministic fakes. Each
remediation task must add the executable tests declared in `ralph/tasks.json`
before that task can pass. All checked-in tests must remain green after every
accepted task.

The plan maps every one of the 119 `SPEC.md` regression criteria exactly once.
Ralph cannot finish until every task is accepted and the final complete suite
passes. Tests must use mocks, fakes, and injected clocks; they must not contact
real Kafka, Schema Registry, secret stores, or another external service.

Shared fakes are in `tests/fakes/`. Tests should be placed according to scope:

- `tests/unit/`: state, scheduling, classification, configuration, and client
  boundary behavior.
- `tests/integration/`: multiple in-process components without external
  services.
- `tests/contract/`: HTTP, metrics, configuration, container, and documentation
  contracts.
- `tests/longevity/`: thousands of deterministic cycles, retries, and topology
  changes with resource-bound assertions.

## Human-Intervention Stops

Exit code `2` means a human decision or authorization is required. The reason is
written to `.ralph-state/human-intervention.json`. Expected stops include:

- An ambiguous or missing specification decision.
- A required secret, network call, dependency change, or destructive action.
- Unexpected files outside task scope.
- Repeated test or review failures.
- A malformed or human-required review.
- A changed task plan during an active run.
- Branch mismatch, concurrent loop lock, or an unsafe Git checkpoint.

Exit code `1` is an automation error. Exit code `0` means the requested task or
loop completed its gates.

If `.ralph-state/lock` remains after a crash, first verify that no Ralph or Codex
process is active. Removing a stale lock is a deliberate human recovery action;
the script never deletes an existing lock speculatively.

## Git Checkpoints

Tasks `R28`, `R12`, `R18`, and `R24` are logical milestones. After their tests
and review pass, the runner displays the working tree and offers two separate
confirmations:

1. `COMMIT` stages all changes since the previously clean/checkpointed baseline
   and creates a milestone commit.
2. `PUSH` pushes the current feature-branch `HEAD` to `origin`.

Declining either action leaves the tested work local. There is no automatic
push, force push, merge, pull request, tag, release, or deployment.
