# Ralph cumulative milestone review

Perform a read-only integration review of all accepted task changes since the
previous milestone, including the current task. Do not modify files, use the
network, access secrets, install dependencies, commit, or push.

Milestone tasks:

```json
{{MILESTONE_TASKS_JSON}}
```

Frozen decisions:

```json
{{DECISION_LEDGER}}
```

Changed files in the cumulative milestone patch:

```text
{{CHANGED_FILES}}
```

The cumulative patch is stored at:

```text
{{PATCH_PATH}}
```

Complete quality-gate evidence:

```text
{{QUALITY_EVIDENCE}}
```

This is review cycle {{REVIEW_CYCLE}}. Prior findings and their known
disposition:

```text
{{PRIOR_FINDINGS}}
```

Review the cumulative patch for conflicts between tasks, incompatible shared
interfaces, duplicated ownership, inconsistent configuration or state models,
security regressions, unbounded work or memory, deployment portability, and
behavior that passes isolated tests but violates the combined architecture.

Reassess the complete cumulative patch rather than limiting review to earlier
findings or the latest correction. For every new centralized policy or
architectural abstraction, trace it to production callers with concrete
evidence. If production integration is intentionally deferred, name its
specific pending owner and verify that task identifier exists in the active
execution plan in `ralph/tasks.json`.

Compare test expectations independently with the assigned `SPEC.md` text.
Passing tests and quality-gate evidence support the review but do not prove
specification compliance.

The normative review scope is strictly limited to the union of the numbered
`criteria_range` values and explicit `acceptance_conditions` in the milestone
tasks. Use targeted `SPEC.md` reads for only the sections listed by those tasks
and only to interpret their assigned criteria or support conditions.
Requirements assigned to future acceptance criteria must not block this
milestone merely because they are not implemented yet. Do not report
pre-existing or future-scope omissions as findings.

You may still fail the milestone when the cumulative patch regresses an
assigned criterion, creates a conflict between completed tasks, or introduces
an architectural choice that directly prevents a future criterion from being
implemented. In that case, identify the concrete conflict introduced by this
patch rather than treating the future criterion itself as currently due. Any
Confluent development reference is non-normative and cannot change the
specification.

Classify every finding using the review schema. Only high/critical
`BLOCKER_CURRENT_TASK` and `INTRODUCED_REGRESSION` findings may fail the milestone.
Low/medium findings are recorded and automatically accepted.
`FUTURE_TASK` findings must name a pending task identifier present in the active
execution plan in `ralph/tasks.json`; they and `NON_BLOCKING_IMPROVEMENT`
findings are non-blocking. `SPEC_GAP` requires `HUMAN_REQUIRED`. Frozen
decisions may be challenged only with a direct conflicting citation from
`SPEC.md`.

On review cycle 2, a newly reported blocker must state whether the correction
introduced it or give a concrete explanation of why it could not reasonably
have been identified in cycle 1. Without that novelty explanation, treat it as
a review-quality issue rather than another implementation attempt.

Evaluate all six schema categories: consistency, security, architecture,
performance, best practices, and specification deviation. Use concrete file
and line evidence. A category must fail when correction is required before the
milestone can be accepted. Use `HUMAN_REQUIRED` only when resolution requires a
decision absent from `SPEC.md`.

Return only the JSON object required by `ralph/review-schema.json`.
