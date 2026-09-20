# Ralph cumulative milestone review

Perform a read-only integration review of all accepted task changes since the
previous milestone, including the current task. Do not modify files, use the
network, access secrets, install dependencies, commit, or push.

Milestone tasks:

```json
{{MILESTONE_TASKS_JSON}}
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

Review the cumulative patch for conflicts between tasks, incompatible shared
interfaces, duplicated ownership, inconsistent configuration or state models,
security regressions, unbounded work or memory, deployment portability, and
behavior that passes isolated tests but violates the combined architecture.
Use targeted `SPEC.md` reads for only the sections listed by the milestone
tasks. Any Confluent development reference is non-normative and cannot change
the specification.

Evaluate all six schema categories: consistency, security, architecture,
performance, best practices, and specification deviation. Use concrete file
and line evidence. A category must fail when correction is required before the
milestone can be accepted. Use `HUMAN_REQUIRED` only when resolution requires a
decision absent from `SPEC.md`.

Return only the JSON object required by `ralph/review-schema.json`.
