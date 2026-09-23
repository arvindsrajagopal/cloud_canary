# Ralph Replan Checkpoint

Ralph uses this checkpoint to split broad tasks before activation. The original
plan grouped work by regression-criterion count, which allowed a single
criterion to span too many ownership boundaries and files. Do not resume a
failed task by increasing an attempt limit.

## Required task shape

Every replacement task must define one invariant, one ownership boundary, its
explicit exclusions, and a fixed failure-mode checklist. A task may cover at
most three specification criteria, four changed files, and 500 diff lines. If
coherent work exceeds a bound, split it before implementation rather than
creating an accepted intermediate baseline inside an active task.

Each task receives two review cycles: the initial review and one correction.
A second failure stops for replanning. The limit cannot be raised by a command-
line option.

## Review contract

Review findings are classified as:

- `BLOCKER_CURRENT_TASK`: violates an assigned invariant or criterion. It blocks
  only at high or critical severity.
- `INTRODUCED_REGRESSION`: the patch breaks previously accepted behavior. It
  blocks only at high or critical severity.
- `FUTURE_TASK`: valid work owned by a later declared task; non-blocking.
- `SPEC_GAP`: resolution requires a human-approved specification or architecture
  decision; stop without another implementation attempt.
- `NON_BLOCKING_IMPROVEMENT`: optional improvement that cannot fail the task.

Low- and medium-severity findings are recorded and automatically accepted.
Only high/critical current-task defects or regressions stop implementation;
`SPEC_GAP` always stops for a human architecture decision.

On the correction review, every newly reported blocker must identify whether it
was introduced by the correction or explain why it could not reasonably have
been found in the initial review. Review advice cannot override the decision
ledger without citing a direct contradiction in `SPEC.md`.

## R07 replacement sequence

Preserve the current draft and review artifacts. Before resuming, restore a
reviewed baseline and replace R07 with these ordered units:

1. First-write-wins shutdown request and immutable deadline.
2. Dispatch transaction linearization and post-request rejection.
3. Worker ownership and cooperative consumer cleanup (already reviewed).
4. Independent daemon execution lanes and admin deadlines (already reviewed).
5. Shutdown-aware Kafka log-handler registration and non-blocking teardown.
6. Startup and runtime lifecycle abandonment at the absolute deadline.
7. End-to-end criterion 32 integration test and review.

Endpoint availability remains in R28. No R07 reviewer may block on `/live` or
`/ready` unless the R07 patch directly makes R28 impossible to implement.

## Remaining-plan split rule

Before reactivation, split every remaining four- or five-criterion task along
its existing behavioral boundaries. Configuration, runtime behavior, endpoint
contracts, deployment artifacts, and longevity/resource tests must not share an
implementation task merely because their criterion numbers are adjacent.

## Proposed ordered task map

Support tasks have explicit acceptance conditions but do not claim a numbered
criterion. The final R07 integration task owns criterion 32. Tasks marked
reviewed may be migrated as completed only after their saved patch and review
artifact are verified against the restored accepted baseline.

| Task | Criteria | Single invariant / ownership boundary |
|---|---:|---|
| R29 | support | First-write-wins shutdown request and immutable deadline |
| R30 | support | Dispatch transaction linearization and post-request rejection |
| R31 | support, reviewed | Worker ownership and cooperative consumer cleanup |
| R32 | support, reviewed | Independent daemon lanes and bounded admin futures |
| R33 | support | Shutdown-aware Kafka log-handler registration and teardown |
| R34 | support | Startup/runtime lifecycle abandonment at the absolute deadline |
| R07 | 32 | End-to-end bounded shutdown integration |
| R85 | support | Authoritative concurrency-safe liveness snapshot |
| R86 | support | Runtime heartbeat, shutdown, and fatal-state publication |
| R87 | support | Startup dependency dispatch authorization |
| R28 | 33–34 | Liveness independence and internal-failure transitions |
| R35 | 35 | Initial post-warmup readiness proof |
| R36 | 36–37 | Readiness persistence and revocation transitions |
| R37 | 38 | Independent `/health`, `/live`, and `/ready` outcomes |
| R38 | 39 | Configuration-aware container liveness probe |
| R39 | 40 | Process-local state reset on restart — milestone |
| R40 | 41–42 | Authoritative state sources and topic-generation fencing |
| R41 | 43 | Immutable concurrent snapshots |
| R42 | 44 | Fatal invariant failure and bounded termination |
| R43 | 45 | Interrupted recreation startup reconciliation |
| R90 | support | Closed bounded failure descriptor model and sanitization |
| R44 | 46–47 | Kafka failure descriptors and client-local classification |
| R45 | 48 | Schema Registry HTTP failure classification |
| R46 | 49–50 | Recovery-policy tests and startup/runtime actions |
| R47 | 51 | Isolated consumer replacement |
| R48 | 52–53 | Unknown-failure policy and sensitive-data exclusion |
| R49 | 54–55 | HTTP worker and queue concurrency bounds |
| R50 | 56 | Queue-saturation semantics |
| R51 | 57–58 | Socket deadlines and sanitized handler failures |
| R52 | 59–60 | HTTP shutdown and dependency-worker separation |
| R53 | 61–62 | Private binding and plaintext exposure warning |
| R54 | 63–64 | HTTP method policy and response redaction |
| R55 | 65 | HTTPS probe verification — milestone |
| R56 | 66–67 | Development monitoring versus enterprise monitoring |
| R102 | support | Retire duplicate-client validation test assumptions |
| R57 | 68 | Long-lived startup client validation |
| R100 | 69 | Deterministic startup failure execution |
| R58 | 70 | Transient startup health and retry-stage isolation |
| R59 | 71–72 | Backoff progression and single-flight initialization |
| R60 | 73–74 | Partial-client cleanup and warmup capability proof |
| R61 | 75 | Production Python entry point |
| R62 | 76–78 | Non-mutating local monitoring workflow |
| R63 | 79–81 | Portable production examples and obsolete launcher removal |
| R64 | 82–83 | Secret-source exclusivity and file parsing |
| R65 | 84–85 | Secret error handling and redaction |
| R66 | 86–87 | Client secret delivery and restart rotation |
| R67 | 88–90 | Minimal non-root image and hash-locked dependencies |
| R68 | 91–93 | Explicit lock refresh and consistent OCI metadata/version |
| R69 | 94–96 | Exec startup, architecture validation, and release evidence — milestone |
| R70 | 97 | Static topic-role configuration |
| R71 | 98–99 | Manager and stable-topic observer behavior |
| R72 | 100–101 | Observer mismatch recovery without election |
| R73 | 102 | Bounded instance-role metric |
| R74 | 103–105 | Role dashboard, manager alert, and observer scaling — milestone |
| R75 | 106–107 | Broker-derived topology and topic policy |
| R76 | 108–109 | Capacity boundary and never-successful component health |
| R77 | 110 | Bounded deterministic health diagnostics |
| R78 | 111 | Closed metric-label enums |
| R79 | 112–113 | Optional Kafka logging administration and failure isolation |
| R80 | 114–115 | Bounded histories and scheduler records |
| R81 | 116 | Topology-churn state and metric reclamation |
| R82 | 117 | Worker, HTTP, and snapshot concurrency bounds |
| R83 | 118 | Failure-summary and snapshot retention bounds |
| R84 | 119 | Native-client queue bounds — final milestone |

## R08 activation

R08 was replaced by R36–R39 after R35 completed. The active sequence now keeps
readiness transitions, endpoint independence, container probing, and restart
reset in separate ownership boundaries. Each task declares explicit exclusions,
a fixed failure-mode checklist, and a four-file/500-line budget. Execution stops
at R09 for the next broad-phase replan.

## R09 activation

R09 was replaced by R40–R43 after the R39 milestone passed. The active sequence
separates state authority and generation fencing, concurrent snapshot safety,
fatal invariant termination, and startup reconciliation. Multi-instance role
policy remains owned by R70–R72. Execution stops at R10 for the next broad-phase
replan.

Before activation, each row must be expanded in `tasks.json` with its exact
files, exclusions, failure-mode checklist, and per-task budget. State migration
must preserve accepted R01–R27 work and must not mark R29–R34 complete merely
because draft code exists.

## R10 activation

R10 was replaced by R90 and R44–R46 after R43 completed. The active sequence
separates the descriptor model, bounded Kafka classification, Schema Registry
HTTP classification, and central recovery-policy decisions. The criterion-50
checkpoint review then added R91–R95 to harden review ownership, correct
transient Schema Registry recovery, add the missing partition-local unhealthy
state transition, preserve consumer failure propagation, and connect the
recovery policy to runtime Kafka operation boundaries. Startup retry timing
remains owned by R15. Major review findings now stop for human approval instead
of automatically starting a correction attempt.
Execution stops at R11 for the next broad-phase replan.

## R11 activation

R11 was replaced after R93 completed. Criteria 51–53 are assigned to the
reviewed R93 implementation because its bounded Kafka recovery integration,
consumer-replacement path, and sanitized completion handling already provide
the required evidence. The active HTTP concurrency sequence is:

1. R99 declares and validates the independent HTTP worker and waiting-queue
   configuration bounds.
2. R49 implements the fixed HTTP worker pool and bounded waiting queue and owns
   criteria 54–55.

Queue-saturation responses, socket deadlines, sanitized handler failures,
owned shutdown, and dependency-worker separation remain in criteria 56–60.
Execution stops at R12 so that phase is split before implementation.

## R12 activation

R12 was replaced after R49 completed. The active sequence separates configuration
from runtime behavior and preserves the original milestone at the lifecycle
boundary:

1. R47 declares and validates HTTP socket and shutdown time limits.
2. R50 owns criterion 56: bounded saturation rejection, overload accounting,
   and Kafka/Schema Registry health-state isolation.
3. R48 owns criterion 57: accepted-socket deadlines and production timeout
   propagation.
4. R51 owns criterion 58: uniformly bounded and sanitized handler failures.
5. R52 owns criteria 59–60: explicit HTTP resource ownership, bounded shutdown,
   and isolation from Kafka workers and dependency calls. R52 is the milestone.

Endpoint exposure, method policy, response redaction, TLS probing, and monitoring
deployment policy remain outside this sequence. Execution stops at R13 so that
security and enterprise-monitoring work is split before implementation.

## R13 activation

R13 was replaced after the R52 milestone passed. The activated sequence keeps
endpoint exposure, method policy, probe TLS, and monitoring deployment policy
in separate ownership boundaries:

1. R53 owns criteria 61–62: loopback listener defaults and the warning for an
   explicitly configured non-loopback plaintext listener.
2. R54 owns criteria 63–64: read-only HTTP method handling and bounded response
   redaction.
3. R55 owns criterion 65: certificate-verifying HTTPS liveness probing. R55 is
   the endpoint-security milestone.
4. R56 owns criteria 66–67: the development-only Prometheus/Grafana stack and
   the externally managed enterprise-monitoring contract.

The monitoring criteria were removed from the old R14 range. Execution stops
at R14 so long-lived startup clients, deterministic failures, and transient
initialization behavior can be split before implementation.

## R14 activation

R14 was replaced after R56 completed. The initial R57 attempt exceeded its
diff budget because it combined two independently testable invariants, so the
activated sequence now separates long-lived client ownership, deterministic
failure execution, and transient startup state:

1. R102 first removes one file of obsolete test assumptions about temporary
   Producer and urllib validation while retaining configuration coverage.
2. R57 owns criterion 68: validation through the actual long-lived Kafka
   administrative and Schema Registry clients, including alignment of the
   existing ordering and reconciliation tests.
3. R100 owns criterion 69: deterministic startup failure through typed,
   centralized recovery and bounded cleanup.
4. R58 owns criterion 70: live-but-unready transient initialization state,
   unhealthy dependency reporting, and retry of only the current failed stage.

Backoff progression, attempt overlap, partial-client cleanup, warmup capability
proof, and the production entry point remain outside this sequence. Execution
stops at R15 so criteria 71–75 can be split before implementation.
