# Cloud Canary Correctness Remediation Specification

## 1. Purpose

This specification defines the required behavior for correcting the reviewed
health reporting, readiness, topic reconciliation, metrics-cardinality, and
Schema Registry transport-security defects in Cloud Canary.

This document is the source of truth for the remediation work. Implementation
tasks and Ralph-loop instructions must refer to this specification and must not
silently change its requirements.

## 2. Scope

The remediation covers:

1. Kafka partition health evaluation.
2. Schema Registry health evaluation.
3. Recent failure-rate evaluation.
4. Readiness after warmup.
5. Fair, per-partition Kafka check scheduling.
6. Bounded worker-owned Kafka consumers.
7. Independent Schema Registry and topic-reconciliation scheduling.
8. Topic scale-down and recreation result handling.
9. Worker and consumer reconciliation after topic recreation.
10. Bounded aggregate and per-partition Prometheus metric cardinality.
11. HTTPS enforcement for Schema Registry.
12. Configuration validation for the new health and scheduling settings.
13. Separate process liveness, operational readiness, and dependency health.
14. A concurrency-safe, non-persistent internal health-state model.
15. Structured error classification and centralized recovery policy.
16. A bounded concurrent HTTP service with explicit lifecycle management.
17. Private-by-default endpoint exposure and enterprise monitoring integration.
18. Runtime-client startup validation and resilient transient initialization.
19. Portable separation of application, container, and local-development launch
    workflows.
20. Layered configuration with file-backed production secrets.
21. A minimal, reproducible, and provenance-labelled production image.
22. Static manager and observer roles for safe multi-instance topic ownership.
23. Topology-bounded in-memory state and native-client resource containment.
24. Automated regression tests for the behaviors defined here.

## 3. Non-Goals

The remediation must not:

- Redesign the canary's produce-and-consume protocol.
- Change the Avro message schema.
- Change the intended one-check-per-partition model.
- Replace Prometheus, the HTTP metrics server, or the Kafka client library.
- Add a JMX metrics subsystem; application and deployment-role visibility must
  use the existing Prometheus-compatible endpoint.
- Add a web framework or external WSGI/ASGI server dependency.
- Implement built-in Basic authentication, bearer-token authentication, or an
  application-managed enterprise identity system for metrics and probes.
- Add platform-specific production orchestration such as Helm, ECS, Nomad, or
  systemd deployment artifacts.
- Integrate directly with vendor-specific secret managers.
- Hot-reload credentials or other secret values in a running process.
- Use direct environment variables as the primary production secret-delivery
  mechanism.
- Adopt a distroless or scratch-based Python image in this remediation.
- Include network-diagnostic utilities in the production image.
- Implement automatic leader election, a distributed lease, or a lock topic for
  topic management.
- Perform unrelated refactoring or formatting changes.
- Access real credentials or require a live Kafka or Schema Registry service in
  automated tests.
- Add, upgrade, or replace dependencies unless separately approved.
- Create branches, commits, pull requests, releases, or deployments unless
  separately approved.
- Persist internal health decision state to a local file, database, Kafka topic,
  or other durable store.

## 4. Terminology

- **Partition check**: One seek, produce, and consume attempt for one Kafka
  partition.
- **Target cadence**: The configured start-to-start scheduling interval for a
  component. It is a scheduling objective, not a guarantee when capacity is
  insufficient.
- **Warmup check**: One of a partition's first `warmup.checks` completed
  attempts.
- **Post-warmup check**: The first or any later check for a partition after that
  partition has completed its configured warmup checks.
- **Staleness**: Elapsed wall-clock time since the last successful check for a
  component.
- **Component**: Either an individual Kafka partition or Schema Registry.
- **Deterministic Schema Registry failure**: An authentication, authorization,
  invalid HTTPS configuration, or certificate-validation failure that is not
  expected to recover through ordinary retries.
- **Transient Schema Registry failure**: A timeout, connection failure, or HTTP
  5xx response that may recover without configuration changes.
- **Topic manager**: The single canary instance authorized and configured to
  mutate the canary topic. It also performs normal canary checks.
- **Topic observer**: A canary instance that performs normal canary checks and
  topology discovery but never creates, deletes, or resizes the canary topic.

## 5. Configuration Requirements

The application must support these settings and defaults:

```ini
# Rolling failure-rate evaluation shared by Kafka and Schema Registry.
health.failure.window.checks=20
health.failure.minimum.checks=4
health.failure.threshold=0.5

# Maximum degraded or unhealthy components returned by /health.
health.max.diagnostic.components=20

# Kafka partition staleness thresholds.
health.kafka.degraded.after.seconds=60
health.kafka.unhealthy.after.seconds=300

# Schema Registry staleness thresholds.
health.sr.degraded.after.seconds=120
health.sr.unhealthy.after.seconds=300

# Schema Registry probe timeout. The existing check interval remains the
# target start-to-start cadence for SR probes.
sr.check.timeout.seconds=10

# Maximum permitted age of the internal scheduler heartbeat for liveness.
liveness.scheduler.max.staleness.seconds=10

# HTTP serving limits. These are independent of Kafka max.workers.
http.max.workers=4
http.request.queue.size=16
http.socket.timeout.seconds=5
http.shutdown.timeout.seconds=10

# Private-by-default endpoint exposure.
metrics.bind.address=127.0.0.1
metrics.port=8000
metrics.ssl.enabled=false

# Transient startup dependency retry policy.
startup.retry.initial.seconds=1
startup.retry.max.seconds=30
startup.retry.multiplier=2
startup.retry.jitter.factor=0.2

# Static topic-management role. Use exactly one manager when multiple canary
# instances monitor the same canary topic.
topic.management.mode=manage

# Optional best-effort Kafka log publishing.
log.topic.enabled=false
log.topic=cloud-canary-logs
log.topic.retention.ms=604800000
```

Production configuration may replace supported inline secret values with
file-backed sources:

```ini
[kafka]
sasl.username=KAFKA_API_KEY_IDENTIFIER
sasl.password.file=/run/secrets/cloud_canary_kafka_password

[schema_registry]
url=https://schema-registry.example
basic.auth.user.info.file=/run/secrets/cloud_canary_sr_credentials
```

The configuration loader must reject invalid values with an actionable error.
At minimum:

- `health.failure.window.checks` must be a positive integer.
- `health.failure.minimum.checks` must be a positive integer no greater than
  `health.failure.window.checks`.
- `health.failure.threshold` must be greater than `0` and no greater than `1`.
- `health.max.diagnostic.components` must be a positive integer.
- Each degraded staleness threshold must be greater than its component's
  configured check interval.
- Each unhealthy staleness threshold must be greater than its corresponding
  degraded threshold.
- `sr.check.timeout.seconds` must be greater than `0` and less than
  `sr.check.interval.seconds`.
- `max.workers` must remain a positive integer and defines the maximum Kafka
  partition-check concurrency, worker-thread count, and worker-owned consumer
  count.
- `liveness.scheduler.max.staleness.seconds` must be a positive number and must
  be greater than the scheduler's normal heartbeat interval.
- `http.max.workers` and `http.request.queue.size` must be positive integers.
- `http.socket.timeout.seconds` and `http.shutdown.timeout.seconds` must be
  positive numbers.
- `metrics.bind.address` must be a valid address for the deployment environment.
- Metrics TLS configuration must continue to require a valid certificate and
  key when enabled.
- `startup.retry.initial.seconds` and `startup.retry.max.seconds` must be
  positive, and the initial delay must not exceed the maximum delay.
- `startup.retry.multiplier` must be greater than `1`.
- `startup.retry.jitter.factor` must be greater than or equal to `0` and less
  than `1`.
- Each required secret must have exactly one configured source: its inline
  value or its corresponding `.file` setting. An optional secret may have no
  source, but must not have both sources.
- A configured secret file must be readable and resolve to a non-empty value.
- `log.topic.enabled` must be a boolean.
- When log publishing is enabled, `log.topic` must be a valid Kafka topic name
  different from the canary check topic, and `log.topic.retention.ms` must be a
  positive integer.

Kafka and Schema Registry use the same rolling failure-rate algorithm, but they
must use separate histories and separate staleness thresholds.

## 6. Operational Endpoint Model

The service must expose three endpoints with distinct responsibilities:

| Endpoint | Responsibility |
|---|---|
| `/live` | Process and internal scheduling-control-plane liveness |
| `/ready` | Initialization completion and ability to perform monitoring work |
| `/health` | Kafka, Schema Registry, and monitoring-capacity health |

External Kafka or Schema Registry health must not determine process liveness.
The endpoint implementations must consume concurrency-safe application-state
snapshots rather than partially updated mutable state.

### 6.1 Liveness

`/live` must return HTTP 200 only when:

- The HTTP service can respond.
- The monotonic scheduler heartbeat is no older than
  `liveness.scheduler.max.staleness.seconds`.
- Graceful shutdown has not started.
- No fatal internal invariant or terminal reconciliation state has been
  recorded.

Kafka check results, Kafka staleness, Kafka failure rate, SR probe results, and
SR staleness must not affect `/live`.

The liveness response must include the current liveness state, scheduler
heartbeat age, and shutdown state without exposing credentials or internal
exception details.

Container and orchestrator liveness probes must use `/live`, not `/health` or
`/ready`. The supplied probe mechanism must honor the configured metrics port
and HTTP/HTTPS mode rather than assuming `http://localhost:8000`.

### 6.2 Readiness

Before readiness is first achieved, `/ready` must require:

- Topic creation or reconciliation has completed and the topic is verified.
- The monotonic scheduler is running.
- The complete worker-owned consumer set is ready.
- Initial Schema Registry validation succeeded.
- Every expected Kafka partition completed its configured warmup checks.
- Every expected Kafka partition subsequently recorded at least one successful
  post-warmup check.

After readiness is achieved, external Kafka or Schema Registry degradation or
outage must not revoke readiness. Those conditions belong to `/health`.

Readiness must return to HTTP 503 for internal transitions or inability to
perform monitoring work, including:

- Destructive topic recreation is in progress.
- Worker or consumer state is being rebuilt.
- A newly expected partition has not completed initialization.
- The scheduler is paused or its heartbeat is stale.
- A fatal reconciliation or internal state has been recorded pending process
  termination.
- Graceful shutdown has started.

Readiness diagnostics must identify the incomplete initialization stage or
partitions without exposing credentials or raw internal exception details.

### 6.3 Dependency and Capacity Health

`/health` must report the current state of:

- Every expected Kafka partition.
- Schema Registry.
- Kafka scheduling capacity, including overdue partition work.

It may return HTTP 503 while both `/live` and `/ready` return HTTP 200. This is
the expected behavior when the canary is functioning correctly and reporting an
external dependency or monitoring-capacity problem.

The health response must be efficient for large clusters and must not include
an unbounded diagnostic payload. It must identify the components responsible
for degraded or unhealthy state while allowing detailed per-partition state to
remain available through metrics.

The response must include at most `health.max.diagnostic.components` degraded
or unhealthy component entries. It must order entries by health severity first
(`unhealthy` before `degraded`), then by the applicable staleness or overdue
duration from greatest to least, and finally by stable component identity for
deterministic ties. A component with no successful observation ranks ahead of
components with a finite staleness value at the same severity. The response
must include the total affected-component count, returned-component count, and
truncated-component count. Healthy components omitted from the response remain
fully visible through aggregate and per-partition metrics.

### 6.4 Bounded HTTP Service

The application must serve `/metrics`, `/live`, `/ready`, and `/health` through
a bounded concurrent HTTP service implemented with the Python standard library.

- Request concurrency must be limited to `http.max.workers`.
- Waiting request work must be limited to `http.request.queue.size`.
- HTTP workers and their queue must be independent of Kafka check workers and
  Kafka scheduling queues.
- The server must not create unbounded request threads or tasks.
- `/live` must remain a minimal constant-time snapshot operation.
- `/ready` and `/health` must evaluate immutable `HealthStateStore` snapshots.
- `/metrics` may serialize the Prometheus registry but must not block unrelated
  requests when another HTTP worker is available.
- HTTP handlers must not acquire Kafka consumers, perform Kafka or SR network
  calls, or hold the state-store mutation lock while serializing or writing a
  response.
- Accepted connections must use `http.socket.timeout.seconds` to bound slow
  request reads and response writes.
- Queue saturation must not create more HTTP workers. It must increment a
  bounded overload metric and reject excess work with a sanitized bounded
  response when possible.
- Queue saturation must not directly change Kafka or SR health state.
- Handler failures must return sanitized JSON and must not expose raw Python
  exception text, tracebacks, credentials, or internal objects.

The application must retain explicit ownership of the HTTP server, request
queue, and worker pool. They must not be left as an unmanaged daemon-only
resource.

HTTP shutdown must:

1. Mark liveness and readiness unavailable.
2. Stop accepting new request work.
3. Drain or terminate bounded in-flight HTTP work for no longer than
   `http.shutdown.timeout.seconds`.
4. Close the listening socket and release HTTP worker resources.

Failure to finish HTTP draining within the shutdown bound must be logged safely
and must not block the remaining process cleanup indefinitely.

### 6.5 Endpoint Exposure and Security

The metrics and probe service must be private by default.

- The default bind address must be `127.0.0.1`.
- Production container or orchestrated deployments that require cross-process
  scraping must explicitly select a non-loopback bind address.
- A non-loopback bind with TLS disabled must emit a prominent warning requiring
  an access-restricted private network, firewall, network policy, reverse proxy,
  or service mesh.
- The endpoint must never be documented or deployed as directly
  internet-accessible.
- Plain HTTP is acceptable only on an explicitly trusted and restricted network.
- Traffic crossing an untrusted network must use TLS at the canary, reverse
  proxy, ingress, sidecar, or service mesh.
- Application TLS provides encryption but must not be described as client
  authentication unless mTLS or an authenticated infrastructure component is
  present.
- Authentication and caller authorization, when required, must be supplied by
  infrastructure such as mTLS, a service mesh, reverse proxy, authenticated
  ingress, firewall, or network policy.

All endpoint responses must be read-only, sanitized, and bounded.

- `GET` must be supported; `HEAD` may be supported.
- Mutation methods such as `POST`, `PUT`, `PATCH`, and `DELETE` must be rejected.
- CORS must remain disabled unless a separately approved browser use case
  requires it.
- Responses must not expose credentials, authenticated URLs, raw exceptions,
  tracebacks, Kafka message contents, configuration contents, certificate
  paths, or internal filesystem paths.

The configuration-aware container probe must target `/live`, use the configured
port and HTTP/HTTPS mode, validate TLS certificates, and must not disable
certificate verification as a fallback or place credentials in process
arguments.

### 6.6 Enterprise Monitoring and Bundled Local Stack

The canary exports Prometheus-compatible metrics using a pull model. It must not
require or accept a Prometheus server URL and must not push metrics to the
bundled or enterprise Prometheus service.

The repository's Prometheus and Grafana deployment is for local development and
testing only. It must:

- Be documented explicitly as unsuitable for production.
- Bind published host ports to loopback by default.
- Avoid unnecessary Prometheus administrative APIs.
- Avoid presenting development credentials such as `admin/admin` as
  production-safe.

Production deployments must supply an externally managed
Prometheus-compatible scraper and visualization system. The production
operator is responsible for retention, high availability, alerting, access
control, authentication, network isolation, audit requirements, and dashboard
security.

The canary remains responsible for stable exposition, bounded label
cardinality, configurable bind address and port, optional transport TLS,
sanitized output, and configuration-aware probes.

### 6.7 Portable Runtime and Development Tooling

The canonical non-container application entry point must be:

```text
python -m src.main
```

The application must not require a repository-specific virtual-environment path
such as `.venv/bin/python`. Users are responsible for selecting and activating
their supported Python environment.

The versioned OCI image is the production runtime boundary supplied by this
repository. Production examples must use an explicit image version rather than
`latest` and must keep credentials in a read-only deployment-provided mount or
another approved secret-delivery mechanism.

The bundled Prometheus and Grafana Compose stack must remain an optional local
development workflow separate from application startup. Development helpers
may wrap explicit Compose commands, but they must:

- Check that the requested container runtime and Compose command are already
  available.
- Never install, start, stop, or reconfigure Colima, Docker Desktop, Podman, or
  another external runtime.
- Use repository-relative paths.
- Start and clean up only resources explicitly requested through that helper.
- Avoid hardcoded Python virtual-environment paths.
- Report missing prerequisites with actionable instructions.

The existing `run.sh`, which combines Colima management, local monitoring, and
application execution, must be removed. No compatibility wrapper is required
because the project has no active users.

Documentation must provide separate commands for:

- Running the Python application.
- Running the versioned OCI image.
- Starting and stopping the optional local monitoring stack.

Platform-specific runtime setup may be documented, but project scripts must not
manage external system services or global configuration.

### 6.8 Production Image and Supply Chain

The production image must contain only the Python runtime, required application
and native libraries, trusted CA certificates, application code, and the
configuration-aware Python liveness probe.

The production image must not install or depend on `curl`, `dnsutils`,
`nslookup`, `dig`, `ping`, or other interactive network-diagnostic tools.
Operational diagnostics must use platform-provided ephemeral debugging
facilities or a separately approved debug image outside the production runtime.

The image must:

- Continue using a base image pinned by digest and document its human-readable
  tag and update procedure.
- Run as a non-root numeric user.
- Support a read-only root filesystem and dropped Linux capabilities.
- Require no runtime writes under `/app` for bytecode, caches, configuration,
  state, or other artifacts.
- Use the configuration-aware Python `/live` probe rather than an external
  command-line HTTP client.
- Perform no DNS diagnostics or system configuration changes during ordinary
  startup.
- Use direct `exec` process semantics so termination signals reach the Python
  process.
- Print no secret-derived configuration during initialization.

Direct dependencies must remain human-reviewable, while production dependency
installation must use a fully resolved lock file containing transitive versions
and hashes. Production installation must use hash verification. Lock generation
or refresh is explicit dependency-update work and must not occur silently during
an unrelated Ralph iteration.

OCI metadata must contain accurate values for source, version, revision, and
creation time. Placeholder repository metadata is prohibited. Application and
image versions must derive from one authoritative release version and must be
validated for consistency during the build or release process. Production
examples must use explicit versioned image references.

Supported image architectures must be documented only after the pinned base
image, Python dependencies, native `confluent-kafka` requirements, application
startup, and probe behavior have been validated on each claimed architecture.

The release process must support dependency hash verification and container
vulnerability scanning. SBOM generation and image signing should be performed
by the enterprise build and release platform where available; this project must
not implement its own signing service.

## 7. Health-State Model

The `/health` endpoint has three states:

| State | HTTP status | Meaning |
|---|---:|---|
| `healthy` | 200 | No component currently meets a degraded or unhealthy condition. |
| `degraded` | 503 | At least one component is degraded and no component is unhealthy. |
| `unhealthy` | 503 | At least one component is unhealthy. |

State precedence is:

```text
unhealthy > degraded > healthy
```

The overall state must be the worst state produced by any expected Kafka
partition or by Schema Registry. A healthy component must not conceal an
unhealthy or degraded component.

An expected component with no recorded success in the current process must be
`unhealthy`: the canary has not yet demonstrated that it can monitor that
component. This applies to every newly discovered Kafka partition and to Schema
Registry after startup or restart. For Kafka, a warmup success is sufficient to
establish partition health, but it does not satisfy the separate post-warmup
readiness requirement. This initial unhealthy state must not make `/live` fail;
`/ready` remains HTTP 503 until all readiness conditions are met.

### 7.1 Kafka Partition Staleness

Kafka staleness must be evaluated independently for every expected partition.

- Last success less than 60 seconds ago by default: no staleness degradation.
- Last success from 60 through 300 seconds ago by default: `degraded`.
- Last success more than 300 seconds ago by default: `unhealthy`.

Configured Kafka thresholds replace the defaults above. Health aggregation must
use the worst partition state, not the freshest partition timestamp.

### 7.2 Kafka Failure Rate

The application must retain the most recent configured number of completed
check results independently for each partition.

- Do not apply the failure-rate rule until the configured minimum number of
  observations exists for that partition.
- Calculate the rate as failed checks divided by completed checks in that
  partition's current rolling window.
- A rate strictly greater than `health.failure.threshold` makes that partition
  `degraded` unless it is already `unhealthy` due to staleness.
- Evaluate the worst partition result. Results from healthy partitions must not
  dilute failures on another partition.
- Rolling history may reset when the process restarts.

### 7.3 Schema Registry Health

Schema Registry health must be evaluated independently from Kafka health.

- A successful scheduled probe records a successful SR observation and updates
  the last-success timestamp.
- A transient SR failure records a failed observation and makes SR `degraded`
  immediately.
- A deterministic SR failure makes SR `unhealthy` immediately.
- SR staleness from 120 through 300 seconds by default is `degraded`.
- SR staleness greater than 300 seconds by default is `unhealthy`.
- Configured SR thresholds replace these defaults.
- A rolling SR failure rate strictly greater than
  `health.failure.threshold`, after the configured minimum observations, makes
  SR `degraded` unless an unhealthy condition already applies.

Lifetime success and failure totals must not be used to infer the current SR
state. In particular, historical successes must not conceal the latest failed
probe.

### 7.4 Health Response Diagnostics

The JSON response must expose enough information to explain the selected state,
including:

- Overall state and message.
- The component or components responsible for degraded or unhealthy state.
- Component staleness where available.
- Rolling observation count and failure rate where available.
- Current Schema Registry state.

Diagnostic field names may follow the existing response style, but their values
must use the behavior defined in this specification.

### 7.5 Authoritative Internal State

The application must use a dedicated, concurrency-safe `HealthStateStore` as
the authoritative internal source for `/live`, `/ready`, and `/health`.

The store must own at least:

- Per-partition rolling results, last attempt, last success, warmup progress,
  post-warmup success, and latest bounded failure classification.
- Schema Registry rolling results, last attempt, last success, and latest
  bounded failure classification.
- The expected partition set and current process-local topic generation.
- Scheduler heartbeat, readiness latch, reconciliation state, shutdown state,
  and fatal internal state.

The state store must:

- Encapsulate mutable state behind narrow update methods.
- Protect mutation with one explicit concurrency mechanism.
- Produce immutable, bounded snapshots for HTTP evaluation and diagnostics.
- Never hold its mutation lock during Kafka, Schema Registry, logging, metrics
  exposition, JSON serialization, or other network or blocking I/O.
- Accept injected monotonic and wall clocks for deterministic testing.
- Store bounded classifications and summaries rather than raw exception
  objects, tracebacks, messages, credentials, or Kafka records.

Prometheus metrics are external time-series output and must not be read back as
the application's authoritative current decision state. A result may become
visible to the internal store and Prometheus at slightly different instants;
endpoint decisions must use the internal store.

### 7.6 Topic Generations and Stale Results

Every dispatched Kafka partition check must carry the current process-local
topic-generation identifier. The state store must reject a completion result
whose generation no longer matches the current generation.

Successful topic recreation must atomically:

1. Advance the process-local topic generation.
2. Replace the expected partition set.
3. Reset Kafka warmup and readiness state.
4. Prevent old in-flight completions from affecting the new topic incarnation.

Broker metadata, not the process-local generation, is authoritative for topic
existence, partition count, and restart recovery.

### 7.7 Restart and State-Loss Policy

`HealthStateStore` must not be persisted. Every process start begins with:

- Empty Kafka and SR rolling histories.
- No component last-success timestamp.
- Reset Kafka warmup and post-warmup state.
- Readiness set to false.
- A fresh process-local topic generation.
- Schema Registry observation state unknown until a live probe completes; this
  maps to `unhealthy` health until the first success.

The recent failure-rate window is intentionally lost across restart. Historical
retention belongs to Prometheus, and current topology recovery belongs to
broker metadata.

Startup must reconcile real broker state rather than attempting to restore an
interrupted administrative transaction from local memory. At minimum:

- In `manage` mode, a missing canary topic is recreated and verified.
- In `observe` mode, a missing canary topic keeps the process live, not ready,
  and unhealthy while read-only topology discovery waits for the manager.
- An existing topic with the expected count is verified and accepted.
- In `manage` mode, an existing topic with an unexpected count is reconciled.
- In `observe` mode, an unexpected count keeps the process live, not ready, and
  unhealthy without issuing a mutation; local state is rebuilt only after the
  configured topology is observed and verified.
- Transiently inaccessible metadata enters the bounded startup retry policy and
  must never be treated as healthy.
- Metadata that remains logically inconsistent or ambiguous after a successful
  response and required verification is a fatal reconciliation failure rather
  than an assumed healthy state.

During restart initialization, `/ready` must remain HTTP 503 and `/health` must
not infer health from observations made by a previous process. Process uptime
or start-time metrics must make the reset externally observable.

### 7.8 State Invariants and Corruption Handling

The state store must validate structural invariants, including:

- Partition histories belong only to the current expected partition set.
- History length does not exceed the configured rolling window.
- Warmup progress is within valid bounds.
- Readiness cannot be true before required initialization completes.
- In-flight results cannot update a different topic generation.
- State precedence remains `unhealthy > degraded > healthy`.

An invariant violation must fail closed. The application must record a
sanitized fatal internal state, make `/live` and `/ready` return HTTP 503, stop
accepting new checks, perform bounded cleanup, and terminate with a non-zero
exit status. It must not silently clear or reconstruct inconsistent state.

### 7.9 Memory and Resource Containment

Long-lived application memory may scale linearly with the latest verified
topology, but it must not grow with process uptime, completed-check count,
retry count, missed schedules, or repeated topology churn. This remediation
must not introduce a configured maximum partition count; complete coverage of
the current broker-count-derived topology remains required.

For `P` current expected partitions, rolling window `W`, and `M =
min(P, max.workers)`, the following invariants must hold:

- Partition state records are at most `P`.
- Kafka rolling-result entries are at most `P * W`; the independent Schema
  Registry history is at most `W`.
- Scheduler records are at most `P`, with at most one record for a partition.
- Live Kafka workers, worker-owned consumers, and concurrent Kafka checks are
  each at most `M`.
- A health snapshot contains derived current state, not copies of rolling
  histories. Completed HTTP requests must not leave snapshots retained.
- Concurrently materialized HTTP snapshots are limited by `http.max.workers`,
  and each response contains at most `health.max.diagnostic.components`
  detailed component entries.
- Per-partition Prometheus children remain a linear function of `P` and are
  removed with obsolete partition and state-label children as defined in
  Section 12.

Per-partition histories must use fixed-length storage whose maximum length is
`health.failure.window.checks`. Completed observations displaced from that
window must become unreachable from application state. Removed partitions must
be deleted from health, scheduling, readiness, and metrics state during the
same atomic topology replacement; state from prior topic generations must not
be retained.

`FailureDescriptor.safe_summary` must be sanitized and truncated to at most 512
characters before it enters application state. HTTP serialization must not be
the first or only truncation point.

The shared check producer, optional log producer, and worker-owned consumers
must use explicit, conservative finite native-client message and byte-buffer
limits defined as documented application constants. The application must not
add a Python retry buffer or shadow queue around a full native-client queue.
Queue-full behavior must produce the applicable bounded check failure or
best-effort log-publishing failure defined elsewhere in this specification.

Automated tests must demonstrate that the invariants above remain true across
thousands of completed checks, repeated failures and retries, and repeated
partition scale-up and scale-down. A topology reduction must return all
topology-dependent collection and metric-child counts to bounds derived from
the new `P`.

## 8. Failure Classification and Recovery

### 8.1 Structured Failure Descriptor

Every handled operational failure must be converted at its boundary into a
bounded structured descriptor. The descriptor must contain:

- `component`: the affected bounded component type, such as Kafka partition,
  Schema Registry, topic administration, scheduler, HTTP server, or internal
  state.
- `phase`: the bounded operation in progress, such as assignment, seek,
  produce, consume, SR probe, metadata fetch, topic deletion, topic creation,
  verification, state update, or shutdown.
- `category`: one value from the bounded category set below.
- `recoverability`: one value from the bounded recoverability set below.
- `code`: an optional stable library, protocol, or application error code.
- `safe_summary`: a sanitized human-readable summary.

Required failure categories are:

```text
NETWORK
BROKER_SERVICE
AUTHENTICATION
AUTHORIZATION
TLS_CERTIFICATE
CONFIGURATION
SERIALIZATION
CLIENT_STATE
CAPACITY
INTERNAL
UNKNOWN
```

Required recoverability values are:

```text
TRANSIENT
DETERMINISTIC
INTERNAL_FATAL
UNKNOWN
```

Required failure-phase values are:

```text
ASSIGNMENT
SEEK
PRODUCE
CONSUME
SCHEMA_REGISTRY
METADATA_FETCH
TOPIC_CREATE
TOPIC_EXPAND
TOPIC_DELETE
TOPIC_VERIFY
CONSUMER_CREATE
CONSUMER_REPLACE
SCHEDULER
STATE_UPDATE
HTTP_REQUEST
STARTUP
SHUTDOWN
UNKNOWN
```

The closest operation boundary must be used. `UNKNOWN` is permitted only when
the failure cannot be assigned to another phase after typed classification; it
must not be used as a convenience fallback for known operations.

Raw exception objects, tracebacks, credentials, authenticated URLs, Kafka
message values, and unbounded exception text must not be retained in
`HealthStateStore` or used as metric labels.

### 8.2 Centralized Recovery Policy

Classification and recovery action must be separate. A centralized policy must
map a structured failure plus its startup or runtime context to one of these
bounded actions:

```text
RECORD_AND_CONTINUE
REPLACE_CONSUMER
MARK_COMPONENT_UNHEALTHY
FAIL_STARTUP
TERMINATE_PROCESS
```

Recovery decisions must not be independently reimplemented across unrelated
`except` blocks.

The default policy must be:

| Failure | Required action |
|---|---|
| DNS, connection, or ordinary network timeout | Record failure and retry at the next target cadence |
| Transient Kafka broker/service response | Record failure and retry at the next target cadence |
| Schema Registry HTTP 5xx | Mark SR degraded and retry at the next SR cadence |
| Authentication or authorization failure at runtime | Mark the affected component unhealthy immediately and continue bounded probes |
| Certificate-validation failure at runtime | Mark the affected component unhealthy immediately and continue bounded probes |
| Consumer invalid or uncertain state | Record the failure, close the worker-owned consumer, and replace it before more work |
| Serialization failure in the fixed canary protocol | Treat as an internal fatal failure and terminate after bounded cleanup |
| Invalid startup configuration | Fail startup before authenticated operational work begins |
| Scheduler or state invariant violation | Terminate after bounded cleanup |
| Topic reconciliation failure | Terminate after bounded cleanup |
| Capacity overload | Mark capacity degraded without creating workers beyond the configured bound |
| Unknown external failure | Record as unknown and retry without claiming the component healthy |
| Unknown internal failure | Treat as internal fatal rather than silently continuing |

Startup connectivity and security validation may fail startup for deterministic
authentication, authorization, TLS, or configuration failures even though an
equivalent failure discovered during established runtime monitoring remains
observable through bounded probes.

### 8.3 Classification Rules

- Prefer typed Kafka error codes, HTTP status codes, and exception types.
- Do not use error-message string matching when a typed code or status is
  available.
- Kafka client-local errors must not all be classified as network errors.
- Schema Registry 4xx responses must be distinguished by semantics rather than
  grouped into one network category.
- Stable detailed codes may appear in sanitized logs or bounded diagnostics,
  but Prometheus labels must use only bounded enums.
- Health-state summaries must be sanitized before storage.
- Classification functions and recovery-policy functions must be independently
  testable without live external services.

### 8.4 Startup Validation and Retry State Machine

Startup connectivity and security validation must use the actual long-lived
clients that will perform runtime work.

- Kafka metadata validation must use the long-lived administrative client and
  its complete runtime Kafka configuration.
- Schema Registry validation must use the long-lived
  `SchemaRegistryClient` and its complete runtime configuration.
- The application must not create a temporary Kafka producer solely for
  connectivity validation.
- The application must not create a separate `urllib` client for Schema
  Registry validation.
- The shared producer and serializers must be created once for operational use;
  complete produce, schema, and consume permissions are validated through
  warmup before readiness.
- Partially initialized long-lived clients must be owned explicitly and cleaned
  up during fatal startup failure or shutdown.

Required initialization states are:

```text
STARTING
VALIDATING_CONFIGURATION
CONNECTING_KAFKA
WAITING_FOR_KAFKA
CONNECTING_SCHEMA_REGISTRY
WAITING_FOR_SCHEMA_REGISTRY
RECONCILING_TOPIC
STARTING_WORKERS
WARMING_UP
READY
FATAL_CONFIGURATION
FATAL_RECONCILIATION
SHUTTING_DOWN
```

Deterministic startup failures, including invalid configuration,
authentication, authorization, and certificate validation, must fail startup
with a non-zero exit status after bounded cleanup.

Transient startup failures, including DNS, connection, timeout, and service 5xx
failures, must keep the process live but not ready. `/health` must expose the
unavailable dependency and retry state. The application must retry the current
initialization stage without starting partition checks or queueing overlapping
initialization attempts.

Transient retries must use monotonic deadlines and exponential backoff:

```text
initial delay = startup.retry.initial.seconds
next delay = min(previous delay * startup.retry.multiplier,
                 startup.retry.max.seconds)
jitter = independently applied within plus or minus
         startup.retry.jitter.factor of the calculated delay
```

With defaults, the unjittered progression is:

```text
1s -> 2s -> 4s -> 8s -> 16s -> 30s cap
```

The retry delay must reset after the current initialization stage succeeds.
Retry attempts, current delay, waiting dependency, and time spent in the current
initialization state must be observable through bounded metrics and sanitized
diagnostics.

A failure after a destructive topic operation has begun remains governed by the
fatal reconciliation requirements in Section 11 and must not be converted into
an indefinite startup retry.

## 9. Scheduling and Concurrency

### 9.1 Kafka Partition Scheduler

Kafka checks must use fair, per-partition scheduling rather than synchronized
batch cycles.

- `check.interval.seconds` is the target start-to-start cadence for each
  partition.
- Every expected partition must have its own next-due time.
- Scheduling deadlines and elapsed durations must use a monotonic clock.
- The scheduler must select the oldest-due partition first.
- A partition must not have more than one check in flight.
- Initial due times should be distributed across the check interval to avoid a
  startup request burst.
- If a check finishes after one or more later occurrences were due, missed
  occurrences must be coalesced. They must not be replayed as a catch-up burst.
- Insufficient capacity must delay the oldest pending work fairly; it must not
  permanently favor low-numbered or otherwise early partitions.
- The scheduler must not maintain an accumulating ready-work queue. It must
  select the oldest eligible partition from the single current scheduling
  record owned by each expected partition, with deterministic tie-breaking.
- A due or missed occurrence must update that partition's existing scheduling
  record rather than append another record.
- The scheduler must expose pending work, in-flight work, oldest overdue age,
  and effective partition-coverage duration as metrics.

The configured interval is a target rather than a guarantee. The application
must emit an actionable warning or degraded-capacity signal when actual
partition coverage cannot keep pace with the configured cadence.

Kafka scheduling capacity becomes `degraded` when the oldest due partition is
overdue by at least one complete `check.interval.seconds`. It returns to healthy
when the oldest overdue age falls below that boundary. Capacity backlog does
not introduce a separate direct `unhealthy` boundary; if it persists, the
per-partition last-success rules make affected partitions and therefore overall
health unhealthy. The inclusive degradation boundary must use monotonic time.

### 9.2 Kafka Workers and Consumers

The application must use a fixed set of Kafka check workers. Each worker owns
one long-lived consumer exclusively.

```text
active workers = min(current partition count, max.workers)
live consumers = active workers
maximum concurrent partition checks = active workers
```

- There is no independently configurable consumer count.
- A worker's consumer must never be used by another worker or concurrently.
- A worker must reassign its consumer to the partition selected by the
  scheduler before executing that partition's check.
- Partition health and scheduling state belong to application state, not to a
  particular consumer, because workers are reused across partitions.
- A consumer left in an uncertain or invalid state must be closed and replaced
  before that worker accepts more work.
- Pool exhaustion or backlog must not create additional workers or consumers
  beyond the configured bound.
- Shutdown must stop dispatch and bound the wait for in-flight work. Each worker
  that releases ownership by the shutdown deadline must close its own consumer
  before terminating.
- Signal handling must attempt one atomic, first-write-wins publication claim as
  its first state-mutating operation, using state allocated before signal
  delivery. The signal that wins that atomic claim owns the shutdown request;
  an earlier handler invocation that is re-entered before reaching the claim is
  not considered published. After winning, the handler records the monotonic
  start time and publishes one immutable request and deadline. The handler must
  not acquire a dispatch-owned lock, log, access a `threading.Event`, or perform
  dependency I/O. Later signals must not replace the winning request or extend
  its deadline.
- A dispatch transaction is linearized when it enters the dispatch guard. A
  transaction that entered before the shutdown request is in-flight work and may
  finish only its bounded in-memory scheduling and executor-queue submission. No
  transaction entering after the shutdown request may submit work, and native or
  dependency operations must never execute inside the dispatch guard.
- A worker permanently blocked in a native or client call must not have its
  consumer closed concurrently from another thread. After the shutdown deadline,
  the process must terminate through the daemon execution boundary; operating
  system process teardown releases resources owned by the wedged call. This is
  an exceptional fatal-termination path, not graceful cleanup or permission to
  resume dispatch.

The effect of shutdown on `/live` and `/ready` is defined in Sections 6.1 and
6.2 and is implemented with the endpoint contracts, independently of the
bounded process-shutdown mechanism above.

The producer remains shared across workers, subject to the Kafka client's
documented thread-safety guarantees.

### 9.3 Independent Scheduled Operations

Kafka partition checks, Schema Registry probes, and topic reconciliation must
have independent schedules and execution lanes.

- A lightweight monotonic scheduler may coordinate due times, but it must not
  perform blocking network operations itself.
- Kafka partition work is dispatched only to Kafka check workers.
- Schema Registry probes use a dedicated execution lane with at most one probe
  in flight.
- In `manage` mode, topic reconciliation uses a dedicated administrative
  execution lane with at most one reconciliation in flight. In `observe` mode,
  the same cadence runs read-only topology discovery and must never issue a
  topic mutation.
- Slow or backlogged Kafka checks must not delay an SR probe or ordinary topic
  reconciliation.
- Missed SR and reconciliation occurrences must be coalesced rather than
  queued.
- The first SR probe must be scheduled immediately at startup; later probes use
  `sr.check.interval.seconds` as their target start-to-start cadence.
- A normal SR probe must be bounded by `sr.check.timeout.seconds`.
- `partition.sync.interval.seconds` is the reconciliation target
  start-to-start cadence.

Destructive topic recreation is an exclusive administrative operation. It may
pause Kafka dispatch and drain or cancel bounded in-flight Kafka work as defined
in Section 11.

## 10. Readiness and Warmup

`/ready` must return `ready` only after every expected partition has recorded at
least one successful post-warmup check.

- Exactly the first `warmup.checks` completed attempts for each partition are
  that partition's warmup checks.
- Completing the configured number of warmup checks is not sufficient by
  itself; every partition must subsequently succeed.
- A failed post-warmup check must not satisfy readiness for that partition.
- Once readiness has been achieved, external Kafka or Schema Registry health
  changes must not revoke it. Internal transitions and inability to perform
  monitoring work follow Section 6.2.
- `warmup.checks=0` means each partition's first check is its first eligible
  post-warmup check.
- Adding a partition makes the application not ready until the new partition
  satisfies the same warmup and post-warmup-success requirements.
- Topic recreation resets Kafka readiness because the monitored topic
  incarnation changed.

Readiness state must not depend on an undocumented fixed value such as ten
checks or cycles.

## 11. Topic Reconciliation

### 11.1 Broker-Count-Derived Canary Topic

The desired canary-topic partition count must equal the latest successfully
verified Kafka broker count. A successful metadata response with fewer than one
broker is inconsistent and must not be used as a desired topology.

The manager must apply the existing topology policy:

- On creation or recreation, create one partition per verified broker.
- Set replication factor to `min(verified broker count, 3)`.
- Set `retention.ms` to `86400000` milliseconds for a newly created or
  recreated canary topic.
- Increase the partition count in place when broker count increases.
- Delete and recreate the canary topic when broker count decreases because
  Kafka cannot reduce a topic's partition count in place.

Partition-count reconciliation is the enforced topology dimension in this
remediation. An already existing topic with the expected partition count is
accepted; this remediation must not silently rewrite its replication factor or
retention configuration. Creation settings apply again when scale-down requires
recreation.

One partition per broker increases the opportunity for broad broker-leader
coverage but does not guarantee that every broker leads a partition. Kafka's
observed metadata remains authoritative for actual placement; the canary must
not claim guaranteed per-broker coverage from partition count alone.

### 11.2 Static Topic-Management Roles

`topic.management.mode` must accept exactly `manage` or `observe` and default to
`manage` so a standalone deployment retains topic-management behavior.

A multi-instance deployment for the same canary topic must configure exactly
one instance as `manage` and every other instance as `observe`. This uniqueness
is a deployment invariant: the application is not required to detect another
manager, and configuring multiple managers for one canary topic is unsupported.

The manager instance must:

- Perform the same partition checks and Schema Registry probes as an observer.
- Own all create, partition-increase, delete, and recreate operations for the
  canary topic.
- Run the mutating reconciliation lane and apply all verification, pause,
  rebuild, readiness-reset, and failure behavior in this section.
- Use a principal whose administrative permissions are restricted to the
  canary topic and, only when optional Kafka log publishing is enabled, the
  limited log-topic permissions defined in Section 11.6.

An observer instance must:

- Discover broker metadata on `partition.sync.interval.seconds` without issuing
  a mutating topic-administration request.
- Continue normal checks while the observed topic and topology are stable.
- Pause Kafka dispatch while the topic is absent or its topology is unstable.
- Rebuild its local workers, consumers, scheduling state, metrics, and readiness
  state after observing a replacement or changed verified topology.
- Report the topology mismatch through dependency health while waiting for the
  configured topology, without treating the absence of mutation privileges as
  an internal fatal error.
- Use a principal that needs only the describe, produce, and consume permissions
  required for normal canary operation and, only when optional Kafka log
  publishing is enabled, produce permission for the configured log topic.

Manager loss must not stop observers from monitoring an already stable topic.
While no manager is running, a missing or mismatched topic remains unreconciled
and must not be reported as healthy. Failover is an operator action: change one
observer to `manage` and restart it. The application must not implement leader
election, a distributed lease, a lock topic, or automatic role promotion.

All instances in one logical canary deployment must target the same Kafka
cluster and canary topic. They derive the target partition count independently
from verified broker metadata; there is no separately configured desired
partition count.

### 11.3 Verified Results

Topic reconciliation must report observed, verified state rather than desired
state.

- A delete failure, deletion-propagation timeout, recreation failure, metadata
  verification failure, or unexpected final partition count is a reconciliation
  failure.
- A failed delete-and-recreate operation must not return the requested lower
  partition count as if it succeeded.
- The replacement topic must be visible in broker metadata with the expected
  partition count before reconciliation is considered successful.
- The in-process current partition count and the exported partition-count metric
  must be updated only from verified reconciliation results.

### 11.4 Failure Behavior

If topic scale-down or recreation cannot be completed and verified, the process
must:

1. Log the failing stage and error without exposing credentials.
2. Perform applicable local resource cleanup.
3. Terminate with a non-zero exit status.

The process must not continue with apparently successful but incomplete worker,
consumer, or scheduling state, because that would allow a monitoring gap to
remain unnoticed.

### 11.5 Worker and Consumer State During Recreation

After successful topic deletion and recreation:

- Kafka partition dispatch must be paused before the destructive operation.
- New Kafka checks must not start while recreation is in progress.
- Bounded in-flight Kafka checks must be drained or cancelled before old client
  state is replaced.
- Every worker-owned consumer from the old topic incarnation must be closed and
  replaced, including consumers whose last numeric partition still exists.
- The worker count must be recalculated as
  `min(replacement partition count, max.workers)`.
- Monitoring must not resume until the complete replacement worker/consumer set
  and fair-scheduling state are ready.
- Replacement state must become visible atomically; the scheduler must not see
  a partially updated worker or partition set.
- Failure to build the complete replacement state must terminate the process
  with a non-zero exit status after applicable cleanup.
- Kafka readiness must reset after recreation.

For an in-place partition increase, the application must update the expected
partition set and fair-scheduling state. It may increase workers and consumers
only until `max.workers` is reached.

### 11.6 Optional Kafka Log Topic

The existing Kafka log-publishing feature must remain optional and disabled by
default. Standard output remains the primary log destination. Enabling Kafka
log publishing explicitly expands the required principal permissions to the
configured log topic; when it is disabled, neither managers nor observers need
access to that topic.

When enabled:

- The configured log-topic name must differ from the canary check-topic name.
- The manager may create the log topic if it is missing, using one partition
  and the configured `log.topic.retention.ms` value.
- The log topic must not be resized, deleted, or recreated as broker count
  changes.
- Observers must never create or otherwise administer the log topic; they may
  only publish to an existing one.
- Log-topic setup and record-publishing failures must be sanitized and counted
  but must not block or change Kafka-check, Schema Registry, readiness,
  liveness, or dependency-health results. The process must continue with
  standard-output logging.
- Logging failures must not recursively generate additional Kafka log records.

Keeping this optional feature does not broaden canary-topic administration:
delete, recreate, and partition-increase permissions remain restricted to the
canary check topic.

## 12. Metrics Cardinality

The application must expose a layered metric model: bounded aggregate metrics
for operational diagnosis and a limited set of stable, linearly scaling
per-partition metrics. Metric behavior and label meaning must not change when a
partition-count threshold is crossed.

The aggregate metric set must not include a `partition` label and must include:

- `canary_checks_total{host,result}`
- `canary_failures_total{host,phase,category,recoverability}`
- `canary_e2e_latency_ms{host}`
- `canary_partitions_by_state{host,state}`
- `canary_worst_partition_staleness_seconds{host}`
- `canary_max_consecutive_failures{host}`
- `canary_oldest_partition_overdue_seconds{host}`
- `canary_log_publish_failures_total{host,category}` when optional Kafka log
  publishing is enabled

Every instance must also expose exactly one deployment-role information series:

- `canary_instance_role{role="manage"} 1`, or
- `canary_instance_role{role="observe"} 1`

`role` is a closed enum matching `topic.management.mode`. The application must
not add a generated instance-identity label to this metric. Prometheus scrape
target labels such as `job`, `instance`, and a deployment-supplied cluster label
must identify and scope instances externally.

The supplied Grafana dashboard and production monitoring documentation must use
`canary_instance_role` together with Prometheus's `up` metric to show, within an
explicit deployment or monitored-cluster scope:

- The number of running managers.
- The number of running observers.
- The total number of running canary instances.
- Configured scrape targets that are currently unreachable.

They must also define a prominent alert when the running manager count is not
exactly one, covering both zero and multiple managers. Queries and alert rules
must handle an absent role series without incorrectly treating the manager
count as healthy. The canary must not configure or enforce an expected observer
count; desired observer replicas remain the responsibility of the deployment
platform and Prometheus service discovery.

The per-partition metric set must be limited to stable, linearly scaling label
combinations:

- `canary_partition_last_success_timestamp_seconds{host,partition}`
- `canary_partition_consecutive_failures{host,partition}`
- `canary_partition_checks_total{host,partition,result}`
- `canary_partition_current_state{host,partition,state}`

Metric label vocabularies must be exact:

- `result`: `success` or `failure`.
- `state`: `healthy`, `degraded`, or `unhealthy`.
- `role`: `manage` or `observe`.
- `phase`: the uppercase failure-phase enum in Section 8.1.
- `category`: the uppercase failure-category enum in Section 8.1.
- `recoverability`: the uppercase recoverability enum in Section 8.1.

There is no `unknown` metric state. A component without a successful current-
process observation is `unhealthy` as defined in Section 7. Arbitrary exception
text, status text, resource names, or messages must never become label values.
A `partition` label must not be combined with `phase`, `category`, or
`recoverability`. Latency histograms must remain aggregate and must not carry a
`partition` label.

The implementation must remove `metrics.partition.threshold` and must not emit
the synthetic label value `partition="*"`. It must not attach a topic
generation or topic-incarnation label to any metric. Detailed historical
correlation between a partition and an error belongs in sanitized structured
logs, not additional Prometheus label dimensions.

Metric children for partitions removed by reconciliation or topic recreation
must be deleted from the registry. Obsolete children for bounded labels, such
as the previous value of a partition `state`, must also be removed so stale
series do not falsely describe current state. Metrics created for the verified
replacement topology must use the latest partition set. At approximately 600
partitions, the resulting per-partition series count must remain linear and on
the order of a few thousand, rather than multiplying by error-classification
dimensions.

## 13. Transport Security and Secret Delivery

### 13.1 Schema Registry Transport Security

Schema Registry must always use HTTPS.

- Configuration containing an `http://` Schema Registry URL must be rejected
  before any authenticated request is attempted.
- Basic-auth credentials must never be sent over plaintext HTTP.
- Startup connectivity validation must not report SSL/TLS validation success for
  a non-HTTPS endpoint.
- Certificate and hostname validation failures are fatal deterministic failures.
- Error messages must not include credentials.

There is no development-mode exception for plaintext Schema Registry HTTP in
this specification.

### 13.2 Layered Configuration and Secret Sources

INI configuration remains the source for non-secret settings, including topic
names, endpoints without embedded credentials, intervals, thresholds, worker
limits, bind addresses, TLS mode, and logging behavior.

Production deployments must be able to provide these sensitive values through
separate file-backed secret sources:

- Kafka `sasl.password` through `sasl.password.file`.
- Schema Registry `basic.auth.user.info` through
  `basic.auth.user.info.file`.
- Kafka `ssl.key.password`, when used, through `ssl.key.password.file`.

For every supported secret, the inline form and file-backed form are mutually
exclusive. Configuring both must produce an actionable sanitized configuration
error. Configuring neither is also an error for required secrets, but is valid
for an optional secret whose associated feature does not require it. Silent
source precedence is prohibited.

Inline secret values may remain supported for local development and for
deployments that mount the complete INI file as a protected secret. Production
documentation must recommend file-backed secrets or a fully secret-mounted INI
rather than direct secret environment variables.

Secret-file resolution must:

- Occur during initialization before operational clients are constructed.
- Read only the explicitly configured file.
- Remove at most one conventional trailing line ending supplied by secret
  mounting tools.
- Reject an empty resolved value.
- Return errors that identify the setting without disclosing the value.
- Never log, metricize, persist, or place the secret in health state,
  diagnostics, process arguments, or exception text.
- Retain only the resolved in-memory value required to configure the client.

The application must not contact Vault, AWS Secrets Manager, Azure Key Vault,
Google Secret Manager, or another vendor service directly. Enterprise secret
systems may project their values into the configured files.

Live secret reload is outside scope. Credential rotation requires a controlled
process restart, after which configuration resolution, startup validation,
warmup, and readiness execute with the new value.

## 14. Testing Requirements

Automated tests must use mocks or fakes and must not contact real Kafka,
Schema Registry, or external network services.

Regression coverage must include at least:

1. One fresh Kafka partition cannot conceal a stale partition.
2. Kafka degraded and unhealthy staleness boundaries.
3. Rolling failure-rate window truncation and minimum-sample behavior.
4. Failure rates are evaluated per partition rather than globally.
5. Latest transient SR failure produces degraded health.
6. Deterministic SR failure produces unhealthy health.
7. Historical SR successes cannot conceal the latest SR failure.
8. Kafka and SR histories and thresholds remain independent.
9. Readiness waits for every partition's successful post-warmup check.
10. `warmup.checks=0` behavior.
11. Delete failure, propagation timeout, recreation failure, and verification
    failure are reported as failures.
12. Failed reconciliation does not remove consumers or claim the requested
    partition count.
13. Successful recreation rebuilds the complete worker-owned consumer set and
    scheduling state.
14. Consumer-pool rebuild failure follows the required fatal path.
15. Aggregate metrics omit the `partition` label and use only bounded label
    values.
16. Per-partition metrics remain limited to the specified linear label sets at
    both small and approximately 600-partition topology sizes.
17. Reconciliation and topic recreation remove metric children for deleted
    partitions and obsolete bounded-label values.
18. Metrics never emit `partition="*"`, a topic-generation label, or a
    partition combined with error-classification dimensions.
19. Plain HTTP Schema Registry URLs are rejected.
20. Valid and invalid combinations of the new health configuration settings.
21. Oldest-due-first partition selection and deterministic tie-breaking.
22. A partition cannot have duplicate concurrent checks.
23. Missed Kafka occurrences are coalesced without catch-up bursts.
24. Startup scheduling distributes initial Kafka work across the interval.
25. Worker and consumer counts equal
    `min(partition_count, max.workers)`.
26. A consumer is owned and used exclusively by one worker.
27. Invalid consumers are replaced without exceeding the configured bound.
28. Kafka backlog does not delay SR probes or ordinary reconciliation.
29. SR and reconciliation operations never overlap with another invocation of
    the same operation.
30. SR timeout and interval validation.
31. Scheduling and timeout calculations use monotonic time.
32. Shutdown stops dispatch and completes within its defined bound.
33. `/live` is unaffected by Kafka and SR dependency failures.
34. `/live` fails for stale scheduler heartbeat, fatal internal state, and
    shutdown.
35. Initial readiness requires every partition's successful post-warmup check
    and initial SR validation.
36. External Kafka and SR outages do not revoke readiness after it is achieved.
37. Topic recreation, worker rebuilding, new partition initialization, stale
    scheduler heartbeat, and shutdown revoke readiness.
38. `/health` can report dependency or capacity failure while `/live` and
    `/ready` remain successful.
39. Container liveness probing honors configured port and HTTP/HTTPS mode and
    targets `/live`.
40. Process restart resets rolling histories, warmup, readiness, SR state, and
    process-local topic generation.
41. Prometheus history and broker metadata are used for their defined external
    purposes without being read back as authoritative endpoint state.
42. Old-generation Kafka results cannot update state after topic recreation.
43. State snapshots are immutable, bounded, and safe under concurrent worker,
    SR, reconciliation, scheduler, and HTTP activity.
44. Invariant violations fail closed and lead to bounded cleanup and non-zero
    process termination.
45. Startup recovers from interruption during topic recreation by inspecting
    and reconciling broker metadata.
46. Typed Kafka and HTTP failures map to the required bounded descriptor fields.
47. Client-local Kafka failures are not all classified as network failures.
48. Schema Registry 4xx responses are distinguished as authentication,
    authorization, configuration, or another applicable bounded category.
49. Classification and recovery policy are independently tested.
50. Startup and runtime contexts produce their specified different actions.
51. Consumer-state failures replace only the affected worker-owned consumer.
52. Unknown external failures retry without producing a healthy result, while
    unknown internal failures terminate after bounded cleanup.
53. Raw exception content and credentials do not enter state snapshots or
    metric labels.
54. A slow `/metrics` request does not block `/live` when an HTTP worker remains
    available.
55. HTTP worker count and waiting work never exceed their configured bounds.
56. HTTP queue saturation rejects excess work without changing Kafka or SR
    health state.
57. Slow request reads and writes are bounded by the configured socket timeout.
58. Handler failures return sanitized JSON without raw exception content.
59. HTTP shutdown stops acceptance, bounds draining, closes the socket, and
    releases request-worker resources.
60. HTTP work never uses Kafka workers or performs dependency network calls.
61. The default listener is reachable only through loopback.
62. A broad plaintext bind emits the required security warning.
63. Read-only methods work as specified and mutation methods are rejected.
64. Endpoint responses omit credentials, raw exceptions, sensitive paths, and
    other prohibited internal data.
65. HTTPS probes validate certificates and never fall back to disabled
    verification.
66. The bundled Prometheus and Grafana stack binds locally, excludes unnecessary
    administrative exposure, and is documented as development-only.
67. Production documentation requires an externally managed secure monitoring
    system and does not configure the canary with a Prometheus server URL.
68. Startup validation uses the actual long-lived AdminClient and
    SchemaRegistryClient configurations without temporary duplicate validation
    clients.
69. Deterministic startup configuration, authentication, authorization, and TLS
    failures terminate with a non-zero status after cleanup.
70. Transient Kafka and SR startup failures remain live, stay not ready, report
    unhealthy, and retry only the current initialization stage.
71. Startup retry delays follow the configured exponential progression, cap,
    jitter bounds, and reset behavior using monotonic time.
72. Initialization attempts do not overlap or queue while a prior attempt is in
    flight.
73. Shutdown and fatal failure clean up every partially initialized long-lived
    client.
74. Produce, schema, and consume capabilities are proven through warmup before
    readiness.
75. The Python application runs through the documented module entry point
    without relying on `.venv/bin/python`.
76. Local monitoring can be started and stopped independently of the canary.
77. Development helpers do not install, start, stop, or reconfigure external
    container runtimes.
78. Missing development prerequisites produce actionable errors without system
    mutation.
79. Production examples use explicit image versions and do not require the
    bundled monitoring stack.
80. Documentation separates Python, OCI-image, and local-monitoring workflows.
81. The obsolete combined `run.sh` workflow is removed without replacement by
    another platform-detecting launcher.
82. Each supported inline or file-backed secret source is accepted individually;
    dual sources fail safely, and missing required sources fail safely.
83. Secret-file values have only one conventional trailing line ending removed,
    and empty results are rejected.
84. Secret-file read and validation errors identify the setting without exposing
    secret contents.
85. Resolved secrets do not appear in logs, metrics, HTTP responses, health
    state, process arguments, or persisted files.
86. Operational clients receive the resolved value through their standard
    configuration key.
87. Rotation is performed through restart; no live-reload watcher or
    vendor-specific secret-manager client is introduced.
88. The production image contains no interactive network-diagnostic tools and
    uses the Python configuration-aware liveness probe.
89. The image runs as a non-root numeric user with a read-only root filesystem,
    dropped capabilities, and no required writes under `/app`.
90. Production dependencies install from a fully resolved hash-locked file, and
    hash verification rejects altered artifacts.
91. Lock-file refresh is not performed implicitly during unrelated work.
92. OCI source, version, revision, and creation metadata are accurate and contain
    no placeholders.
93. Application and image version consistency is validated from one release
    version source.
94. Startup uses direct process-exec semantics and performs no diagnostics or
    system reconfiguration.
95. Every claimed image architecture successfully builds, starts, imports native
    dependencies, and executes the liveness probe.
96. The release validation supports vulnerability scanning and build-platform
    SBOM or signing integration without embedding a signing service in the
    application.
97. `topic.management.mode` accepts only `manage` and `observe`, defaults to
    `manage`, and rejects other values.
98. A manager performs mutation and verification while continuing normal
    canary checks.
99. An observer never issues create, delete, or partition-increase requests and
    continues monitoring a stable topic without a running manager.
100. An observer pauses dispatch and reports unhealthy for a missing or
     mismatched topic, then rebuilds local state after observing the verified
     configured topology.
101. No automatic leader election, lease, lock topic, or role promotion is
     introduced.
102. Each instance exports exactly one `canary_instance_role` series with the
     configured bounded role and no application-generated instance identifier.
103. Dashboard queries count only running managers and observers in the selected
     deployment or monitored-cluster scope by incorporating Prometheus `up`.
104. The manager-count alert fires for both zero and multiple running managers,
     including when the role series is absent.
105. Observer scaling does not require or introduce an expected-observer-count
     application setting.
106. Verified broker count determines desired canary-topic partition count;
     scale-up expands in place and scale-down performs verified recreation.
107. Creation and recreation apply replication factor
     `min(verified broker count, 3)` and the specified retention, while an
     existing correctly partitioned topic is not silently reconfigured.
108. Scheduling capacity becomes degraded exactly when the oldest-due age
     reaches one check interval, clears below that boundary, and relies on
     partition staleness for eventual unhealthy state.
109. A Kafka partition or Schema Registry with no successful current-process
     observation is unhealthy without making `/live` fail or `/ready` succeed.
110. `/health` returns no more than the configured diagnostic limit, orders the
     worst components deterministically, and reports accurate affected,
     returned, and truncated counts.
111. Metrics accept only the specified result, state, role, phase, category,
     and recoverability label values and never emit an `unknown` state.
112. Disabled Kafka log publishing requires no log-topic activity; enabled
     manager and observer modes enforce their distinct administration and
     publishing behavior.
113. Kafka log-topic setup and record-publishing failures are sanitized and
     counted without changing check results or operational endpoint state and
     without recursive logging.
114. Thousands of completed Kafka and Schema Registry observations never grow
     rolling histories beyond `P * W` and `W`, respectively.
115. Missed schedules and repeated retries never create more than one current
     scheduling record per expected partition or an accumulating ready queue.
116. Repeated scale-up, scale-down, and topic recreation return health,
     scheduling, readiness, and Prometheus child counts to bounds derived from
     the latest verified partition count.
117. Workers, consumers, in-flight Kafka checks, HTTP work, and concurrently
     materialized snapshots remain within their specified bounds under load.
118. Failure summaries are sanitized and truncated to 512 characters before
     storage, and snapshots contain no copies of rolling histories or retained
     prior snapshots.
119. Kafka native-client queues use explicit finite message and byte limits;
     queue saturation creates no Python retry or shadow queue.

## 15. Acceptance Criteria

The remediation is complete only when:

- All requirements in this specification are implemented.
- Every identified defect has a regression test.
- All relevant automated tests pass.
- All Python source files parse successfully.
- No test requires credentials, private data, or a live external service.
- Documentation and the configuration template describe the implemented
  behavior and defaults accurately.
- The final diff contains no unrelated changes, generated artifacts, secrets,
  or credential-bearing files.
- No unresolved failure is hidden by a successful exit status or a healthy
  endpoint response.

## 16. Ralph-Loop Constraints

When this specification is implemented through a Ralph loop:

- Define a maximum iteration count before starting.
- Address one bounded task per iteration.
- Assign no more than five Section 14 criteria to an implementation task and
  inject those exact criteria plus targeted specification section identifiers
  into a fresh ephemeral agent session. Do not require a whole-spec reread.
- Inspect repository status and current file contents before editing.
- Add or update the narrowest relevant regression test with each behavioral
  change.
- Run targeted validation before broader validation.
- Record changed files, commands, results, assumptions, blockers, and the next
  task after every iteration.
- Require schema-validated implementation output. Preserve complete agent,
  validation, and review logs locally, but forward only bounded feedback and
  concrete blocking findings to subsequent attempts.
- Bound each iteration to 12 changed files and 1,200 diff lines. Stop for a
  human task split if either bound is exceeded.
- Do not repeat an unchanged approach after the same validation failure.
- Stop immediately when the acceptance criteria are met or when the configured
  iteration or repeated-failure limit is reached.
- Run the complete existing automated test and deterministic quality suite
  before and after every implementation attempt accepted as progress.
- After tests pass, run a separate read-only review covering consistency,
  security, architecture, performance, best practices, and deviation from this
  specification. A failed review category or high/critical finding blocks the
  iteration.
- Give the reviewer the isolated current-iteration patch, changed-file list,
  exact assigned criteria, and bounded validation evidence. Do not make prior
  accepted task diffs part of the review scope. Forward at most 20 blocking
  findings and 8,000 feedback characters to the next implementation attempt.
- Stop for human intervention rather than guessing when work requires an
  unspecified architectural choice, secrets, network or dependency changes,
  destructive action, expanded scope, or repeated failed attempts.
- Map every regression criterion in Section 14 to exactly one bounded task and
  require that task's declared executable tests before marking it complete.
- Permit Git checkpoints only at declared logical milestones after the complete
  test and review gates pass. Branch creation, commits, and pushes require
  explicit human confirmation; force pushes are prohibited.
