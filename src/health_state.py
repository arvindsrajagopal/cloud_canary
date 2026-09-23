"""Concurrency-safe, process-local dependency health state."""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from functools import wraps
from math import isfinite
from threading import RLock
from typing import Callable, Deque, Iterable, Optional, TypeVar
from uuid import UUID, uuid4

from src.error_classifier import (
    FailureComponent,
    FailureDescriptor,
    Recoverability,
)


HEALTHY = "healthy"
DEGRADED = "degraded"
UNHEALTHY = "unhealthy"
_STATUS_RANK = {HEALTHY: 0, DEGRADED: 1, UNHEALTHY: 2}
_INITIALIZATION_STATES = frozenset(
    {
        "STARTING", "VALIDATING_CONFIGURATION", "CONNECTING_KAFKA",
        "WAITING_FOR_KAFKA", "CONNECTING_SCHEMA_REGISTRY",
        "WAITING_FOR_SCHEMA_REGISTRY", "RECONCILING_TOPIC",
        "STARTING_WORKERS", "WARMING_UP", "READY",
        "FATAL_CONFIGURATION", "FATAL_RECONCILIATION", "SHUTTING_DOWN",
    }
)
_T = TypeVar("_T")


class StateInvariantError(RuntimeError):
    """Raised after corrupted authoritative state is failed closed."""


def _invariant_boundary(method: Callable[..., _T]) -> Callable[..., _T]:
    """Publish one sanitized failure after the store lock has unwound."""

    @wraps(method)
    def guarded(self: "HealthStateStore", *args: object, **kwargs: object) -> _T:
        try:
            return method(self, *args, **kwargs)
        except StateInvariantError:
            self._publish_invariant_failure()
            raise

    return guarded


@dataclass(frozen=True)
class ComponentHealth:
    """Immutable, derived health for one dependency component."""

    component: str
    status: str
    staleness_seconds: Optional[float]
    observation_count: int
    failure_rate: Optional[float]
    latest_failure: Optional[str] = None


@dataclass(frozen=True)
class HealthSnapshot:
    """Immutable health view; rolling histories remain private to the store."""

    status: str
    generated_at: float
    partitions: tuple[ComponentHealth, ...]
    schema_registry: ComponentHealth
    scheduling_capacity: ComponentHealth


@dataclass(frozen=True)
class ReadinessSnapshot:
    """Immutable view of post-warmup initialization progress."""

    ready: bool
    generated_at: float
    incomplete_partitions: tuple[int, ...]
    truncated_partition_count: int
    warmup_remaining: tuple[tuple[int, int], ...]
    schema_registry_validated: bool
    scheduler_heartbeat_age_seconds: Optional[float]
    blocking_reasons: tuple[str, ...]


@dataclass(frozen=True)
class LivenessSnapshot:
    """Immutable view of the process scheduling control plane."""

    live: bool
    generated_at: float
    scheduler_heartbeat_age_seconds: Optional[float]
    shutdown_started: bool
    fatal_internal: Optional[str]


@dataclass
class _ComponentState:
    results: Deque[bool]
    last_attempt: Optional[float] = None
    last_success: Optional[float] = None
    latest_failure: Optional[str] = None
    immediate_failure: Optional[str] = None
    completed_attempts: int = 0
    post_warmup_success: bool = False


class HealthStateStore:
    """Own bounded Kafka and Schema Registry observations behind one lock."""

    def __init__(
        self,
        expected_partitions: Iterable[int] = (),
        *,
        window_checks: int = 20,
        minimum_checks: int = 4,
        failure_threshold: float = 0.5,
        max_diagnostic_components: int = 20,
        kafka_check_interval: float = 15.0,
        kafka_degraded_after: float = 60.0,
        kafka_unhealthy_after: float = 300.0,
        sr_check_interval: float = 60.0,
        sr_degraded_after: float = 120.0,
        sr_unhealthy_after: float = 300.0,
        warmup_checks: int = 2,
        liveness_scheduler_max_staleness: float = 10.0,
        monotonic_clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
        invariant_failure_callback: Optional[Callable[[], None]] = None,
    ) -> None:
        if window_checks <= 0:
            raise ValueError("window_checks must be positive")
        if minimum_checks <= 0 or minimum_checks > window_checks:
            raise ValueError("minimum_checks must be between 1 and window_checks")
        if not 0 < failure_threshold <= 1:
            raise ValueError("failure_threshold must be greater than 0 and at most 1")
        if max_diagnostic_components <= 0:
            raise ValueError("health.max.diagnostic.components must be positive")
        if warmup_checks < 0:
            raise ValueError("warmup_checks must be non-negative")
        if liveness_scheduler_max_staleness <= 0:
            raise ValueError(
                "liveness.scheduler.max.staleness.seconds must be positive"
            )
        if kafka_degraded_after <= kafka_check_interval:
            raise ValueError(
                "health.kafka.degraded.after.seconds must be greater than "
                "check.interval.seconds"
            )
        if kafka_unhealthy_after <= kafka_degraded_after:
            raise ValueError(
                "health.kafka.unhealthy.after.seconds must be greater than "
                "health.kafka.degraded.after.seconds"
            )
        if sr_degraded_after <= sr_check_interval:
            raise ValueError(
                "health.sr.degraded.after.seconds must be greater than "
                "sr.check.interval.seconds"
            )
        if sr_unhealthy_after <= sr_degraded_after:
            raise ValueError(
                "health.sr.unhealthy.after.seconds must be greater than "
                "health.sr.degraded.after.seconds"
            )

        partitions = set(expected_partitions)
        if any(not isinstance(partition, int) or partition < 0 for partition in partitions):
            raise ValueError("expected partitions must be non-negative integers")

        self._window_checks = window_checks
        self._minimum_checks = minimum_checks
        self._failure_threshold = failure_threshold
        self.max_diagnostic_components = max_diagnostic_components
        self._warmup_checks = warmup_checks
        self._liveness_scheduler_max_staleness = float(
            liveness_scheduler_max_staleness
        )
        self._kafka_degraded_after = kafka_degraded_after
        self._kafka_check_interval = kafka_check_interval
        self._kafka_unhealthy_after = kafka_unhealthy_after
        self._sr_degraded_after = sr_degraded_after
        self._sr_unhealthy_after = sr_unhealthy_after
        self._monotonic = monotonic_clock
        self._wall = wall_clock
        self._lock = RLock()
        self._topic_generation = uuid4()
        self._partitions = {
            partition: self._new_component() for partition in sorted(partitions)
        }
        self._schema_registry = self._new_component()
        self._scheduler_oldest_overdue = 0.0
        self._readiness_latched = False
        self._readiness_transition: Optional[str] = None
        self._scheduler_heartbeat: Optional[float] = None
        self._shutdown_started = False
        self._fatal_internal: Optional[str] = None
        self._initialization_state: Optional[str] = None
        self._initialization_entered_at: Optional[float] = None
        self._startup_retry_attempts = 0
        self._startup_failure: Optional[FailureDescriptor] = None
        self._invariant_failure_callback = invariant_failure_callback
        self._invariant_failure_notified = False

    def _new_component(self) -> _ComponentState:
        return _ComponentState(results=deque(maxlen=self._window_checks))

    @staticmethod
    def _finite_number(value: object) -> bool:
        return (
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and isfinite(value)
        )

    @staticmethod
    def _invariant(condition: bool, message: str) -> None:
        if not condition:
            raise StateInvariantError(message)

    def _validate_global_state(self) -> None:
        """Validate only constant-size state shared by hot mutations."""
        integer_values = (
            self._window_checks,
            self._minimum_checks,
            self.max_diagnostic_components,
            self._warmup_checks,
        )
        self._invariant(
            all(type(value) is int for value in integer_values)
            and 0 < self._minimum_checks <= self._window_checks
            and self.max_diagnostic_components > 0
            and self._warmup_checks >= 0,
            "invalid integer configuration",
        )
        timing_values = (
            self._liveness_scheduler_max_staleness,
            self._kafka_check_interval,
            self._kafka_degraded_after,
            self._kafka_unhealthy_after,
            self._sr_degraded_after,
            self._sr_unhealthy_after,
        )
        numeric_values = (self._failure_threshold, *timing_values)
        self._invariant(
            all(self._finite_number(value) for value in numeric_values)
            and 0 < self._failure_threshold <= 1
            and self._liveness_scheduler_max_staleness > 0
            and 0 < self._kafka_check_interval < self._kafka_degraded_after
            < self._kafka_unhealthy_after
            and 0 < self._sr_degraded_after < self._sr_unhealthy_after,
            "invalid numeric configuration",
        )
        self._invariant(isinstance(self._partitions, dict), "invalid partitions")
        self._invariant(
            isinstance(self._schema_registry, _ComponentState),
            "invalid schema registry state",
        )
        self._invariant(isinstance(self._topic_generation, UUID), "invalid generation")
        self._invariant(
            isinstance(self._readiness_latched, bool)
            and isinstance(self._shutdown_started, bool)
            and isinstance(self._invariant_failure_notified, bool),
            "invalid boolean state",
        )
        self._invariant(
            self._readiness_transition is None
            or (
                isinstance(self._readiness_transition, str)
                and self._readiness_transition
                in {"topic_recreation", "worker_rebuild", "scheduler_paused"}
            ),
            "invalid readiness transition",
        )
        self._invariant(
            self._finite_number(self._scheduler_oldest_overdue)
            and self._scheduler_oldest_overdue >= 0,
            "invalid scheduler capacity",
        )
        self._invariant(
            self._scheduler_heartbeat is None
            or self._finite_number(self._scheduler_heartbeat),
            "invalid scheduler heartbeat",
        )
        self._invariant(
            self._fatal_internal is None or isinstance(self._fatal_internal, str),
            "invalid fatal state",
        )
        self._invariant(
            self._initialization_state is None
            or self._initialization_state in _INITIALIZATION_STATES,
            "invalid initialization state",
        )
        self._invariant(
            self._initialization_entered_at is None
            or self._finite_number(self._initialization_entered_at),
            "invalid initialization timestamp",
        )
        self._invariant(
            type(self._startup_retry_attempts) is int
            and self._startup_retry_attempts >= 0,
            "invalid startup retry count",
        )
        self._invariant(
            self._startup_failure is None
            or isinstance(self._startup_failure, FailureDescriptor),
            "invalid startup failure",
        )

    @_invariant_boundary
    def publish_initialization_state(
        self,
        state: str,
        *,
        failure: Optional[FailureDescriptor] = None,
        retry_attempts: int = 0,
    ) -> None:
        """Atomically publish bounded startup progress and retry diagnostics."""
        if state not in _INITIALIZATION_STATES:
            raise ValueError("unknown initialization state")
        if failure is not None and not isinstance(failure, FailureDescriptor):
            raise TypeError("failure must be a FailureDescriptor")
        if type(retry_attempts) is not int or retry_attempts < 0:
            raise ValueError("retry_attempts must be a non-negative integer")
        now = self._monotonic()
        with self._lock:
            self._validate_global_state()
            if state != self._initialization_state:
                self._initialization_entered_at = now
            self._initialization_state = state
            self._startup_retry_attempts = retry_attempts
            self._startup_failure = failure
            self._validate_global_state()

    def _validate_component(self, component: object) -> None:
        self._invariant(isinstance(component, _ComponentState), "invalid component")
        assert isinstance(component, _ComponentState)
        self._invariant(isinstance(component.results, deque), "invalid history")
        self._invariant(
            component.results.maxlen == self._window_checks
            and len(component.results) <= self._window_checks
            and all(isinstance(result, bool) for result in component.results),
            "invalid history",
        )
        self._invariant(
            component.last_attempt is None
            or self._finite_number(component.last_attempt),
            "invalid last attempt",
        )
        self._invariant(
            component.last_success is None
            or self._finite_number(component.last_success),
            "invalid last success",
        )
        self._invariant(
            component.latest_failure is None
            or (
                isinstance(component.latest_failure, str)
                and len(component.latest_failure) <= 512
            ),
            "invalid failure classification",
        )
        self._invariant(
            component.immediate_failure is None
            or (
                isinstance(component.immediate_failure, str)
                and len(component.immediate_failure) <= 512
            ),
            "invalid immediate failure classification",
        )
        self._invariant(
            isinstance(component.completed_attempts, int)
            and not isinstance(component.completed_attempts, bool)
            and component.completed_attempts >= 0,
            "invalid warmup progress",
        )
        self._invariant(
            isinstance(component.post_warmup_success, bool),
            "invalid warmup state",
        )

    def _validate_full_state(self) -> None:
        self._validate_global_state()
        for partition, component in self._partitions.items():
            self._invariant(
                isinstance(partition, int)
                and not isinstance(partition, bool)
                and partition >= 0,
                "invalid partition",
            )
            self._validate_component(component)
        self._validate_component(self._schema_registry)
        if self._readiness_latched:
            self._invariant(
                bool(self._partitions)
                and self._schema_registry.last_success is not None
                and self._readiness_transition is None
                and all(
                    component.post_warmup_success
                    for component in self._partitions.values()
                ),
                "readiness latched before initialization",
            )

    def _publish_invariant_failure(self) -> None:
        callback = None
        with self._lock:
            self._fatal_internal = "state_invariant"
            if self._invariant_failure_notified is not True:
                self._invariant_failure_notified = True
                callback = self._invariant_failure_callback
        if callback is not None:
            try:
                callback()
            except Exception:
                pass

    @property
    @_invariant_boundary
    def topic_generation(self) -> UUID:
        """Return this store's opaque, process-local topic generation."""
        with self._lock:
            self._validate_global_state()
            return self._topic_generation

    @_invariant_boundary
    def replace_expected_partitions(
        self,
        expected_partitions: Iterable[int],
        *,
        preserve_existing: bool = True,
        scheduler_oldest_overdue: Optional[float] = None,
    ) -> None:
        """Atomically publish topology and its detached scheduler capacity."""
        partitions = set(expected_partitions)
        if any(not isinstance(partition, int) or partition < 0 for partition in partitions):
            raise ValueError("expected partitions must be non-negative integers")
        if scheduler_oldest_overdue is not None and scheduler_oldest_overdue < 0:
            raise ValueError("oldest overdue age must be non-negative")
        with self._lock:
            self._validate_full_state()
            if not preserve_existing:
                self._topic_generation = uuid4()
            self._partitions = {
                partition: (
                    self._partitions.get(partition, self._new_component())
                    if preserve_existing
                    else self._new_component()
                )
                for partition in sorted(partitions)
            }
            if not preserve_existing or any(
                not component.post_warmup_success
                for component in self._partitions.values()
            ):
                self._readiness_latched = False
            if scheduler_oldest_overdue is not None:
                self._scheduler_oldest_overdue = float(
                    scheduler_oldest_overdue
                )
            self._validate_full_state()

    @_invariant_boundary
    def record_partition_result(
        self,
        partition: int,
        *,
        success: bool,
        failure: Optional[str] = None,
        generation: Optional[UUID] = None,
    ) -> Optional[bool]:
        """Record a current-generation result, returning ``None`` if stale."""
        now = self._monotonic()
        with self._lock:
            self._validate_global_state()
            if generation is not None and generation != self._topic_generation:
                return None
            try:
                component = self._partitions[partition]
            except KeyError as exc:
                raise ValueError(f"partition {partition} is not expected") from exc
            self._validate_component(component)
            is_warmup = component.completed_attempts < self._warmup_checks
            self._record(component, bool(success), now, failure)
            component.completed_attempts += 1
            if not is_warmup and success:
                component.post_warmup_success = True
            self._validate_component(component)
            return is_warmup

    @_invariant_boundary
    def mark_partition_deterministic_failure(
        self,
        partition: int,
        *,
        failure: FailureDescriptor,
        generation: UUID,
    ) -> Optional[bool]:
        """Immediately fail one current-generation Kafka partition."""
        if not isinstance(failure, FailureDescriptor):
            raise TypeError("failure must be a FailureDescriptor")
        if failure.component is not FailureComponent.KAFKA_PARTITION:
            raise ValueError("failure must describe a Kafka partition")
        if failure.recoverability is not Recoverability.DETERMINISTIC:
            raise ValueError("failure must be deterministic")
        bounded = ":".join(
            (
                "deterministic",
                failure.phase.value,
                failure.category.value,
                failure.code or "none",
                failure.safe_summary,
            )
        )[:512]
        with self._lock:
            self._validate_global_state()
            if generation != self._topic_generation:
                return None
            try:
                component = self._partitions[partition]
            except KeyError as exc:
                raise ValueError(f"partition {partition} is not expected") from exc
            self._validate_component(component)
            component.latest_failure = bounded
            component.immediate_failure = bounded
            self._validate_component(component)
            return True

    @_invariant_boundary
    def record_schema_registry_result(
        self,
        *,
        success: bool,
        deterministic_failure: bool = False,
        failure: Optional[str] = None,
    ) -> None:
        """Record one completed SR probe with its bounded failure class."""
        now = self._monotonic()
        classification = None
        if not success:
            classification = "deterministic" if deterministic_failure else "transient"
            if failure:
                classification = f"{classification}:{failure[:128]}"
        with self._lock:
            self._validate_global_state()
            self._validate_component(self._schema_registry)
            self._record(self._schema_registry, bool(success), now, classification)
            self._validate_component(self._schema_registry)

    @_invariant_boundary
    def record_scheduler_capacity(self, oldest_overdue_seconds: float) -> None:
        """Publish the current monotonic scheduling backlog for health."""
        if oldest_overdue_seconds < 0:
            raise ValueError("oldest overdue age must be non-negative")
        with self._lock:
            self._validate_global_state()
            self._scheduler_oldest_overdue = float(oldest_overdue_seconds)
            self._validate_global_state()

    @_invariant_boundary
    def mark_scheduler_capacity_degraded(self) -> None:
        """Publish a bounded overload until the next capacity observation."""
        with self._lock:
            self._validate_global_state()
            self._scheduler_oldest_overdue = max(
                self._scheduler_oldest_overdue,
                float(self._kafka_check_interval),
            )
            self._validate_global_state()

    @_invariant_boundary
    def record_scheduler_heartbeat(self) -> None:
        """Publish scheduler progress using the injected monotonic clock."""
        now = self._monotonic()
        with self._lock:
            self._validate_global_state()
            self._scheduler_heartbeat = now
            self._validate_global_state()

    @_invariant_boundary
    def begin_readiness_transition(self, stage: str) -> None:
        """Revoke readiness while a bounded internal transition is active."""
        if stage not in {"topic_recreation", "worker_rebuild", "scheduler_paused"}:
            raise ValueError("unknown readiness transition")
        with self._lock:
            self._validate_global_state()
            self._readiness_transition = stage
            self._readiness_latched = False

    @_invariant_boundary
    def end_readiness_transition(self) -> None:
        """Publish transition completion and re-evaluate initialized state."""
        with self._lock:
            self._validate_global_state()
            self._readiness_transition = None

    @_invariant_boundary
    def begin_shutdown(self) -> None:
        """Publish that graceful shutdown has started."""
        with self._lock:
            self._validate_global_state()
            self._shutdown_started = True

    @_invariant_boundary
    def record_fatal_internal(self, classification: str) -> None:
        """Publish the first bounded fatal classification without exception data."""
        bounded = str(classification).strip()[:512] or "fatal_internal"
        with self._lock:
            self._validate_global_state()
            if self._fatal_internal is None:
                self._fatal_internal = bounded

    def _update_readiness_latch(self) -> None:
        if (
            self._readiness_transition is None
            and not self._shutdown_started
            and self._fatal_internal is None
            and self._partitions
            and self._schema_registry.last_success is not None
            and all(
                component.post_warmup_success
                for component in self._partitions.values()
            )
        ):
            self._readiness_latched = True
            if self._initialization_state == "WARMING_UP":
                self._initialization_state = "READY"
                self._initialization_entered_at = self._monotonic()
                self._startup_retry_attempts = 0
                self._startup_failure = None

    @staticmethod
    def _record(
        component: _ComponentState,
        success: bool,
        now: float,
        failure: Optional[str],
    ) -> None:
        component.results.append(success)
        component.last_attempt = now
        if success:
            component.last_success = now
            component.latest_failure = None
            component.immediate_failure = None
        else:
            # State retains only a bounded classification, never an exception.
            component.latest_failure = (failure or "failure")[:512]

    @staticmethod
    def _startup_retry_health(component: str, failure: FailureDescriptor,
                              attempts: int) -> ComponentHealth:
        detail = ":".join(
            (
                failure.phase.value,
                failure.category.value,
                failure.code or "none",
                failure.safe_summary,
            )
        )[:512]
        return ComponentHealth(
            component=component,
            status=UNHEALTHY,
            staleness_seconds=None,
            observation_count=attempts,
            failure_rate=None,
            latest_failure=detail,
        )

    @_invariant_boundary
    def snapshot(self) -> HealthSnapshot:
        """Derive an immutable snapshot without exposing rolling histories."""
        now = self._monotonic()
        with self._lock:
            self._validate_full_state()
            scheduler_oldest_overdue = self._scheduler_oldest_overdue
            partitions = tuple(
                self._evaluate(
                    f"kafka:{partition}",
                    component,
                    now,
                    self._kafka_degraded_after,
                    self._kafka_unhealthy_after,
                )
                for partition, component in sorted(self._partitions.items())
            )
            if (
                self._startup_failure is not None
                and self._startup_failure.component
                is not FailureComponent.SCHEMA_REGISTRY
            ):
                partitions = (
                    *partitions,
                    self._startup_retry_health(
                        "kafka_startup_retry",
                        self._startup_failure,
                        self._startup_retry_attempts,
                    ),
                )
            schema_registry = self._evaluate(
                "schema_registry",
                self._schema_registry,
                now,
                self._sr_degraded_after,
                self._sr_unhealthy_after,
                schema_registry=True,
            )
            if (
                self._startup_failure is not None
                and self._startup_failure.component
                is FailureComponent.SCHEMA_REGISTRY
            ):
                schema_registry = self._startup_retry_health(
                    "schema_registry_startup_retry",
                    self._startup_failure,
                    self._startup_retry_attempts,
                )
            capacity_status = (
                DEGRADED
                if scheduler_oldest_overdue >= self._kafka_check_interval
                else HEALTHY
            )
            scheduling_capacity = ComponentHealth(
                component="kafka_scheduling_capacity",
                status=capacity_status,
                staleness_seconds=scheduler_oldest_overdue,
                observation_count=0,
                failure_rate=None,
            )
            statuses = [component.status for component in partitions]
            statuses.extend((schema_registry.status, scheduling_capacity.status))
            overall = max(statuses, key=_STATUS_RANK.__getitem__)
            return HealthSnapshot(
                status=overall,
                generated_at=self._wall(),
                partitions=partitions,
                schema_registry=schema_registry,
                scheduling_capacity=scheduling_capacity,
            )

    @_invariant_boundary
    def readiness_snapshot(self) -> ReadinessSnapshot:
        """Report Schema Registry and per-partition initialization progress."""
        now = self._monotonic()
        with self._lock:
            self._validate_full_state()
            self._update_readiness_latch()
            all_incomplete = tuple(
                partition
                for partition, component in sorted(self._partitions.items())
                if not component.post_warmup_success
            )
            incomplete = all_incomplete[: self.max_diagnostic_components]
            truncated_count = len(all_incomplete) - len(incomplete)
            remaining = tuple(
                (
                    partition,
                    max(0, self._warmup_checks - component.completed_attempts),
                )
                for partition, component in sorted(self._partitions.items())
                if partition in incomplete and not component.post_warmup_success
            )
            sr_validated = self._schema_registry.last_success is not None
            heartbeat_age = (
                None
                if self._scheduler_heartbeat is None
                else max(0.0, now - self._scheduler_heartbeat)
            )
            blockers = []
            if self._shutdown_started:
                blockers.append("shutdown")
            if self._fatal_internal is not None:
                blockers.append("fatal_internal")
            if self._readiness_transition is not None:
                blockers.append(self._readiness_transition)
            if self._initialization_state not in (None, "READY"):
                blockers.append(self._initialization_state.lower())
            # Runtime initialization always publishes a heartbeat before it can
            # complete partition warmup.  Treat only an observed heartbeat as
            # stale so isolated store users can build initialization state.
            if (
                heartbeat_age is not None
                and heartbeat_age > self._liveness_scheduler_max_staleness
            ):
                blockers.append("scheduler_heartbeat_stale")
            return ReadinessSnapshot(
                ready=self._readiness_latched and not blockers,
                generated_at=self._wall(),
                incomplete_partitions=incomplete,
                truncated_partition_count=truncated_count,
                warmup_remaining=remaining,
                schema_registry_validated=sr_validated,
                scheduler_heartbeat_age_seconds=heartbeat_age,
                blocking_reasons=tuple(blockers),
            )

    @_invariant_boundary
    def liveness_snapshot(self) -> LivenessSnapshot:
        """Derive liveness solely from process-internal control-plane state."""
        now = self._monotonic()
        with self._lock:
            self._validate_full_state()
            heartbeat_age = (
                None
                if self._scheduler_heartbeat is None
                else max(0.0, now - self._scheduler_heartbeat)
            )
            shutdown_started = self._shutdown_started
            fatal_internal = self._fatal_internal
            live = (
                (
                    (
                        heartbeat_age is not None
                        and heartbeat_age <= self._liveness_scheduler_max_staleness
                    )
                    or (
                        heartbeat_age is None
                        and self._initialization_state is not None
                    )
                )
                and not shutdown_started
                and fatal_internal is None
            )
            return LivenessSnapshot(
                live=live,
                generated_at=self._wall(),
                scheduler_heartbeat_age_seconds=heartbeat_age,
                shutdown_started=shutdown_started,
                fatal_internal=fatal_internal,
            )

    def _evaluate(
        self,
        name: str,
        component: _ComponentState,
        now: float,
        degraded_after: float,
        unhealthy_after: float,
        *,
        schema_registry: bool = False,
    ) -> ComponentHealth:
        observations = len(component.results)
        failure_rate = None
        if observations >= self._minimum_checks:
            failure_rate = sum(not result for result in component.results) / observations

        if component.last_success is None:
            status = UNHEALTHY
            staleness = None
        else:
            staleness = max(0.0, now - component.last_success)
            if staleness > unhealthy_after:
                status = UNHEALTHY
            elif staleness >= degraded_after:
                status = DEGRADED
            else:
                status = HEALTHY

        if status != UNHEALTHY and failure_rate is not None:
            if failure_rate > self._failure_threshold:
                status = DEGRADED

        if component.immediate_failure is not None:
            status = UNHEALTHY
        elif schema_registry and component.latest_failure:
            if component.latest_failure.startswith("deterministic"):
                status = UNHEALTHY
            elif status != UNHEALTHY:
                status = DEGRADED

        return ComponentHealth(
            component=name,
            status=status,
            staleness_seconds=staleness,
            observation_count=observations,
            failure_rate=failure_rate,
            latest_failure=component.immediate_failure or component.latest_failure,
        )
