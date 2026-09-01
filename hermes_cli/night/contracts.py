"""Plain data contracts for the Night state machine.

All unknown or unresolved facts are represented explicitly and fail closed.
The workflow depends on existing lifecycle owners through Protocols; it does
not create a second task, process, consent, notification, or watchdog store.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Protocol, Sequence


@dataclass(frozen=True)
class AdmissionEvidence:
    NIGHT_REQUESTED_BY_OPERATOR: bool
    CONSENT_RESOLVED: bool
    CRITICAL_TASK_STATE_KNOWN: bool
    WRITER_OWNERSHIP_UNAMBIGUOUS: bool
    RESUME_STATE_CAN_BE_PERSISTED: bool
    SYSTEM_SAFETY_STATE_KNOWN: bool

    def rejection_reasons(self) -> tuple[str, ...]:
        checks = {
            "operator_intent_missing": self.NIGHT_REQUESTED_BY_OPERATOR,
            "consent_unresolved": self.CONSENT_RESOLVED,
            "critical_task_state_unknown": self.CRITICAL_TASK_STATE_KNOWN,
            "writer_ownership_ambiguous": self.WRITER_OWNERSHIP_UNAMBIGUOUS,
            "resume_state_not_persistable": self.RESUME_STATE_CAN_BE_PERSISTED,
            "system_safety_state_unknown": self.SYSTEM_SAFETY_STATE_KNOWN,
        }
        return tuple(name for name, passed in checks.items() if not passed)


@dataclass(frozen=True)
class QuiescenceEvidence:
    NO_ACTIVE_MUTATION: bool
    NO_ACTIVE_TOOL_CALL: bool
    NO_LIVE_CHILD_PROCESS_REQUIRING_COMPLETION: bool
    NO_PENDING_CONSENT: bool
    NO_UNBOUND_WRITER: bool
    NO_IN_FLIGHT_NOTIFICATION: bool
    RESUME_MANIFEST_DURABLE: bool
    WATCHDOG_AVAILABLE: bool
    WATCHDOG_ACTIVE_RECOVERY: bool = False

    def unsafe_reasons(self) -> tuple[str, ...]:
        checks = {
            "active_mutation": self.NO_ACTIVE_MUTATION,
            "active_tool_call": self.NO_ACTIVE_TOOL_CALL,
            "live_child_process_requiring_completion": (
                self.NO_LIVE_CHILD_PROCESS_REQUIRING_COMPLETION
            ),
            "pending_consent": self.NO_PENDING_CONSENT,
            "unbound_or_stale_writer": self.NO_UNBOUND_WRITER,
            "notification_in_flight": self.NO_IN_FLIGHT_NOTIFICATION,
            "resume_manifest_not_durable": self.RESUME_MANIFEST_DURABLE,
            "watchdog_unavailable": self.WATCHDOG_AVAILABLE,
            "watchdog_active_recovery": not self.WATCHDOG_ACTIVE_RECOVERY,
        }
        return tuple(name for name, passed in checks.items() if not passed)

    @property
    def NIGHT_SAFE_TO_HIBERNATE(self) -> bool:
        return not self.unsafe_reasons()


@dataclass(frozen=True)
class ProcessIdentity:
    process_id: str
    owner: str
    requires_completion: bool = False


@dataclass(frozen=True)
class MasterTaskSnapshot:
    master_task_id: str
    master_task_name: str
    task_status: str
    current_capability: str
    first_unproven_boundary: str
    last_durable_checkpoint: str
    active_workers: tuple[str, ...] = ()
    process_identities_if_required: tuple[ProcessIdentity, ...] = ()
    pending_operator_action: str = ""
    resume_instruction: str = ""
    repository_head: str = ""
    runtime_identity: str = ""


@dataclass(frozen=True)
class NightSnapshot:
    admission: AdmissionEvidence
    quiescence: QuiescenceEvidence
    task: MasterTaskSnapshot


@dataclass(frozen=True)
class RecoveryEvidence:
    master_task_id: str
    repository_head: str
    runtime_identity: str
    active_workers: tuple[str, ...]
    process_identities_if_required: tuple[ProcessIdentity, ...]
    worker_ownership_valid: bool
    process_ownership_valid: bool
    notification_route_available: bool
    watchdog_available: bool
    master_task_identity_valid: bool

    def rejection_reasons(self) -> tuple[str, ...]:
        checks = {
            "worker_ownership_invalid": self.worker_ownership_valid,
            "process_ownership_invalid": self.process_ownership_valid,
            "notification_route_unavailable": self.notification_route_available,
            "watchdog_unavailable": self.watchdog_available,
            "master_task_identity_invalid": self.master_task_identity_valid,
        }
        return tuple(name for name, passed in checks.items() if not passed)


@dataclass(frozen=True)
class NightOutcome:
    status: str
    night_session_id: str = ""
    manifest_id: str = ""
    reasons: tuple[str, ...] = ()
    SAME_MASTER_TASK: bool = False
    DUPLICATE_RESUME_COUNT: int = 0
    WATCHDOG_RECOVERY_ACTION: str = "NOOP"
    evidence: dict[str, bool] = field(default_factory=dict)


class NotificationV3Authority(Protocol):
    """Join interface implemented by the canonical Notification V3 router.

    Night never owns Telegram transport.  The injected authority accepts one
    semantic event and returns an authoritative delivery decision.
    """

    def route(self, event: object) -> object: ...


class MasterTaskLifecycle(Protocol):
    """Existing master-task owner; deliberately exposes no create method."""

    def resume_existing(
        self, master_task_id: str, semantic_boundary: str, instruction: str
    ) -> object: ...


class WatchdogInteraction(Protocol):
    def observe_for_night(self, master_task_id: str) -> str: ...

    def revalidate_before_recovery(self, master_task_id: str) -> bool: ...


SnapshotProvider = Callable[[], NightSnapshot]
RecoveryProvider = Callable[[str], RecoveryEvidence]


SAFE_PARKED_STATUSES: Sequence[str] = (
    "completed",
    "blocked",
    "review_requested",
    "waiting",
    "parked",
    "todo",
)
