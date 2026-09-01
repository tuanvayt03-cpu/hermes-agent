"""Single canonical /night admission, hibernate, and wake state machine."""

from __future__ import annotations

from dataclasses import asdict, replace
from typing import Any

from .contracts import (
    MasterTaskLifecycle,
    NightOutcome,
    NightSnapshot,
    NotificationV3Authority,
    RecoveryProvider,
    SAFE_PARKED_STATUSES,
    SnapshotProvider,
    WatchdogInteraction,
)
from .manifest import (
    DurableNightStore,
    ManifestError,
    NightStateConflict,
    ResumeManifest,
    new_night_session_id,
)
from .notification import build_night_hibernation_ready_event


class NightWorkflow:
    """Orchestrates existing authorities without replacing any of them."""

    def __init__(
        self,
        *,
        store: DurableNightStore,
        notification_router: NotificationV3Authority,
        hibernation_executor: object,
        master_task_lifecycle: MasterTaskLifecycle,
        watchdog: WatchdogInteraction,
        session_id_factory=new_night_session_id,
    ) -> None:
        self.store = store
        self.notification_router = notification_router
        self.hibernation_executor = hibernation_executor
        self.master_task_lifecycle = master_task_lifecycle
        self.watchdog = watchdog
        self.session_id_factory = session_id_factory

    @staticmethod
    def _manifest_durable(snapshot: NightSnapshot) -> NightSnapshot:
        return replace(
            snapshot,
            quiescence=replace(snapshot.quiescence, RESUME_MANIFEST_DURABLE=True),
        )

    @staticmethod
    def _notification_delivered(result: Any, expected_key: str) -> bool:
        delivered = bool(
            getattr(result, "DELIVERED", getattr(result, "delivered", False))
        )
        result_key = str(
            getattr(result, "DEDUP_KEY", getattr(result, "dedup_key", ""))
        )
        event_type = str(
            getattr(result, "EVENT_TYPE", getattr(result, "event_type", ""))
        )
        return (
            delivered
            and result_key == expected_key
            and event_type == "NIGHT_HIBERNATION_READY"
        )

    def _abort(
        self, night_session_id: str, manifest_id: str, *reasons: str
    ) -> NightOutcome:
        try:
            self.store.transition(
                night_session_id,
                expected_phase=(
                    "MANIFEST_DURABLE",
                    "NOTIFICATION_DELIVERED",
                    "READY_TO_HIBERNATE",
                ),
                phase="ABORTED",
                abort_reasons=list(reasons),
            )
        except ManifestError:
            pass
        return NightOutcome(
            status="HIBERNATE_REJECTED",
            night_session_id=night_session_id,
            manifest_id=manifest_id,
            reasons=tuple(reasons),
        )

    def request_night(
        self,
        snapshot_provider: SnapshotProvider,
        *,
        requested_by_operator: bool,
    ) -> NightOutcome:
        initial = snapshot_provider()
        if not requested_by_operator or not initial.admission.NIGHT_REQUESTED_BY_OPERATOR:
            return NightOutcome(
                status="ADMISSION_REJECTED", reasons=("operator_intent_missing",)
            )
        admission_reasons = initial.admission.rejection_reasons()
        if admission_reasons:
            return NightOutcome(status="ADMISSION_REJECTED", reasons=admission_reasons)
        if initial.task.task_status.lower() not in SAFE_PARKED_STATUSES:
            return NightOutcome(
                status="ADMISSION_REJECTED",
                reasons=("task_not_completed_or_safely_parked",),
            )
        identity_reasons = []
        if not initial.task.repository_head.strip():
            identity_reasons.append("repository_identity_unknown")
        if not initial.task.runtime_identity.strip():
            identity_reasons.append("runtime_identity_unknown")
        if identity_reasons:
            return NightOutcome(
                status="ADMISSION_REJECTED", reasons=tuple(identity_reasons)
            )

        night_session_id = self.session_id_factory()
        manifest = ResumeManifest.from_task(
            initial.task, night_session_id=night_session_id
        )
        try:
            self.store.write_manifest(manifest)
            self.store.create_session(night_session_id, manifest)
        except ManifestError as exc:
            return NightOutcome(
                status="ADMISSION_REJECTED",
                night_session_id=night_session_id,
                manifest_id=manifest.MANIFEST_ID,
                reasons=(f"resume_manifest_not_durable:{exc}",),
            )

        initial = self._manifest_durable(initial)
        quiescence_reasons = initial.quiescence.unsafe_reasons()
        if quiescence_reasons:
            return self._abort(
                night_session_id, manifest.MANIFEST_ID, *quiescence_reasons
            )

        watchdog_action = self.watchdog.observe_for_night(manifest.MASTER_TASK_ID)
        if watchdog_action != "NOOP":
            return self._abort(
                night_session_id,
                manifest.MANIFEST_ID,
                "watchdog_recovery_action_not_noop",
            )

        event = build_night_hibernation_ready_event(night_session_id, manifest)
        try:
            self.store.transition(
                night_session_id,
                expected_phase="MANIFEST_DURABLE",
                phase="NOTIFICATION_IN_FLIGHT",
                notification_phase="IN_FLIGHT",
            )
            delivery = self.notification_router.route(event)
            if not self._notification_delivered(delivery, event.dedup_key):
                return NightOutcome(
                    status="HIBERNATE_REJECTED",
                    night_session_id=night_session_id,
                    manifest_id=manifest.MANIFEST_ID,
                    reasons=("notification_delivery_unconfirmed",),
                )
            self.store.transition(
                night_session_id,
                expected_phase="NOTIFICATION_IN_FLIGHT",
                phase="NOTIFICATION_DELIVERED",
                notification_phase="DELIVERED",
            )
        except Exception as exc:
            # IN_FLIGHT is durable before the route call.  An exception is
            # intentionally ambiguous and must never cause an automatic resend.
            return NightOutcome(
                status="HIBERNATE_REJECTED",
                night_session_id=night_session_id,
                manifest_id=manifest.MANIFEST_ID,
                reasons=(f"notification_outcome_ambiguous:{type(exc).__name__}",),
            )

        final = self._manifest_durable(snapshot_provider())
        if final.admission != initial.admission:
            return self._abort(
                night_session_id, manifest.MANIFEST_ID, "admission_changed_before_execute"
            )
        if final.task != initial.task:
            return self._abort(
                night_session_id, manifest.MANIFEST_ID, "task_state_changed_before_execute"
            )
        if final.quiescence != initial.quiescence:
            return self._abort(
                night_session_id,
                manifest.MANIFEST_ID,
                "quiescence_predicate_changed_before_execute",
            )
        final_reasons = final.quiescence.unsafe_reasons()
        if final_reasons:
            return self._abort(
                night_session_id, manifest.MANIFEST_ID, *final_reasons
            )

        if getattr(self.hibernation_executor, "dry_run", None) is not True:
            return self._abort(
                night_session_id,
                manifest.MANIFEST_ID,
                "real_hibernation_executor_forbidden",
            )
        self.store.transition(
            night_session_id,
            expected_phase="NOTIFICATION_DELIVERED",
            phase="READY_TO_HIBERNATE",
        )
        receipt = self.hibernation_executor.execute(night_session_id)
        if not bool(getattr(receipt, "accepted", False)) or not bool(
            getattr(receipt, "dry_run", False)
        ):
            return self._abort(
                night_session_id, manifest.MANIFEST_ID, "dry_run_hibernate_rejected"
            )
        self.store.transition(
            night_session_id,
            expected_phase="READY_TO_HIBERNATE",
            phase="HIBERNATED",
            hibernate_dry_run=True,
        )
        return NightOutcome(
            status="HIBERNATE_DRY_RUN_COMPLETE",
            night_session_id=night_session_id,
            manifest_id=manifest.MANIFEST_ID,
            evidence={
                "NIGHT_REQUEST_ACCEPTED": True,
                "QUIESCENCE_GATE_PASS": True,
                "RESUME_MANIFEST_DURABLE": True,
                "PRE_HIBERNATE_NOTIFICATION_DECISION_PASS": True,
                "HIBERNATE_DRY_RUN_PASS": True,
            },
        )

    def wake(
        self,
        night_session_id: str,
        recovery_provider: RecoveryProvider,
    ) -> NightOutcome:
        try:
            state = self.store.read_session(night_session_id)
            manifest = self.store.read_manifest(str(state.get("manifest_id", "")))
        except ManifestError as exc:
            return NightOutcome(
                status="WAKE_REJECTED",
                night_session_id=night_session_id,
                reasons=(f"manifest_missing_or_corrupt:{exc}",),
            )

        if state.get("phase") == "RESUMED":
            return NightOutcome(
                status="ALREADY_RESUMED",
                night_session_id=night_session_id,
                manifest_id=manifest.MANIFEST_ID,
                SAME_MASTER_TASK=True,
                DUPLICATE_RESUME_COUNT=0,
            )
        if state.get("phase") in {"NOTIFICATION_IN_FLIGHT", "RESUME_IN_FLIGHT"}:
            return NightOutcome(
                status="WAKE_REJECTED",
                night_session_id=night_session_id,
                manifest_id=manifest.MANIFEST_ID,
                reasons=("ambiguous_in_flight_outcome",),
            )
        if state.get("phase") != "HIBERNATED" or not state.get("hibernate_dry_run"):
            return NightOutcome(
                status="WAKE_REJECTED",
                night_session_id=night_session_id,
                manifest_id=manifest.MANIFEST_ID,
                reasons=("night_session_not_hibernated",),
            )
        if state.get("manifest_hash") != manifest.MANIFEST_HASH:
            return NightOutcome(
                status="WAKE_REJECTED",
                night_session_id=night_session_id,
                manifest_id=manifest.MANIFEST_ID,
                reasons=("session_manifest_hash_mismatch",),
            )

        recovery = recovery_provider(night_session_id)
        recovery_reasons = list(recovery.rejection_reasons())
        if recovery.master_task_id != manifest.MASTER_TASK_ID:
            recovery_reasons.append("master_task_identity_changed")
        if tuple(recovery.active_workers) != tuple(manifest.ACTIVE_WORKERS):
            recovery_reasons.append("active_worker_identity_changed")
        recovered_processes = tuple(
            asdict(identity) for identity in recovery.process_identities_if_required
        )
        if recovered_processes != tuple(manifest.PROCESS_IDENTITIES_IF_REQUIRED):
            recovery_reasons.append("process_identity_changed")
        if recovery.repository_head != manifest.REPOSITORY_HEAD:
            recovery_reasons.append("repository_identity_changed")
        if recovery.runtime_identity != manifest.RUNTIME_IDENTITY:
            recovery_reasons.append("runtime_identity_changed")
        if recovery_reasons:
            return NightOutcome(
                status="WAKE_REJECTED",
                night_session_id=night_session_id,
                manifest_id=manifest.MANIFEST_ID,
                reasons=tuple(recovery_reasons),
            )
        if not self.watchdog.revalidate_before_recovery(manifest.MASTER_TASK_ID):
            return NightOutcome(
                status="WAKE_REJECTED",
                night_session_id=night_session_id,
                manifest_id=manifest.MANIFEST_ID,
                reasons=("watchdog_task_revalidation_failed",),
            )

        try:
            self.store.transition(
                night_session_id,
                expected_phase="HIBERNATED",
                phase="RESUME_IN_FLIGHT",
            )
            self.master_task_lifecycle.resume_existing(
                manifest.MASTER_TASK_ID,
                manifest.FIRST_UNPROVEN_BOUNDARY,
                manifest.RESUME_INSTRUCTION,
            )
            self.store.transition(
                night_session_id,
                expected_phase="RESUME_IN_FLIGHT",
                phase="RESUMED",
                resume_count=1,
            )
        except NightStateConflict as exc:
            return NightOutcome(
                status="WAKE_REJECTED",
                night_session_id=night_session_id,
                manifest_id=manifest.MANIFEST_ID,
                reasons=(f"resume_claim_conflict:{exc}",),
            )
        except Exception as exc:
            return NightOutcome(
                status="WAKE_REJECTED",
                night_session_id=night_session_id,
                manifest_id=manifest.MANIFEST_ID,
                reasons=(f"resume_outcome_ambiguous:{type(exc).__name__}",),
            )

        return NightOutcome(
            status="WAKE_RESUME_COMPLETE",
            night_session_id=night_session_id,
            manifest_id=manifest.MANIFEST_ID,
            SAME_MASTER_TASK=True,
            DUPLICATE_RESUME_COUNT=0,
            evidence={
                "WAKE_RESUME_PASS": True,
                "SAME_MASTER_TASK": True,
            },
        )
