from __future__ import annotations

import json
from dataclasses import dataclass, replace

import pytest

from hermes_cli.night.command import NightCommand, NightCommandRequest
from hermes_cli.night.contracts import (
    AdmissionEvidence,
    MasterTaskSnapshot,
    NightSnapshot,
    QuiescenceEvidence,
    RecoveryEvidence,
)
from hermes_cli.night.executor import DryRunHibernateExecutor
from hermes_cli.night.manifest import DurableNightStore, new_night_session_id
from hermes_cli.night.notification import (
    NightHibernationReadyEvent,
    NotificationV3Adapter,
)
from hermes_cli.night.workflow import NightWorkflow


@dataclass(frozen=True)
class Delivery:
    DELIVERED: bool
    EVENT_TYPE: str
    DEDUP_KEY: str


class RecordingNotificationV3Router:
    def __init__(self) -> None:
        self.events = []
        self.delivered_keys = set()

    def route(self, event):
        assert event.event_type == "NIGHT_HIBERNATION_READY"
        if event.dedup_key in self.delivered_keys:
            return Delivery(False, event.event_type, event.dedup_key)
        self.delivered_keys.add(event.dedup_key)
        self.events.append(event)
        return Delivery(True, event.event_type, event.dedup_key)


class RecordingMasterTaskLifecycle:
    def __init__(self) -> None:
        self.resume_calls = []
        self.new_engineering_task_count = 0

    def resume_existing(self, master_task_id, semantic_boundary, instruction):
        self.resume_calls.append((master_task_id, semantic_boundary, instruction))
        return {"resumed": master_task_id}


class RecordingWatchdog:
    def __init__(self, *, revalidation=True) -> None:
        self.observations = []
        self.revalidations = []
        self.revalidation = revalidation
        self.disabled = False

    def observe_for_night(self, master_task_id):
        self.observations.append(master_task_id)
        return "NOOP"

    def revalidate_before_recovery(self, master_task_id):
        self.revalidations.append(master_task_id)
        return self.revalidation


def admission(**changes):
    value = AdmissionEvidence(
        NIGHT_REQUESTED_BY_OPERATOR=True,
        CONSENT_RESOLVED=True,
        CRITICAL_TASK_STATE_KNOWN=True,
        WRITER_OWNERSHIP_UNAMBIGUOUS=True,
        RESUME_STATE_CAN_BE_PERSISTED=True,
        SYSTEM_SAFETY_STATE_KNOWN=True,
    )
    return replace(value, **changes)


def quiescence(**changes):
    value = QuiescenceEvidence(
        NO_ACTIVE_MUTATION=True,
        NO_ACTIVE_TOOL_CALL=True,
        NO_LIVE_CHILD_PROCESS_REQUIRING_COMPLETION=True,
        NO_PENDING_CONSENT=True,
        NO_UNBOUND_WRITER=True,
        NO_IN_FLIGHT_NOTIFICATION=True,
        RESUME_MANIFEST_DURABLE=False,
        WATCHDOG_AVAILABLE=True,
        WATCHDOG_ACTIVE_RECOVERY=False,
    )
    return replace(value, **changes)


def task(*, status="completed", boundary="tests-green", checkpoint="commit abc123"):
    return MasterTaskSnapshot(
        master_task_id="task-HERMES-NIGHT-AUTO-HIBERNATE-V1",
        master_task_name="Hermes Night auto-hibernate V1",
        task_status=status,
        current_capability="night-workflow",
        first_unproven_boundary=boundary,
        last_durable_checkpoint=checkpoint,
        active_workers=(),
        process_identities_if_required=(),
        pending_operator_action="",
        resume_instruction=(
            "Continue the exact task from its semantic boundary; "
            "do not replay a tool call."
        ),
        repository_head="08b4875f4a8af7a2162666fc0de0043b2db7ff5d",
        runtime_identity="hermes-0.20.5-local",
    )


def snapshot(*, adm=None, quiet=None, master=None):
    return NightSnapshot(
        admission=adm or admission(),
        quiescence=quiet or quiescence(),
        task=master or task(),
    )


def constant_provider(value):
    return lambda: value


def recovery_for(master):
    return lambda _night_session_id: RecoveryEvidence(
        master_task_id=master.master_task_id,
        repository_head=master.repository_head,
        runtime_identity=master.runtime_identity,
        active_workers=master.active_workers,
        process_identities_if_required=master.process_identities_if_required,
        worker_ownership_valid=True,
        process_ownership_valid=True,
        notification_route_available=True,
        watchdog_available=True,
        master_task_identity_valid=True,
    )


@pytest.fixture
def rig(tmp_path):
    store = DurableNightStore(tmp_path / "night-runtime")
    router = RecordingNotificationV3Router()
    executor = DryRunHibernateExecutor(platform="windows-mock")
    lifecycle = RecordingMasterTaskLifecycle()
    watchdog = RecordingWatchdog()
    ids = iter(f"night-20260829T23000{i}000000Z-test{i:02d}" for i in range(20))
    workflow = NightWorkflow(
        store=store,
        notification_router=router,
        hibernation_executor=executor,
        master_task_lifecycle=lifecycle,
        watchdog=watchdog,
        session_id_factory=lambda: next(ids),
    )
    return workflow, store, router, executor, lifecycle, watchdog


def enter_night(workflow, value):
    command = NightCommand(workflow)
    return command.execute(
        NightCommandRequest("/night", NIGHT_REQUESTED_BY_OPERATOR=True),
        constant_provider(value),
    )


def test_a_completed_manifest_dry_run_wake_no_duplicate(rig):
    workflow, store, router, executor, lifecycle, watchdog = rig
    current = task(status="completed", boundary="focused-tests-pass")
    night = enter_night(workflow, snapshot(master=current))
    assert night.status == "HIBERNATE_DRY_RUN_COMPLETE"
    assert executor.execution_count == 1
    assert len(router.events) == 1

    durable_reopen = DurableNightStore(store.root)
    manifest = durable_reopen.read_manifest(night.manifest_id)
    assert manifest.MASTER_TASK_ID == current.master_task_id
    assert manifest.MANIFEST_HASH == manifest.calculate_hash()

    wake = workflow.wake(night.night_session_id, recovery_for(current))
    wake_again = workflow.wake(night.night_session_id, recovery_for(current))
    assert wake.status == "WAKE_RESUME_COMPLETE"
    assert wake_again.status == "ALREADY_RESUMED"
    assert len(lifecycle.resume_calls) == 1
    assert lifecycle.new_engineering_task_count == 0
    assert watchdog.disabled is False
    assert wake.SAME_MASTER_TASK is True
    assert wake.DUPLICATE_RESUME_COUNT == 0

    witnesses = {
        **night.evidence,
        **wake.evidence,
    }
    for key in (
        "NIGHT_REQUEST_ACCEPTED",
        "QUIESCENCE_GATE_PASS",
        "RESUME_MANIFEST_DURABLE",
        "PRE_HIBERNATE_NOTIFICATION_DECISION_PASS",
        "HIBERNATE_DRY_RUN_PASS",
        "WAKE_RESUME_PASS",
        "SAME_MASTER_TASK",
    ):
        assert witnesses[key] is True
        print(f"{key}=true")
    assert wake.DUPLICATE_RESUME_COUNT == 0
    print("DUPLICATE_RESUME_COUNT=0")


def test_b_parked_long_running_task_restores_exact_boundary(rig):
    workflow, _store, _router, _executor, lifecycle, _watchdog = rig
    current = task(
        status="parked",
        boundary="after-durable-checkpoint-before-next-repair-wave",
        checkpoint="focused test matrix persisted",
    )
    night = enter_night(workflow, snapshot(master=current))
    wake = workflow.wake(night.night_session_id, recovery_for(current))
    assert wake.status == "WAKE_RESUME_COMPLETE"
    assert lifecycle.resume_calls == [
        (
            current.master_task_id,
            current.first_unproven_boundary,
            current.resume_instruction,
        )
    ]


def test_c_active_mutation_rejects_hibernate(rig):
    workflow, _store, router, executor, _lifecycle, _watchdog = rig
    result = enter_night(
        workflow, snapshot(quiet=quiescence(NO_ACTIVE_MUTATION=False))
    )
    assert result.status == "HIBERNATE_REJECTED"
    assert "active_mutation" in result.reasons
    assert executor.execution_count == 0
    assert router.events == []


def test_d_pending_consent_rejects_hibernate(rig):
    workflow, _store, _router, executor, _lifecycle, _watchdog = rig
    result = enter_night(
        workflow, snapshot(quiet=quiescence(NO_PENDING_CONSENT=False))
    )
    assert result.status == "HIBERNATE_REJECTED"
    assert "pending_consent" in result.reasons
    assert executor.execution_count == 0


def test_e_unbound_or_stale_writer_rejects_hibernate(rig):
    workflow, _store, _router, executor, _lifecycle, _watchdog = rig
    result = enter_night(
        workflow, snapshot(quiet=quiescence(NO_UNBOUND_WRITER=False))
    )
    assert result.status == "HIBERNATE_REJECTED"
    assert "unbound_or_stale_writer" in result.reasons
    assert executor.execution_count == 0


def test_f_notification_delivery_pending_rejects_hibernate(rig):
    workflow, _store, router, executor, _lifecycle, _watchdog = rig
    result = enter_night(
        workflow,
        snapshot(quiet=quiescence(NO_IN_FLIGHT_NOTIFICATION=False)),
    )
    assert result.status == "HIBERNATE_REJECTED"
    assert "notification_in_flight" in result.reasons
    assert router.events == []
    assert executor.execution_count == 0


@pytest.mark.parametrize("damage", ["missing", "corrupt"])
def test_g_manifest_missing_or_corrupt_fails_closed(rig, damage):
    workflow, store, _router, _executor, lifecycle, _watchdog = rig
    current = task()
    night = enter_night(workflow, snapshot(master=current))
    path = store.manifest_dir / f"{night.manifest_id}.json"
    if damage == "missing":
        path.unlink()
    else:
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["LAST_DURABLE_CHECKPOINT"] = "tampered"
        path.write_text(json.dumps(payload), encoding="utf-8")
    wake = workflow.wake(night.night_session_id, recovery_for(current))
    assert wake.status == "WAKE_REJECTED"
    assert wake.reasons[0].startswith("manifest_missing_or_corrupt:")
    assert lifecycle.resume_calls == []


def test_h_wake_twice_resumes_exactly_once(rig):
    workflow, _store, _router, _executor, lifecycle, watchdog = rig
    current = task()
    night = enter_night(workflow, snapshot(master=current))
    first = workflow.wake(night.night_session_id, recovery_for(current))
    second = workflow.wake(night.night_session_id, recovery_for(current))
    assert first.status == "WAKE_RESUME_COMPLETE"
    assert second.status == "ALREADY_RESUMED"
    assert len(lifecycle.resume_calls) == 1
    assert watchdog.revalidations == [current.master_task_id]


def test_i_watchdog_active_recovery_makes_night_wait(rig):
    workflow, _store, _router, executor, _lifecycle, watchdog = rig
    result = enter_night(
        workflow,
        snapshot(quiet=quiescence(WATCHDOG_ACTIVE_RECOVERY=True)),
    )
    assert result.status == "HIBERNATE_REJECTED"
    assert "watchdog_active_recovery" in result.reasons
    assert executor.execution_count == 0
    assert watchdog.disabled is False


def test_j_live_child_process_makes_night_wait(rig):
    workflow, _store, _router, executor, _lifecycle, _watchdog = rig
    result = enter_night(
        workflow,
        snapshot(
            quiet=quiescence(
                NO_LIVE_CHILD_PROCESS_REQUIRING_COMPLETION=False
            )
        ),
    )
    assert result.status == "HIBERNATE_REJECTED"
    assert "live_child_process_requiring_completion" in result.reasons
    assert executor.execution_count == 0


def test_final_predicate_change_aborts_before_executor(rig):
    workflow, store, router, executor, _lifecycle, _watchdog = rig
    first = snapshot()
    changed = snapshot(quiet=quiescence(NO_ACTIVE_TOOL_CALL=False))
    values = iter((first, changed))
    result = workflow.request_night(
        lambda: next(values), requested_by_operator=True
    )
    assert result.status == "HIBERNATE_REJECTED"
    assert result.reasons == ("quiescence_predicate_changed_before_execute",)
    assert len(router.events) == 1
    assert executor.execution_count == 0
    assert store.read_session(result.night_session_id)["phase"] == "ABORTED"


def test_non_dry_run_executor_is_refused(tmp_path):
    class ForbiddenExecutor:
        dry_run = False

        def execute(self, _night_session_id):
            raise AssertionError("must never execute")

    workflow = NightWorkflow(
        store=DurableNightStore(tmp_path / "night-runtime"),
        notification_router=RecordingNotificationV3Router(),
        hibernation_executor=ForbiddenExecutor(),
        master_task_lifecycle=RecordingMasterTaskLifecycle(),
        watchdog=RecordingWatchdog(),
        session_id_factory=lambda: "night-20260829T230000000000Z-forbidden",
    )
    result = enter_night(workflow, snapshot())
    assert result.status == "HIBERNATE_REJECTED"
    assert result.reasons == ("real_hibernation_executor_forbidden",)


def test_operator_intent_is_never_inferred(rig):
    workflow, _store, router, executor, _lifecycle, _watchdog = rig
    command = NightCommand(workflow)
    result = command.execute(
        NightCommandRequest("/night", NIGHT_REQUESTED_BY_OPERATOR=False),
        constant_provider(snapshot()),
    )
    assert result.status == "ADMISSION_REJECTED"
    assert result.reasons == ("operator_intent_missing",)
    assert router.events == []
    assert executor.execution_count == 0


def test_manifest_rejects_secret_like_material(rig):
    workflow, store, _router, executor, _lifecycle, _watchdog = rig
    unsafe = replace(task(), pending_operator_action="token=do-not-persist")
    result = enter_night(workflow, snapshot(master=unsafe))
    assert result.status == "ADMISSION_REJECTED"
    assert result.reasons[0].startswith("resume_manifest_not_durable:")
    assert store.unresolved_session_ids() == ()
    assert executor.execution_count == 0


def test_notification_adapter_delegates_to_exact_v3_route_signature():
    calls = []
    sender = object()

    class V3Router:
        def route(self, event, telegram_sender):
            calls.append((event, telegram_sender))
            return Delivery(True, event.event_type, event.dedup_key)

    event = NightHibernationReadyEvent(
        event_type="NIGHT_HIBERNATION_READY",
        task_name="Night task",
        task_id="task-1",
        status="parked",
        summary="checkpoint durable; machine chuẩn bị sleep",
        useful_verification="manifest hash verified",
        next_action="resume from exact semantic boundary",
        dedup_key="night:session-1:NIGHT_HIBERNATION_READY",
    )
    result = NotificationV3Adapter(V3Router(), sender).route(event)
    assert result.DELIVERED is True
    assert calls == [(event, sender)]


def test_night_session_id_uses_utc_uuid_scheme():
    import re

    assert re.fullmatch(
        r"night-\d{8}T\d{12}Z-[0-9a-f]{12}", new_night_session_id()
    )
