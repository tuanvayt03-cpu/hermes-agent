"""
Hermes Watchdog V1 - Recovery Planner

Plans recovery actions based on classification results and proven capabilities.
"""

import logging
import time
import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional

from core.classifier import (
    ClassificationResult, LifecycleState, FailureClass, RecoveryAction
)
from core.recovery_kernel import (
    RecoveryCapabilityRegistry,
    build_fault_envelope,
)

logger = logging.getLogger(__name__)


@dataclass
class RecoveryPlan:
    """A planned recovery action."""
    action_id: str
    task_id: str
    action_type: str
    failure_class: Optional[str]
    priority: int  # Lower = higher priority
    params: Dict
    idempotency_key: str
    scheduled_at: float
    lease_required: bool = True
    reasoning: str = ""
    fault_envelope: Optional[Dict] = None
    requires_effect_verification: bool = True


class RecoveryPlanner:
    """Plans recovery actions based on task classification and available capabilities."""

    def __init__(self, config: Dict, capabilities: Dict):
        self.config = config
        self.capabilities = capabilities or {}
        self.registry = RecoveryCapabilityRegistry(self.capabilities)
        self.recovery_budgets = config.get("recovery_budgets", {})

        # Recovery action priority (from spec) - V2 updated with CONTEXT_WINDOW_EXCEEDED
        self.action_priority = {
            RecoveryAction.COMPACT_CONTEXT: 0,  # Highest priority for context overflow
            RecoveryAction.RECONCILE_SIDE_EFFECT: 0,  # Same priority - critical
            RecoveryAction.RETRY_TRANSPORT: 1,
            RecoveryAction.MODEL_PROVIDER_RETRY: 1,
            RecoveryAction.RESUME_SESSION: 2,
            RecoveryAction.SESSION_RESUME_FROM_CHECKPOINT: 2,
            RecoveryAction.MODEL_PROVIDER_SWITCH: 3,
            RecoveryAction.NUDGE_AGENT: 3,
            RecoveryAction.WORKER_RESUME_FROM_CHECKPOINT: 4,
            RecoveryAction.VERIFY_RECOVERY: 10,
            RecoveryAction.NO_ACTION: 99,
        }

    def plan_recovery(self, task: 'DiscoveredTask', classification: ClassificationResult,
                       store: 'WatchdogStore', watchdog_id: str,
                       durable_state: Optional[Dict] = None,
                       checkpoint_valid: bool = True,
                       invalidators: Optional[List[str]] = None) -> List[RecoveryPlan]:
        """Generate recovery plans for a classified task."""
        plans = []
        self._store = store
        self._durable_state = durable_state or {}
        fault = build_fault_envelope(
            task,
            classification,
            durable_state=durable_state,
            checkpoint_valid=checkpoint_valid,
            invalidators=invalidators,
        )

        if classification.state == LifecycleState.TERMINAL_COMPLETE:
            # Never act on terminal complete
            return []

        if classification.state == LifecycleState.TERMINAL_BLOCKED:
            # Never act on terminal blocked
            return []

        if classification.state == LifecycleState.WAITING_EXTERNAL:
            # Don't auto-recover waiting states
            return []

        if classification.state == LifecycleState.NEEDS_ATTENTION:
            # Unknown state - no automatic recovery
            return []

        if classification.state == LifecycleState.HEALTHY:
            # No recovery needed
            return []

        if classification.state == LifecycleState.BUSY:
            # Busy tasks need no intervention
            return []

        if fault and fault.invalidators:
            plan = self._create_verify_recovery_plan(task, classification, fault, watchdog_id)
            return [plan] if plan else []

        if classification.state == LifecycleState.SUSPECTED_STALL:
            # Plan nudge if no safer recovery available
            if not self._has_safer_recovery(fault):
                plan = self._create_nudge_plan(task, classification, watchdog_id, fault)
                if plan:
                    plans.append(plan)

        if classification.state == LifecycleState.TRANSIENT_FAILURE and fault:
            # Domain-scoped recovery instead of per-provider special casing.
            plans.extend(self._plan_for_fault(task, classification, watchdog_id, fault))

        if classification.state == LifecycleState.RECOVERY_PENDING and fault:
            # Check if previous recovery needs follow-up
            if fault.recovery_action in (RecoveryAction.COMPACT_CONTEXT, RecoveryAction.RECONCILE_SIDE_EFFECT):
                plans.extend(self._plan_for_fault(task, classification, watchdog_id, fault, primary_only=True))
            else:
                # Check if previous recovery needs follow-up
                plans.extend(self._plan_followup(task, classification, store, watchdog_id, fault))

        # V3: Plan RECONCILE_SIDE_EFFECT if durable state indicates UNKNOWN side effect
        # This is triggered by the watchdog reconciler when durable state shows UNKNOWN
        # The watchdog will inject this via task metadata or classification
        if getattr(task, '_v3_reconcile_side_effect', False):
            side_effect_fault = fault or build_fault_envelope(
                task, classification, durable_state=durable_state,
                checkpoint_valid=checkpoint_valid, invalidators=invalidators
            )
            if side_effect_fault:
                plans.extend(self._plan_for_fault(task, classification, watchdog_id, side_effect_fault, primary_only=True))

        # Sort by priority
        plans.sort(key=lambda p: p.priority)
        return plans

    def _has_safer_recovery(self, fault) -> bool:
        """Check if a safer recovery action is available for this failure class."""
        if not fault:
            return False
        for primitive in self.registry.primitives_for_domain(fault.domain):
            if primitive.action_type not in (
                RecoveryAction.NUDGE_AGENT,
                RecoveryAction.VERIFY_RECOVERY,
            ):
                return True
        return False

    def _plan_for_fault(self, task: 'DiscoveredTask',
                        classification: ClassificationResult,
                        watchdog_id: str, fault,
                        primary_only: bool = False) -> List[RecoveryPlan]:
        """Plan recovery generically from a normalized fault envelope."""
        if fault.domain == "model_provider":
            return self._plan_model_provider_fault(task, classification, watchdog_id, fault)

        plans = []
        fc = classification.failure_class

        # Check retry budget
        budget = self.recovery_budgets.get(fc, {})
        max_retries = budget.get("max_retries", 0)

        if classification.state == LifecycleState.TRANSIENT_FAILURE and max_retries <= 0:
            logger.info(f"No retry budget for {fc}, skipping recovery")
            return []

        for primitive in self.registry.primitives_for_domain(fault.domain):
            if primitive.action_type == RecoveryAction.VERIFY_RECOVERY:
                continue
            plan = self._create_plan_for_primitive(primitive.action_type, task, classification, watchdog_id, fault)
            if plan:
                plans.append(plan)
                break

        return plans

    def _plan_model_provider_fault(self, task: 'DiscoveredTask',
                                   classification: ClassificationResult,
                                   watchdog_id: str, fault) -> List[RecoveryPlan]:
        """Plan only the first proven model-provider primitive in ladder order."""
        for primitive in self.registry.primitives_for_domain(fault.domain):
            if primitive.action_type == RecoveryAction.VERIFY_RECOVERY:
                continue
            plan = self._create_plan_for_primitive(
                primitive.action_type, task, classification, watchdog_id, fault
            )
            if plan:
                return [plan]
        return []

    def _create_plan_for_primitive(self, action_type: str, task: 'DiscoveredTask',
                                   classification: ClassificationResult,
                                   watchdog_id: str, fault) -> Optional[RecoveryPlan]:
        if action_type == RecoveryAction.MODEL_PROVIDER_RETRY:
            return self._create_model_provider_retry_plan(task, classification, watchdog_id, fault)
        if action_type == RecoveryAction.SESSION_RESUME_FROM_CHECKPOINT:
            return self._create_session_resume_from_checkpoint_plan(task, classification, watchdog_id, fault)
        if action_type == RecoveryAction.MODEL_PROVIDER_SWITCH:
            return self._create_model_provider_switch_plan(task, classification, watchdog_id, fault)
        if action_type == RecoveryAction.WORKER_RESUME_FROM_CHECKPOINT:
            return self._create_worker_resume_from_checkpoint_plan(task, classification, watchdog_id, fault)
        if action_type == RecoveryAction.RETRY_TRANSPORT:
            return self._create_transport_retry_plan(task, classification, watchdog_id, fault)
        if action_type == RecoveryAction.RESUME_SESSION:
            return self._create_resume_plan(task, classification, watchdog_id, fault)
        if action_type == RecoveryAction.NUDGE_AGENT:
            return self._create_nudge_plan(task, classification, watchdog_id, fault)
        if action_type == RecoveryAction.COMPACT_CONTEXT:
            return self._create_compact_context_plan(task, classification, watchdog_id, fault)
        if action_type == RecoveryAction.RECONCILE_SIDE_EFFECT:
            return self._create_reconcile_side_effect_plan(task, classification, watchdog_id, fault)
        if action_type == RecoveryAction.VERIFY_RECOVERY:
            return self._create_verify_recovery_plan(task, classification, fault, watchdog_id)
        return None

    def _build_idempotency_key(self, task: 'DiscoveredTask', action_type: str, fault) -> str:
        evidence = (fault or {}).evidence if hasattr(fault, "evidence") else (fault or {}).get("evidence", {})
        error_event_id = evidence.get("error_event_id") or "NO_ERROR_EVENT"
        recovery_generation = evidence.get("recovery_generation", self._durable_state.get("generation", 1))
        return f"{action_type}:{task.task_id}:{error_event_id}:{recovery_generation}"

    def _seen_idempotency_key(self, idempotency_key: str) -> bool:
        store = getattr(self, '_store', None)
        return bool(store and store.has_action_idempotency_key(idempotency_key))

    def _fault_scope_params(self, fault) -> Dict:
        evidence = (fault or {}).evidence if hasattr(fault, "evidence") else (fault or {}).get("evidence", {})
        return {
            "fault_id": getattr(fault, "fault_id", "") if hasattr(fault, "fault_id") else (fault or {}).get("fault_id", ""),
            "error_event_id": evidence.get("error_event_id"),
            "recovery_generation": evidence.get("recovery_generation", self._durable_state.get("generation", 1)),
        }

    def _configured_model_provider_switch(self) -> Optional[Dict]:
        nested = (self.config.get("model_provider_recovery") or {}).get("switch")
        direct = self.config.get("model_provider_switch")
        configured = nested if nested is not None else direct
        if not configured:
            return None
        if isinstance(configured, str):
            return {"provider": configured}
        if not isinstance(configured, dict):
            return None
        if configured.get("enabled") is False:
            return None
        if any(configured.get(field) for field in ("provider", "target_provider", "model", "raw_input")):
            return dict(configured)
        return None

    def _create_model_provider_retry_plan(self, task: 'DiscoveredTask',
                                          classification: ClassificationResult,
                                          watchdog_id: str, fault) -> Optional[RecoveryPlan]:
        """Create a direct model-provider retry plan when a real API exists."""
        action_id = f"model_provider_retry_{task.task_id}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        idempotency_key = self._build_idempotency_key(task, RecoveryAction.MODEL_PROVIDER_RETRY, fault)
        if self._seen_idempotency_key(idempotency_key):
            logger.debug("Model provider retry already executed for %s", task.task_id)
            return None

        return RecoveryPlan(
            action_id=action_id,
            task_id=task.task_id,
            action_type=RecoveryAction.MODEL_PROVIDER_RETRY,
            failure_class=classification.failure_class,
            priority=self.action_priority[RecoveryAction.MODEL_PROVIDER_RETRY],
            params={
                "session_key": task.session_key,
                "session_id": task.session_id,
                **self._fault_scope_params(fault),
            },
            idempotency_key=idempotency_key,
            scheduled_at=time.time(),
            lease_required=True,
            reasoning=f"Direct model-provider retry for {fault.kind}",
            fault_envelope=fault.to_dict(),
        )

    def _create_session_resume_from_checkpoint_plan(self, task: 'DiscoveredTask',
                                                    classification: ClassificationResult,
                                                    watchdog_id: str, fault) -> Optional[RecoveryPlan]:
        """Create a checkpoint-based session resume plan for model-provider faults."""
        action_id = f"session_resume_checkpoint_{task.task_id}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        idempotency_key = self._build_idempotency_key(
            task, RecoveryAction.SESSION_RESUME_FROM_CHECKPOINT, fault
        )
        if self._seen_idempotency_key(idempotency_key):
            logger.debug("Session resume-from-checkpoint already executed for %s", task.task_id)
            return None

        return RecoveryPlan(
            action_id=action_id,
            task_id=task.task_id,
            action_type=RecoveryAction.SESSION_RESUME_FROM_CHECKPOINT,
            failure_class=classification.failure_class,
            priority=self.action_priority[RecoveryAction.SESSION_RESUME_FROM_CHECKPOINT],
            params={
                "session_key": task.session_key,
                "session_id": task.session_id,
                "resume_reason": f"model_provider_fault:{fault.kind}",
                **self._fault_scope_params(fault),
            },
            idempotency_key=idempotency_key,
            scheduled_at=time.time(),
            lease_required=True,
            reasoning=f"Resume same session from checkpoint for {fault.kind}",
            fault_envelope=fault.to_dict(),
        )

    def _create_model_provider_switch_plan(self, task: 'DiscoveredTask',
                                           classification: ClassificationResult,
                                           watchdog_id: str, fault) -> Optional[RecoveryPlan]:
        """Create a configured fallback-provider switch plan."""
        switch_config = self._configured_model_provider_switch()
        if not switch_config:
            return None

        action_id = f"model_provider_switch_{task.task_id}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        idempotency_key = self._build_idempotency_key(task, RecoveryAction.MODEL_PROVIDER_SWITCH, fault)
        if self._seen_idempotency_key(idempotency_key):
            logger.debug("Model provider switch already executed for %s", task.task_id)
            return None

        return RecoveryPlan(
            action_id=action_id,
            task_id=task.task_id,
            action_type=RecoveryAction.MODEL_PROVIDER_SWITCH,
            failure_class=classification.failure_class,
            priority=self.action_priority[RecoveryAction.MODEL_PROVIDER_SWITCH],
            params={
                "session_key": task.session_key,
                "session_id": task.session_id,
                **self._fault_scope_params(fault),
                **switch_config,
            },
            idempotency_key=idempotency_key,
            scheduled_at=time.time(),
            lease_required=True,
            reasoning=f"Switch to configured fallback provider for {fault.kind}",
            fault_envelope=fault.to_dict(),
        )

    def _create_worker_resume_from_checkpoint_plan(self, task: 'DiscoveredTask',
                                                   classification: ClassificationResult,
                                                   watchdog_id: str, fault) -> Optional[RecoveryPlan]:
        """Create a process-registry recovery plan as final model-provider fallback."""
        action_id = f"worker_resume_checkpoint_{task.task_id}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        idempotency_key = self._build_idempotency_key(
            task, RecoveryAction.WORKER_RESUME_FROM_CHECKPOINT, fault
        )
        if self._seen_idempotency_key(idempotency_key):
            logger.debug("Worker resume-from-checkpoint already executed for %s", task.task_id)
            return None

        return RecoveryPlan(
            action_id=action_id,
            task_id=task.task_id,
            action_type=RecoveryAction.WORKER_RESUME_FROM_CHECKPOINT,
            failure_class=classification.failure_class,
            priority=self.action_priority[RecoveryAction.WORKER_RESUME_FROM_CHECKPOINT],
            params={
                "session_key": task.session_key,
                "session_id": task.session_id,
                **self._fault_scope_params(fault),
            },
            idempotency_key=idempotency_key,
            scheduled_at=time.time(),
            lease_required=True,
            reasoning=f"Recover worker/process checkpoint for {fault.kind}",
            fault_envelope=fault.to_dict(),
        )

    def _create_transport_retry_plan(self, task: 'DiscoveredTask',
                                      classification: ClassificationResult,
                                      watchdog_id: str, fault) -> Optional[RecoveryPlan]:
        """Create a transport retry plan."""
        action_id = f"retry_transport_{task.task_id}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        idempotency_key = self._build_idempotency_key(task, RecoveryAction.RETRY_TRANSPORT, fault)

        # Check if we've already executed this action recently
        if self._seen_idempotency_key(idempotency_key):
            logger.debug(f"Transport retry already executed for {task.task_id}")
            return None

        return RecoveryPlan(
            action_id=action_id,
            task_id=task.task_id,
            action_type=RecoveryAction.RETRY_TRANSPORT,
            failure_class=classification.failure_class,
            priority=self.action_priority[RecoveryAction.RETRY_TRANSPORT],
            params={
                "platform": task.platform,
                "session_key": task.session_key,
                **self._fault_scope_params(fault),
            },
            idempotency_key=idempotency_key,
            scheduled_at=time.time(),
            lease_required=True,
            reasoning=f"Transport recovery for {fault.kind}",
            fault_envelope=fault.to_dict(),
        )

    def _create_resume_plan(self, task: 'DiscoveredTask',
                             classification: ClassificationResult,
                             watchdog_id: str, fault) -> Optional[RecoveryPlan]:
        """Create a session resume plan."""
        action_id = f"resume_session_{task.task_id}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        idempotency_key = self._build_idempotency_key(task, RecoveryAction.RESUME_SESSION, fault)
        if self._seen_idempotency_key(idempotency_key):
            logger.debug("Session recovery already executed for %s", task.task_id)
            return None

        return RecoveryPlan(
            action_id=action_id,
            task_id=task.task_id,
            action_type=RecoveryAction.RESUME_SESSION,
            failure_class=classification.failure_class,
            priority=self.action_priority[RecoveryAction.RESUME_SESSION],
            params={
                "session_key": task.session_key,
                "session_id": task.session_id,
                **self._fault_scope_params(fault),
            },
            idempotency_key=idempotency_key,
            scheduled_at=time.time(),
            lease_required=True,
            reasoning=f"Session recovery for {fault.kind}",
            fault_envelope=fault.to_dict(),
        )

    def _create_nudge_plan(self, task: 'DiscoveredTask',
                            classification: ClassificationResult,
                            watchdog_id: str, fault) -> Optional[RecoveryPlan]:
        """Create an agent nudge plan."""
        action_id = f"nudge_agent_{task.task_id}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        idempotency_key = self._build_idempotency_key(task, RecoveryAction.NUDGE_AGENT, fault)

        # Nudge is invasive - only if truly needed
        if classification.state != LifecycleState.SUSPECTED_STALL:
            if classification.state != LifecycleState.TRANSIENT_FAILURE:
                return None
            # For transient failure, only nudge if no other recovery available
            if self._has_safer_recovery(fault):
                return None

        # Check if we already nudged recently
        if self._seen_idempotency_key(idempotency_key):
            logger.debug(f"Nudge already sent for {task.task_id} in current recovery scope")
            return None

        return RecoveryPlan(
            action_id=action_id,
            task_id=task.task_id,
            action_type=RecoveryAction.NUDGE_AGENT,
            failure_class=classification.failure_class,
            priority=self.action_priority[RecoveryAction.NUDGE_AGENT],
            params={
                "session_key": task.session_key,
                "session_id": task.session_id,
                "message": "Continue from the last verified state. Re-read current state before acting. Do not redo completed work. Do not repeat any action whose side-effect outcome is uncertain.",
                **self._fault_scope_params(fault),
            },
            idempotency_key=idempotency_key,
            scheduled_at=time.time(),
            lease_required=True,
            reasoning=f"Agent nudge for {fault.kind}",
            fault_envelope=fault.to_dict(),
        )

    def _create_compact_context_plan(self, task: 'DiscoveredTask',
                             classification: ClassificationResult,
                             watchdog_id: str, fault) -> Optional[RecoveryPlan]:
        """Create a context compaction plan."""
        action_id = f"compact_context_{task.task_id}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        idempotency_key = self._build_idempotency_key(task, RecoveryAction.COMPACT_CONTEXT, fault)

        # Check if we've already executed this action
        if self._seen_idempotency_key(idempotency_key):
            logger.debug(f"Context compaction already executed for {task.task_id}")
            return None

        return RecoveryPlan(
            action_id=action_id,
            task_id=task.task_id,
            action_type=RecoveryAction.COMPACT_CONTEXT,
            failure_class=classification.failure_class,
            priority=self.action_priority[RecoveryAction.COMPACT_CONTEXT],
            params={
                "session_key": task.session_key,
                "session_id": task.session_id,
                "checkpoint_generation": (fault.evidence or {}).get("recovery_generation", 1),
                **self._fault_scope_params(fault),
            },
            idempotency_key=idempotency_key,
            scheduled_at=time.time(),
            lease_required=True,
            reasoning=f"Context recovery for {fault.kind}",
            fault_envelope=fault.to_dict(),
        )

    def _create_reconcile_side_effect_plan(self, task: 'DiscoveredTask',
                                            classification: ClassificationResult,
                                            watchdog_id: str, fault) -> Optional[RecoveryPlan]:
        """Create a side effect reconciliation plan."""
        action_id = f"reconcile_side_effect_{task.task_id}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        idempotency_key = self._build_idempotency_key(task, RecoveryAction.RECONCILE_SIDE_EFFECT, fault)

        # Check if we've already executed this action
        if self._seen_idempotency_key(idempotency_key):
            logger.debug(f"Side effect reconciliation already executed for {task.task_id}")
            return None

        return RecoveryPlan(
            action_id=action_id,
            task_id=task.task_id,
            action_type=RecoveryAction.RECONCILE_SIDE_EFFECT,
            failure_class=classification.failure_class,
            priority=self.action_priority[RecoveryAction.RECONCILE_SIDE_EFFECT],
            params={
                "session_key": task.session_key,
                "session_id": task.session_id,
                "pending_action": getattr(task, '_v3_pending_action', None),
                "last_completed_action": getattr(task, '_v3_last_completed_action', None),
                **self._fault_scope_params(fault),
            },
            idempotency_key=idempotency_key,
            scheduled_at=time.time(),
            lease_required=True,
            reasoning=f"Side effect reconciliation for {fault.kind}",
            fault_envelope=fault.to_dict(),
        )

    def _plan_followup(self, task: 'DiscoveredTask', classification: ClassificationResult,
                        store: 'WatchdogStore', watchdog_id: str, fault=None) -> List[RecoveryPlan]:
        """Plan follow-up for tasks in RECOVERY_PENDING/RECOVERING state."""
        if not store:
            return []

        # Check recent recovery attempts
        attempts = store.get_pending_recovery_attempts(task.task_id)

        plans = []
        for attempt in attempts:
            if attempt['status'] == 'executing':
                # Still executing - check timeout
                planned_at = attempt['planned_at']
                if time.time() - planned_at > 300:  # 5 min timeout
                    # Plan a follow-up verification
                    plan = RecoveryPlan(
                        action_id=f"verify_{attempt['action_id']}",
                        task_id=task.task_id,
                        action_type=RecoveryAction.VERIFY_RECOVERY,
                        failure_class=attempt.get('error_fingerprint_hash'),
                        priority=10,
                        params={
                            "original_action_id": attempt['action_id'],
                            "fault_id": fault.fault_id if fault else None,
                            **(self._fault_scope_params(fault) if fault else {}),
                        },
                        idempotency_key=self._build_idempotency_key(
                            task,
                            RecoveryAction.VERIFY_RECOVERY,
                            fault or {"evidence": {"error_event_id": attempt["action_id"], "recovery_generation": 1}},
                        ),
                        scheduled_at=time.time(),
                        lease_required=False,
                        reasoning="Verify recovery effect after timeout",
                        fault_envelope=fault.to_dict() if fault else None,
                    )
                    plans.append(plan)

        return plans

    def _create_verify_recovery_plan(self, task: 'DiscoveredTask',
                                     classification: ClassificationResult, fault,
                                     watchdog_id: str) -> Optional[RecoveryPlan]:
        """Create a read-only verification plan."""
        action_id = f"verify_recovery_{task.task_id}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        idempotency_key = self._build_idempotency_key(task, RecoveryAction.VERIFY_RECOVERY, fault)
        return RecoveryPlan(
            action_id=action_id,
            task_id=task.task_id,
            action_type=RecoveryAction.VERIFY_RECOVERY,
            failure_class=classification.failure_class,
            priority=self.action_priority[RecoveryAction.VERIFY_RECOVERY],
            params={
                "fault_id": fault.fault_id,
                "invalidators": list(fault.invalidators),
                **self._fault_scope_params(fault),
            },
            idempotency_key=idempotency_key,
            scheduled_at=time.time(),
            lease_required=False,
            reasoning=f"Verify recovery authority for {fault.kind}",
            fault_envelope=fault.to_dict(),
        )

    def validate_plan(self, plan: RecoveryPlan, task: 'DiscoveredTask',
                       store: 'WatchdogStore') -> bool:
        """Validate a plan before execution."""
        # Re-read current task state
        current_task = store.get_task(task.task_id)
        if not current_task:
            logger.warning(f"Task {task.task_id} no longer exists")
            return False

        # Check if task became terminal
        # Would need to re-classify - simplified for now
        if current_task.structured_state in ("COMPLETE", "BLOCKED", "TERMINAL"):
            logger.info(f"Task {task.task_id} became terminal, canceling recovery")
            return False

        # Check idempotency
        if store.has_action_idempotency_key(plan.idempotency_key):
            logger.info(f"Action {plan.action_id} already executed (idempotency)")
            return False

        return True
