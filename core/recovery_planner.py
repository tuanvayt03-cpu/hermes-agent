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
            RecoveryAction.RESUME_SESSION: 2,
            RecoveryAction.NUDGE_AGENT: 3,
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
            if primitive.action_type != RecoveryAction.NUDGE_AGENT:
                return True
        return False

    def _plan_for_fault(self, task: 'DiscoveredTask',
                        classification: ClassificationResult,
                        watchdog_id: str, fault,
                        primary_only: bool = False) -> List[RecoveryPlan]:
        """Plan recovery generically from a normalized fault envelope."""
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
                if primary_only:
                    break
            if primary_only:
                break
            if plans and primitive.action_type == RecoveryAction.RETRY_TRANSPORT:
                break

        return plans

    def _create_plan_for_primitive(self, action_type: str, task: 'DiscoveredTask',
                                   classification: ClassificationResult,
                                   watchdog_id: str, fault) -> Optional[RecoveryPlan]:
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

    def _create_transport_retry_plan(self, task: 'DiscoveredTask',
                                      classification: ClassificationResult,
                                      watchdog_id: str, fault) -> Optional[RecoveryPlan]:
        """Create a transport retry plan."""
        action_id = f"retry_transport_{task.task_id}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        idempotency_key = f"transport_retry:{task.task_id}:{fault.kind}"

        # Check if we've already executed this action recently
        if store := getattr(self, '_store', None):
            if store.has_action_idempotency_key(idempotency_key):
                logger.debug(f"Transport retry already executed for {task.task_id}")
                return None

        return RecoveryPlan(
            action_id=action_id,
            task_id=task.task_id,
            action_type=RecoveryAction.RETRY_TRANSPORT,
            failure_class=classification.failure_class,
            priority=self.action_priority[RecoveryAction.RETRY_TRANSPORT],
            params={"platform": task.platform, "session_key": task.session_key},
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
        idempotency_key = f"resume_session:{task.task_id}:{task.session_key}"

        return RecoveryPlan(
            action_id=action_id,
            task_id=task.task_id,
            action_type=RecoveryAction.RESUME_SESSION,
            failure_class=classification.failure_class,
            priority=self.action_priority[RecoveryAction.RESUME_SESSION],
            params={"session_key": task.session_key, "session_id": task.session_id},
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
        idempotency_key = f"nudge_agent:{task.task_id}:{int(time.time() // 3600)}"  # Hourly bucket

        # Nudge is invasive - only if truly needed
        if classification.state != LifecycleState.SUSPECTED_STALL:
            if classification.state != LifecycleState.TRANSIENT_FAILURE:
                return None
            # For transient failure, only nudge if no other recovery available
            if self._has_safer_recovery(classification):
                return None

        # Check if we already nudged recently
        if store := getattr(self, '_store', None):
            if store.has_action_idempotency_key(idempotency_key):
                logger.debug(f"Nudge already sent for {task.task_id} this hour")
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
                "message": "Continue from the last verified state. Re-read current state before acting. Do not redo completed work. Do not repeat any action whose side-effect outcome is uncertain."
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
        idempotency_key = f"compact_context:{task.task_id}:{fault.kind}"

        # Check if we've already executed this action
        if store := getattr(self, '_store', None):
            if store.has_action_idempotency_key(idempotency_key):
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
                "checkpoint_generation": 1,
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
        idempotency_key = f"reconcile_side_effect:{task.task_id}:{task.session_key}"

        # Check if we've already executed this action
        if store := getattr(self, '_store', None):
            if store.has_action_idempotency_key(idempotency_key):
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
                        action_type="VERIFY_RECOVERY",
                        failure_class=attempt.get('error_fingerprint_hash'),
                        priority=10,
                        params={"original_action_id": attempt['action_id']},
                        idempotency_key=f"verify:{attempt['action_id']}",
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
        idempotency_key = f"verify_recovery:{task.task_id}:{fault.fault_id}"
        return RecoveryPlan(
            action_id=action_id,
            task_id=task.task_id,
            action_type=RecoveryAction.VERIFY_RECOVERY,
            failure_class=classification.failure_class,
            priority=self.action_priority[RecoveryAction.VERIFY_RECOVERY],
            params={"fault_id": fault.fault_id, "invalidators": list(fault.invalidators)},
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
