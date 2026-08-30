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

class RecoveryPlanner:
    """Plans recovery actions based on task classification and available capabilities."""
    
    def __init__(self, config: Dict, capabilities: Dict):
        self.config = config
        self.capabilities = capabilities or {}
        self.recovery_budgets = config.get("recovery_budgets", {})
        
        # Recovery action priority (from spec) - V2 updated with CONTEXT_WINDOW_EXCEEDED
        self.action_priority = {
            RecoveryAction.COMPACT_CONTEXT: 0,  # Highest priority for context overflow
            RecoveryAction.RETRY_TRANSPORT: 1,
            RecoveryAction.RESUME_SESSION: 2,
            RecoveryAction.NUDGE_AGENT: 3,
            RecoveryAction.NO_ACTION: 99,
        }
    
    def plan_recovery(self, task: 'DiscoveredTask', classification: ClassificationResult,
                       store: 'WatchdogStore', watchdog_id: str) -> List[RecoveryPlan]:
        """Generate recovery plans for a classified task."""
        plans = []
        
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
        
        if classification.state == LifecycleState.SUSPECTED_STALL:
            # Plan nudge if no safer recovery available
            if not self._has_safer_recovery(classification):
                plan = self._create_nudge_plan(task, classification, watchdog_id)
                if plan:
                    plans.append(plan)
        
        if classification.state == LifecycleState.TRANSIENT_FAILURE:
            # Plan based on failure class and capability
            plans.extend(self._plan_for_transient_failure(task, classification, watchdog_id))
        
        if classification.state == LifecycleState.RECOVERY_PENDING:
            # Check if previous recovery needs follow-up
            # For CONTEXT_WINDOW_EXCEEDED, plan COMPACT_CONTEXT
            if classification.failure_class == FailureClass.CONTEXT_WINDOW_EXCEEDED:
                if self.capabilities.get("can_compact_context"):
                    plan = self._create_compact_context_plan(task, classification, watchdog_id)
                    if plan:
                        plans.append(plan)
            else:
                # Check if previous recovery needs follow-up
                plans.extend(self._plan_followup(task, classification, store, watchdog_id))
        
        # Sort by priority
        plans.sort(key=lambda p: p.priority)
        return plans
    
    def _has_safer_recovery(self, classification: ClassificationResult) -> bool:
        """Check if a safer recovery action is available for this failure class."""
        if not classification.failure_class:
            return False
        
        fc = classification.failure_class
        
        # For provider overload, timeout, 429 - RETRY_TRANSPORT is preferred
        if fc in (FailureClass.PROVIDER_OVERLOAD, FailureClass.NETWORK_TRANSIENT, FailureClass.HTTP_429_TEMP):
            if self.capabilities.get("can_retry_transport"):
                return True
        
        return False
    
    def _plan_for_transient_failure(self, task: 'DiscoveredTask', 
                                     classification: ClassificationResult,
                                     watchdog_id: str) -> List[RecoveryPlan]:
        """Plan recovery for transient failures."""
        plans = []
        fc = classification.failure_class
        
        # Check retry budget
        budget = self.recovery_budgets.get(fc, {})
        max_retries = budget.get("max_retries", 0)
        
        if max_retries <= 0:
            logger.info(f"No retry budget for {fc}, skipping recovery")
            return []
        
        # Priority 1: RETRY_TRANSPORT (if supported)
        if self.capabilities.get("can_retry_transport") and fc in (
            FailureClass.PROVIDER_OVERLOAD, FailureClass.NETWORK_TRANSIENT, FailureClass.HTTP_429_TEMP
        ):
            plan = self._create_transport_retry_plan(task, classification, watchdog_id)
            if plan:
                plans.append(plan)
        
        # Priority 2: RESUME_SESSION (if supported and transport retry not available/failed)
        if self.capabilities.get("can_resume_session") and not plans:
            plan = self._create_resume_plan(task, classification, watchdog_id)
            if plan:
                plans.append(plan)
        
        # Priority 3: NUDGE_AGENT (last resort)
        if not plans and self.capabilities.get("can_send_task_message"):
            plan = self._create_nudge_plan(task, classification, watchdog_id)
            if plan:
                plans.append(plan)
        
        return plans
    
    def _create_transport_retry_plan(self, task: 'DiscoveredTask',
                                      classification: ClassificationResult,
                                      watchdog_id: str) -> Optional[RecoveryPlan]:
        """Create a transport retry plan."""
        action_id = f"retry_transport_{task.task_id}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        idempotency_key = f"transport_retry:{task.task_id}:{classification.failure_class}"
        
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
            reasoning=f"Transport retry for {classification.failure_class}"
        )
    
    def _create_resume_plan(self, task: 'DiscoveredTask',
                             classification: ClassificationResult,
                             watchdog_id: str) -> Optional[RecoveryPlan]:
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
            reasoning=f"Session resume for {classification.failure_class}"
        )
    
    def _create_nudge_plan(self, task: 'DiscoveredTask',
                            classification: ClassificationResult,
                            watchdog_id: str) -> Optional[RecoveryPlan]:
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
            reasoning=f"Agent nudge for suspected stall or unrecoverable transient failure"
        )
    
    def _create_compact_context_plan(self, task: 'DiscoveredTask',
                             classification: ClassificationResult,
                             watchdog_id: str) -> Optional[RecoveryPlan]:
        """Create a context compaction plan."""
        action_id = f"compact_context_{task.task_id}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        idempotency_key = f"compact_context:{task.task_id}:{classification.failure_class}"
        
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
            reasoning=f"Context compaction for {classification.failure_class}"
        )

    def _plan_followup(self, task: 'DiscoveredTask', classification: ClassificationResult,
                        store: 'WatchdogStore', watchdog_id: str) -> List[RecoveryPlan]:
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
                        reasoning="Verify recovery effect after timeout"
                    )
                    plans.append(plan)
        
        return plans
    
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