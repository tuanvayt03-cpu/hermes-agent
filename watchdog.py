"""
Hermes Watchdog V1 - Main Entrypoint

Global, project-agnostic watchdog that supervises all Hermes/Codex tasks.
"""

import argparse
import json
import logging
import os
import signal
import sys
import time
import yaml
from pathlib import Path
from typing import Optional

# Add watchdog to path
watchdog_root = Path(__file__).parent.parent
sys.path.insert(0, str(watchdog_root))

from persistence.sqlite_store import WatchdogStore
from adapters.hermes_adapter import HermesAdapter, HermesCapabilities
from core.classifier import LifecycleClassifier
from core.recovery_planner import RecoveryPlanner
from core.recovery_kernel import build_checkpoint_snapshot
from core.scheduler import Scheduler, LeaseManager, ScanCycleResult

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("hermes_watchdog")

class HermesWatchdog:
    """Main watchdog orchestrator."""
    
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.watchdog_id = f"watchdog_{int(time.time())}_{os.getpid()}"
        
        # Initialize components
        self.store = WatchdogStore(self.config["watchdog"]["persistence"]["db_path"])
        self.adapter = HermesAdapter(self.config.get("hermes_adapter", {}))
        self.classifier = LifecycleClassifier(self.config)
        self.capabilities = HermesCapabilities()
        
        # Recovery planner needs capabilities
        self.planner = RecoveryPlanner(self.config["watchdog"], {})
        self.planner._store = self.store  # Inject store for idempotency checks
        
        # Lease manager and scheduler
        self.lease_manager = LeaseManager(self.store, self.config["watchdog"])
        self.scheduler = Scheduler(
            self.config["watchdog"],
            self._run_scan_cycle
        )
        
        # State
        self.mode = self.config["watchdog"].get("mode", "OBSERVE")
        self._shutdown = False
        
        logger.info(f"Hermes Watchdog V1 initialized (ID: {self.watchdog_id})")
        logger.info(f"Mode: {self.mode}")
        logger.info(f"Scan interval: {self.config['watchdog']['scan_interval_seconds']}s")
        logger.info(f"Persistence: {self.config['watchdog']['persistence']['db_path']}")
    
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file."""
        path = Path(config_path).expanduser()
        if not path.exists():
            raise FileNotFoundError(f"Config not found: {config_path}")
        
        with open(path, 'r') as f:
            config = yaml.safe_load(f)
        
        logger.info(f"Loaded config from {config_path}")
        return config
    
    def start(self):
        """Start the watchdog."""
        logger.info("Starting Hermes Watchdog...")
        
        # Probe capabilities
        logger.info("Probing Hermes runtime capabilities...")
        self.capabilities = self.adapter.probe_capabilities()
        self.planner = RecoveryPlanner(
            self.config["watchdog"],
            {
                "can_retry_transport": self.capabilities.can_retry_transport,
                "can_resume_session": self.capabilities.can_resume_session,
                "can_send_task_message": self.capabilities.can_send_task_message,
                "can_compact_context": self.capabilities.can_compact_context,
                "can_reconcile_side_effect": self.capabilities.can_reconcile_side_effect,
            }
        )
        self.planner._store = self.store  # Inject store for idempotency checks
        
        logger.info(f"Capabilities: {self.capabilities}")
        
        # Record initial promotion state
        self.store.set_promotion_state("mode", self.mode)
        self.store.set_promotion_state("watchdog_id", self.watchdog_id)
        self.store.set_promotion_state("started_at", str(time.time()))
        
        # Start scheduler
        self.scheduler.start()
        
        # Run initial scan
        logger.info("Running initial scan...")
        self.scheduler.trigger_scan()
        
        logger.info("Hermes Watchdog started successfully")
    
    def stop(self):
        """Stop the watchdog."""
        logger.info("Stopping Hermes Watchdog...")
        self._shutdown = True
        self.scheduler.stop()
        
        # Release all leases
        for task_id, action_id in list(self.lease_manager._held_leases.items()):
            self.lease_manager.release_lease(task_id, action_id)
        
        # Update promotion state
        self.store.set_promotion_state("stopped_at", str(time.time()))
        
        logger.info("Hermes Watchdog stopped")
    
    def _run_scan_cycle(self, cycle_number: int) -> ScanCycleResult:
        """Execute a single scan cycle with V3 durable reconciliation."""
        start_time = time.time()
        tasks_discovered = 0
        tasks_classified = 0
        recoveries_planned = 0
        recoveries_executed = 0
        error = None
        
        try:
            # Record run start
            self.store.record_run_start(cycle_number, self.mode)
            
            # 1. Discover tasks
            tasks = self.adapter.discover_tasks()
            tasks_discovered = len(tasks)
            
            # Update task records in store
            for task in tasks:
                self._upsert_task_record(task)
            
            # 2. Observe and classify each task
            for task in tasks:
                if self._shutdown:
                    break
                
                # Enrich with observations
                observed_task = self.adapter.observe_task(task)
                
                # Classify
                classification = self.classifier.classify(observed_task)
                tasks_classified += 1
                
                # Record observation
                self._record_observation(observed_task, cycle_number, classification)
                
                # V3: Load durable task state machine
                durable_state = self.store.get_task_state_machine(observed_task.task_id)
                
                # V3: Validate checkpoint if exists
                checkpoint_valid = True
                if durable_state and durable_state.get("checkpoint_hash"):
                    checkpoint_valid = self._validate_checkpoint(observed_task, durable_state)

                invalidators = self._detect_invalidators(
                    observed_task, durable_state or {}, checkpoint_valid=checkpoint_valid
                )
                
                # V3: Determine desired state from durable state machine
                desired_state = self._compute_desired_state(observed_task, durable_state, classification)
                
                # V3: Compare durable desired vs observed
                reconciliation_action = self._reconcile_state(observed_task, durable_state, desired_state, classification)
                
                # V3: Persist transition event if state changed
                if durable_state and self._state_changed(durable_state, desired_state):
                    self._record_state_transition(observed_task.task_id, durable_state, desired_state, reconciliation_action)
                
                # V3: Update state machine with optimistic locking
                self._update_task_state_machine(observed_task.task_id, desired_state, durable_state.get("state_version", 1) if durable_state else 1)
                
                # 3. Plan recovery if not in OBSERVE mode
                if self.mode != "OBSERVE":
                    plans = self.planner.plan_recovery(
                        observed_task, classification, self.store, self.watchdog_id,
                        durable_state=durable_state, checkpoint_valid=checkpoint_valid,
                        invalidators=invalidators,
                    )
                    recoveries_planned += len(plans)
                    
                    # Execute recoveries (respecting canary limits)
                    for plan in plans:
                        if self._should_execute_recovery(plan, cycle_number):
                            executed = self._execute_recovery(plan, observed_task)
                            if executed:
                                recoveries_executed += 1
            
            # 4. Cleanup expired leases
            self.lease_manager.cleanup_expired_leases()
            
            # 5. Prune old data periodically
            if cycle_number % 100 == 0:
                retention = self.config["watchdog"]["persistence"].get("retention_days", 30)
                self.store.prune_old_data(retention)
            
        except Exception as e:
            error = str(e)
            logger.error(f"Scan cycle {cycle_number} error: {e}", exc_info=True)
        
        elapsed = time.time() - start_time
        
        result = ScanCycleResult(
            cycle_number=cycle_number,
            started_at=start_time,
            completed_at=time.time(),
            tasks_discovered=tasks_discovered,
            tasks_classified=tasks_classified,
            recoveries_planned=recoveries_planned,
            recoveries_executed=recoveries_executed,
            mode=self.mode,
            error=error
        )
        
        # Record run
        self.store.record_run_complete(
            cycle_number=cycle_number,
            tasks_discovered=tasks_discovered,
            tasks_classified=tasks_classified,
            recoveries_planned=recoveries_planned,
            recoveries_executed=recoveries_executed,
            error=error or "",
            metadata_json=f'{{"elapsed_seconds": {elapsed:.2f}}}'
        )
        
        # Shadow mode logging
        if self.mode == "OBSERVE":
            self._log_shadow_decisions(cycle_number)
        
        return result
    
    def _upsert_task_record(self, task):
        """Upsert task record in store."""
        from persistence.sqlite_store import TaskRecord
        record = TaskRecord(
            task_id=task.task_id,
            session_id=task.session_id,
            session_key=task.session_key,
            source=task.source,
            platform=task.platform,
            cwd=task.cwd,
            git_repo_root=task.git_repo_root,
            first_seen_at=task.started_at,
            last_seen_at=time.time(),
            last_activity_at=task.last_activity_at,
            last_activity_description=task.last_activity_description,
            structured_state=task.structured_state,
            is_active=1 if task.is_active else 0,
            metadata_json=__import__('json').dumps(task.metadata)
        )
        self.store.upsert_task(record)
    
    def _record_observation(self, task, cycle_number: int, classification):
        """Record observation in store."""
        from persistence.sqlite_store import ObservationRecord
        import json
        
        record = ObservationRecord(
            task_id=task.task_id,
            scan_cycle=cycle_number,
            observed_at=time.time(),
            structured_state=task.structured_state,
            last_agent_event_at=task.last_agent_event_at,
            last_tool_start_at=task.last_tool_start_at,
            last_tool_end_at=task.last_tool_end_at,
            last_provider_request_at=task.last_provider_request_at,
            provider_request_state=task.provider_request_state,
            subprocess_active=1 if task.subprocess_active else 0,
            session_state=task.session_state,
            worker_process_alive=1 if task.worker_process_alive else 0,
            explicit_markers_json=json.dumps(task.explicit_markers),
            raw_evidence_json=json.dumps({
                "time_since_activity": time.time() - task.last_activity_at,
                "time_since_agent_event": time.time() - task.last_agent_event_at,
                "metadata": task.metadata
            }),
            classification=classification.state,
            classification_confidence=classification.confidence
        )
        self.store.record_observation(record)
    
    def _should_execute_recovery(self, plan, cycle_number: int) -> bool:
            """Check if recovery should be executed based on mode."""
            if self.mode == "OBSERVE":
                return False

            fault_domain = (plan.fault_envelope or {}).get("domain")

            if self.mode == "ACTIVE_CANARY":
                # Only one recovery at a time in canary
                if self.lease_manager._held_leases:
                    return False
                # Context recovery must stay on compaction primitive.
                if fault_domain == "context":
                    if plan.action_type != "COMPACT_CONTEXT" and self.capabilities.can_compact_context:
                        return False
                # Only transport recovery and read-only verification are allowed in canary.
                elif fault_domain not in ("transport", "verify"):
                    return False
                # Prefer the lowest-risk transport primitive.
                elif fault_domain == "transport" and plan.action_type not in ("RETRY_TRANSPORT", "VERIFY_RECOVERY") and self.capabilities.can_retry_transport:
                    return False
            elif self.mode == "ACTIVE_GLOBAL":
                # In global mode context recovery still requires the compaction primitive.
                if fault_domain == "context":
                    if plan.action_type != "COMPACT_CONTEXT" and self.capabilities.can_compact_context:
                        return False

            return True
    
    def _execute_recovery(self, plan, task) -> bool:
        """Execute a recovery plan."""
        logger.info(f"Executing recovery: {plan.action_type} for task {plan.task_id}")
        
        # Acquire lease
        if plan.lease_required:
            if not self.lease_manager.try_acquire_lease(plan.task_id, plan.action_id):
                logger.warning(f"Could not acquire lease for {plan.task_id}")
                return False
        
        # Record attempt
        self.store.record_recovery_attempt(
            task_id=plan.task_id,
            action_id=plan.action_id,
            action_type=plan.action_type,
            error_fingerprint_hash=plan.failure_class,
            lease_id=plan.action_id if plan.lease_required else None
        )
        
        self.store.update_recovery_attempt(plan.action_id, "executing")
        self._record_recovery_started(plan.task_id, plan.action_type)
        
        try:
            durable_state = self.store.get_task_state_machine(plan.task_id) or {}
            checkpoint_valid = True
            if durable_state.get("checkpoint_hash"):
                checkpoint_valid = self._validate_checkpoint(task, durable_state)

            fresh_invalidators = self._detect_invalidators(
                task, durable_state, checkpoint_valid=checkpoint_valid
            )
            if fresh_invalidators:
                verification = {
                    "verified": False,
                    "effect_state": "BLOCKED",
                    "details": "Fresh invalidator authority blocks mutating recovery execution",
                    "evidence": {"invalidators": fresh_invalidators, "task_id": plan.task_id},
                }
                self.store.update_recovery_attempt(
                    plan.action_id, "failed",
                    result_json=self._json_dumps({"verification": verification}),
                    evidence_json=self._json_dumps(verification["evidence"])
                )
                self._record_recovery_completed(plan.task_id, plan.action_type, False)
                return False

            checkpoint_hash = self._ensure_recovery_checkpoint(task, plan, durable_state)
            execution_params = dict(plan.params)
            execution_params["_fault_envelope"] = plan.fault_envelope or {}
            execution_params["_checkpoint_hash"] = checkpoint_hash

            # Execute via adapter
            result = self.adapter.execute_recovery(plan.action_type, task, execution_params)
            verification = self.adapter.verify_recovery_effect(
                plan.action_type, task, execution_params, result
            )
            combined_result = {
                "result": result,
                "verification": verification,
                "fault_envelope": plan.fault_envelope or {},
                "checkpoint_hash": checkpoint_hash,
            }
            
            # Record action with idempotency key
            executed = self.store.record_recovery_action(
                action_id=plan.action_id,
                task_id=plan.task_id,
                action_type=plan.action_type,
                idempotency_key=plan.idempotency_key,
                result_summary=self._json_dumps(combined_result)
            )
            
            if executed:
                status = "executed" if verification.get("verified") else (
                    "failed" if plan.action_type == "VERIFY_RECOVERY" else "executing"
                )
                self.store.update_recovery_attempt(
                    plan.action_id, status,
                    result_json=self._json_dumps(combined_result),
                    evidence_json=self._json_dumps(verification.get("evidence", {"task_id": plan.task_id}))
                )
                self._record_recovery_completed(plan.task_id, plan.action_type, bool(verification.get("verified")))
                if verification.get("verified"):
                    logger.info(f"Recovery effect verified: {plan.action_id}")
                else:
                    logger.warning(f"Recovery effect not yet verified: {plan.action_id}")
            else:
                self.store.update_recovery_attempt(
                    plan.action_id, "skipped",
                    result_json="Idempotency key already exists"
                )
                logger.info(f"Recovery skipped (idempotent): {plan.action_id}")
            
            return executed and bool(verification.get("verified"))
            
        except Exception as e:
            logger.error(f"Recovery execution failed: {e}")
            self.store.update_recovery_attempt(
                plan.action_id, "failed",
                result_json=f"Error: {e}"
            )
            self._record_recovery_completed(plan.task_id, plan.action_type, False)
            return False
        finally:
            if plan.lease_required:
                self.lease_manager.release_lease(plan.task_id, plan.action_id)
    
    def _log_shadow_decisions(self, cycle_number: int):
        """Log shadow mode decisions for promotion evidence."""
        # Get recent observations with planned actions
        runs = self.store.get_recent_runs(1)
        if runs:
            run = runs[0]
            logger.info(f"SHADOW cycle {cycle_number}: "
                       f"discovered={run['tasks_discovered']} "
                       f"classified={run['tasks_classified']} "
                       f"would_retry={run['recoveries_planned']} "
                       f"would_execute={run['recoveries_executed']}")

    # V3: Durable reconciliation helpers
    def _compute_desired_state(self, observed_task, durable_state, classification):
        """Compute desired state from observation and durable state."""
        desired = {}
        
        # Start with durable state as baseline
        if durable_state:
            desired.update({
                "program_id": durable_state.get("program_id", "unknown"),
                "generation": durable_state.get("generation", 1),
                "goal": durable_state.get("goal"),
                "capability": durable_state.get("capability"),
                "first_unproven_boundary": durable_state.get("first_unproven_boundary"),
                "accepted_baseline": durable_state.get("accepted_baseline"),
                "completed_boundaries_json": durable_state.get("completed_boundaries_json"),
                "active_writer_identity": durable_state.get("active_writer_identity"),
                "active_transaction_id": durable_state.get("active_transaction_id"),
                "pending_action": durable_state.get("pending_action"),
                "last_completed_action": durable_state.get("last_completed_action"),
                "side_effect_state": durable_state.get("side_effect_state", "NONE"),
                "checkpoint_hash": durable_state.get("checkpoint_hash"),
            })
        
        # Update from observation
        desired["task_id"] = observed_task.task_id
        desired["session_id"] = observed_task.session_id
        
        # Update from classification
        if classification.state == "HEALTHY":
            desired["side_effect_state"] = "NONE"
        elif classification.state == "RECOVERY_PENDING":
            desired["pending_action"] = classification.recovery_action
        elif classification.state == "TERMINAL_COMPLETE":
            desired["side_effect_state"] = "KNOWN_COMPLETE"
        elif classification.state == "TERMINAL_BLOCKED":
            desired["side_effect_state"] = "KNOWN_FAILED"
        
        return desired

    def _reconcile_state(self, observed_task, durable_state, desired_state, classification):
        """Reconcile observed state with durable desired state."""
        # If no durable state, initialize new
        if not durable_state:
            return "INITIALIZE"
        
        # Check for side effect UNKNOWN
        if durable_state.get("side_effect_state") == "UNKNOWN":
            return "RECONCILE_SIDE_EFFECT"
        
        # Check for completed boundaries that should not be replayed
        if durable_state.get("completed_boundaries_json"):
            import json
            completed = json.loads(durable_state["completed_boundaries_json"])
            # If we're at a boundary that's already completed, don't replay
            if desired_state.get("first_unproven_boundary") in completed:
                return "SKIP_COMPLETED_BOUNDARY"
        
        # Check for active writer continuity
        if durable_state.get("active_writer_identity"):
            if not self._is_writer_alive(durable_state["active_writer_identity"]):
                return "WRITER_DIED"
        
        # Check for invalidators (new evidence that invalidates durable state)
        invalidators = self._detect_invalidators(observed_task, durable_state)
        if invalidators:
            return "INVALIDATE"
        
        # Default: continue with current plan
        return "CONTINUE"

    def _state_changed(self, durable_state, desired_state):
        """Check if state has changed in a way that requires event."""
        key_fields = [
            "side_effect_state", "pending_action", "last_completed_action",
            "first_unproven_boundary", "completed_boundaries_json",
            "active_writer_identity", "active_transaction_id",
            "pending_action", "side_effect_state"
        ]
        for field in key_fields:
            if durable_state.get(field) != desired_state.get(field):
                return True
        return False

    def _record_state_transition(self, task_id, old_state, new_state, action):
        """Record a state transition event."""
        event_data = {
            "old_state": {k: old_state.get(k) for k in ["side_effect_state", "pending_action", "last_completed_action", "first_unproven_boundary", "completed_boundaries_json", "active_writer_identity", "active_transaction_id"]},
            "new_state": {k: new_state.get(k) for k in ["side_effect_state", "pending_action", "last_completed_action", "first_unproven_boundary", "completed_boundaries_json", "active_writer_identity", "active_transaction_id"]},
            "reconciliation_action": action,
        }
        event_identity = f"state_transition:{task_id}:{int(time.time() * 1000)}"
        self.store.record_task_event(
            task_id=task_id,
            event_type="TASK_STATE_CHANGED",
            event_data=event_data,
            event_identity=event_identity,
            source_component="watchdog_reconciler"
        )

    def _record_worker_started(self, task_id, worker_identity):
        """Record worker started event."""
        event_identity = f"worker_started:{task_id}:{worker_identity}:{int(time.time() * 1000)}"
        self.store.record_task_event(
            task_id=task_id,
            event_type="WORKER_STARTED",
            event_data={"worker_identity": worker_identity},
            event_identity=event_identity,
            source_component="watchdog_reconciler"
        )

    def _record_worker_exited(self, task_id, worker_identity, exit_reason):
        """Record worker exited event."""
        event_identity = f"worker_exited:{task_id}:{worker_identity}:{int(time.time() * 1000)}"
        self.store.record_task_event(
            task_id=task_id,
            event_type="WORKER_EXITED",
            event_data={"worker_identity": worker_identity, "exit_reason": exit_reason},
            event_identity=event_identity,
            source_component="watchdog_reconciler"
        )

    def _record_checkpoint_created(self, task_id, checkpoint_hash, boundary_name):
        """Record checkpoint created event."""
        event_identity = f"checkpoint:{task_id}:{boundary_name}:{int(time.time() * 1000)}"
        self.store.record_task_event(
            task_id=task_id,
            event_type="CHECKPOINT_CREATED",
            event_data={"checkpoint_hash": checkpoint_hash, "boundary_name": boundary_name},
            event_identity=event_identity,
            source_component="watchdog_reconciler"
        )

    def _record_recovery_planned(self, task_id, recovery_action, failure_class):
        """Record recovery planned event."""
        event_identity = f"recovery_planned:{task_id}:{recovery_action}:{int(time.time() * 1000)}"
        self.store.record_task_event(
            task_id=task_id,
            event_type="RECOVERY_PLANNED",
            event_data={"recovery_action": recovery_action, "failure_class": failure_class},
            event_identity=event_identity,
            source_component="watchdog_reconciler"
        )

    def _record_recovery_started(self, task_id, recovery_action):
        """Record recovery started event."""
        event_identity = f"recovery_started:{task_id}:{recovery_action}:{int(time.time() * 1000)}"
        self.store.record_task_event(
            task_id=task_id,
            event_type="RECOVERY_STARTED",
            event_data={"recovery_action": recovery_action},
            event_identity=event_identity,
            source_component="watchdog_reconciler"
        )

    def _record_recovery_completed(self, task_id, recovery_action, success):
        """Record recovery completed event."""
        event_identity = f"recovery_completed:{task_id}:{recovery_action}:{int(time.time() * 1000)}"
        self.store.record_task_event(
            task_id=task_id,
            event_type="RECOVERY_COMPLETED",
            event_data={"recovery_action": recovery_action, "success": success},
            event_identity=event_identity,
            source_component="watchdog_reconciler"
        )

    def _record_side_effect_unknown(self, task_id, side_effect_type, details):
        """Record side effect unknown event."""
        event_identity = f"side_effect_unknown:{task_id}:{side_effect_type}:{int(time.time() * 1000)}"
        self.store.record_task_event(
            task_id=task_id,
            event_type="SIDE_EFFECT_UNKNOWN",
            event_data={"side_effect_type": side_effect_type, "details": details},
            event_identity=event_identity,
            source_component="watchdog_reconciler"
        )

    def _record_side_effect_reconciled(self, task_id, side_effect_type, new_state, evidence):
        """Record side effect reconciled event."""
        event_identity = f"side_effect_reconciled:{task_id}:{side_effect_type}:{int(time.time() * 1000)}"
        self.store.record_task_event(
            task_id=task_id,
            event_type="SIDE_EFFECT_RECONCILED",
            event_data={"side_effect_type": side_effect_type, "new_state": new_state, "evidence": evidence},
            event_identity=event_identity,
            source_component="watchdog_reconciler"
        )

    def _record_task_completed(self, task_id, completed_boundaries, final_state):
        """Record task completed event."""
        event_identity = f"task_completed:{task_id}:{int(time.time() * 1000)}"
        self.store.record_task_event(
            task_id=task_id,
            event_type="TASK_COMPLETED",
            event_data={"completed_boundaries": completed_boundaries, "final_state": final_state},
            event_identity=event_identity,
            source_component="watchdog_reconciler"
        )

    def _update_task_state_machine(self, task_id, desired_state, expected_version):
        """Update task state machine with optimistic locking."""
        max_retries = 3
        for attempt in range(max_retries):
            success = self.store.increment_state_version(task_id, expected_version)
            if success:
                # Version incremented, now upsert with new version
                desired_state["state_version"] = expected_version + 1
                self.store.upsert_task_state_machine(desired_state)
                return True
            else:
                # Retry with fresh version
                current = self.store.get_task_state_machine(task_id)
                if current:
                    expected_version = current.get("state_version", 1)
                else:
                    break
        logger.warning(f"Failed to update state machine for {task_id} after {max_retries} retries")
        return False

    def _detect_invalidators(self, observed_task, durable_state, checkpoint_valid: bool = True):
        """Detect new evidence that invalidates durable state."""
        invalidators = []
        
        # Check for new provider error
        if observed_task.provider_request_state and durable_state:
            # If we have a new error that contradicts durable state
            if durable_state.get("pending_action") or durable_state.get("side_effect_state") == "KNOWN_COMPLETE":
                invalidators.append("PROVIDER_STATE_CONTRADICTION")
        
        if durable_state.get("checkpoint_hash") and not checkpoint_valid:
            invalidators.append("CHECKPOINT_MISMATCH")
        
        # Check for worker death
        if durable_state.get("active_writer_identity") and not observed_task.worker_process_alive:
            invalidators.append("WRITER_DEAD")
        
        # Check for explicit invalidation markers
        if observed_task.explicit_markers.get("invalidate_checkpoint"):
            invalidators.append("EXPLICIT_INVALIDATION")
        
        return invalidators

    def _validate_checkpoint(self, observed_task, durable_state):
        """Validate the durable checkpoint hash against current observed state."""
        expected = durable_state.get("checkpoint_hash")
        if not expected:
            return True
        current_hash, _ = build_checkpoint_snapshot(observed_task, durable_state)
        return current_hash == expected

    def _ensure_recovery_checkpoint(self, task, plan, durable_state):
        """Persist a deterministic checkpoint before mutating recovery primitives."""
        if plan.action_type == "VERIFY_RECOVERY":
            return durable_state.get("checkpoint_hash")

        checkpoint_hash, _ = build_checkpoint_snapshot(
            task,
            durable_state,
            None if not plan.fault_envelope else type("FaultView", (), {"to_dict": lambda self: plan.fault_envelope})()
        )
        if durable_state.get("checkpoint_hash") == checkpoint_hash:
            return checkpoint_hash

        checkpoint_state = dict(durable_state)
        checkpoint_state.update({
            "task_id": task.task_id,
            "program_id": durable_state.get("program_id") or f"program:{task.task_id}",
            "generation": durable_state.get("generation", 1),
            "goal": durable_state.get("goal") or task.metadata.get("goal"),
            "capability": durable_state.get("capability") or task.metadata.get("capability") or "codex",
            "first_unproven_boundary": durable_state.get("first_unproven_boundary")
                or (plan.fault_envelope or {}).get("evidence", {}).get("first_unproven_boundary")
                or (plan.fault_envelope or {}).get("kind")
                or plan.action_type,
            "active_writer_identity": durable_state.get("active_writer_identity"),
            "active_transaction_id": durable_state.get("active_transaction_id"),
            "pending_action": durable_state.get("pending_action") or plan.action_type,
            "last_completed_action": durable_state.get("last_completed_action"),
            "side_effect_state": durable_state.get("side_effect_state", "NONE"),
            "checkpoint_hash": checkpoint_hash,
            "state_version": durable_state.get("state_version", 1),
        })
        self.store.upsert_task_state_machine(checkpoint_state)
        self._record_checkpoint_created(
            task.task_id,
            checkpoint_hash,
            checkpoint_state["first_unproven_boundary"],
        )
        return checkpoint_hash

    def _json_dumps(self, payload):
        """Serialize JSON payloads for durable evidence storage."""
        return json.dumps(payload, default=str, sort_keys=True)

    def _is_writer_alive(self, writer_identity):
        """Check if writer process is still alive."""
        try:
            # Parse writer identity: "type:pid:start_time:cmd"
            parts = writer_identity.split(":")
            if len(parts) >= 3 and parts[1] == "pid":
                pid = int(parts[2])
                import psutil
                proc = psutil.Process(pid)
                if proc.is_running():
                    # Verify start time matches
                    create_time = int(proc.create_time() * 1000)
                    # Extract start_time from identity
                    start_time_part = [p for p in writer_identity.split(":") if p.startswith("start_time:")]
                    if start_time_part:
                        expected_start = int(start_time_part[0].split(":")[1])
                        if abs(create_time - expected_start) < 10000:
                            return True
            return False
        except Exception:
            return False

def main():
    parser = argparse.ArgumentParser(description="Hermes Watchdog V1")
    parser.add_argument("--config", default="~/AppData/Local/hermes/hermes-watchdog/config/watchdog.yaml",
                       help="Path to config file")
    parser.add_argument("--mode", choices=["OBSERVE", "ACTIVE_CANARY", "ACTIVE_GLOBAL"],
                       help="Override watchdog mode")
    parser.add_argument("--once", action="store_true",
                       help="Run single scan cycle and exit")
    parser.add_argument("--status", action="store_true",
                       help="Show watchdog status and exit")
    args = parser.parse_args()
    
    watchdog = HermesWatchdog(args.config)
    
    if args.mode:
        watchdog.mode = args.mode
        watchdog.config["watchdog"]["mode"] = args.mode
    
    if args.status:
        # Show status
        runs = watchdog.store.get_recent_runs(5)
        print(f"Watchdog ID: {watchdog.watchdog_id}")
        print(f"Mode: {watchdog.mode}")
        print(f"Capabilities: {watchdog.capabilities}")
        print(f"Recent runs: {len(runs)}")
        for run in runs:
            print(f"  Cycle {run['cycle_number']}: {run['tasks_discovered']} tasks, "
                  f"{run['recoveries_planned']} planned, {run['recoveries_executed']} executed")
        return 0
    
    if args.once:
        watchdog.start()
        watchdog.scheduler.trigger_scan()
        watchdog.stop()
        return 0
    
    # Set up signal handlers
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        watchdog.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Run
    watchdog.start()
    
    # Keep running
    try:
        while not watchdog._shutdown:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        watchdog.stop()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
