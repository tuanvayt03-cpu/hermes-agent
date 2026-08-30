"""
Hermes Watchdog V1 - Main Entrypoint

Global, project-agnostic watchdog that supervises all Hermes/Codex tasks.
"""

import argparse
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
        """Execute a single scan cycle."""
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
                
                # 3. Plan recovery if not in OBSERVE mode
                if self.mode != "OBSERVE":
                    plans = self.planner.plan_recovery(
                        observed_task, classification, self.store, self.watchdog_id
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

            if self.mode == "ACTIVE_CANARY":
                # Only one recovery at a time in canary
                if self.lease_manager._held_leases:
                    return False
                # CONTEXT_WINDOW_EXCEEDED uses COMPACT_CONTEXT - allow in canary
                if plan.failure_class == "CONTEXT_WINDOW_EXCEEDED":
                    if plan.action_type != "COMPACT_CONTEXT" and self.capabilities.can_compact_context:
                        return False
                # Only low-risk transient cases
                elif plan.failure_class not in ("PROVIDER_OVERLOAD", "NETWORK_TRANSIENT"):
                    return False
                # Prefer RETRY_TRANSPORT for transient
                elif plan.action_type != "RETRY_TRANSPORT" and self.capabilities.can_retry_transport:
                    return False
            elif self.mode == "ACTIVE_GLOBAL":
                # For CONTEXT_WINDOW_EXCEEDED in ACTIVE_GLOBAL, allow COMPACT_CONTEXT
                if plan.failure_class == "CONTEXT_WINDOW_EXCEEDED":
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
        
        try:
            # Execute via adapter
            result = self.adapter.execute_recovery(plan.action_type, task, plan.params)
            
            # Record action with idempotency key
            executed = self.store.record_recovery_action(
                action_id=plan.action_id,
                task_id=plan.task_id,
                action_type=plan.action_type,
                idempotency_key=plan.idempotency_key,
                result_summary=str(result)
            )
            
            if executed:
                self.store.update_recovery_attempt(
                    plan.action_id, "executed", 
                    result_json=str(result),
                    evidence_json=f'{{"task_id": "{plan.task_id}"}}'
                )
                logger.info(f"Recovery executed successfully: {plan.action_id}")
            else:
                self.store.update_recovery_attempt(
                    plan.action_id, "skipped",
                    result_json="Idempotency key already exists"
                )
                logger.info(f"Recovery skipped (idempotent): {plan.action_id}")
            
            return executed
            
        except Exception as e:
            logger.error(f"Recovery execution failed: {e}")
            self.store.update_recovery_attempt(
                plan.action_id, "failed",
                result_json=f"Error: {e}"
            )
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