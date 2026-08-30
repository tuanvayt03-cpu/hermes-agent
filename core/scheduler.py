"""
Hermes Watchdog V1 - Scheduler and Lease Manager

Manages the 60-second scan cycle and recovery leases.
"""

import logging
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

@dataclass
class ScanCycleResult:
    """Result of a single scan cycle."""
    cycle_number: int
    started_at: float
    completed_at: float
    tasks_discovered: int
    tasks_classified: int
    recoveries_planned: int
    recoveries_executed: int
    mode: str
    error: Optional[str] = None

class LeaseManager:
    """Manages exclusive recovery leases."""
    
    def __init__(self, store: 'WatchdogStore', config: Dict):
        self.store = store
        self.config = config
        self.lease_config = config.get("lease", {})
        self.default_ttl = self.lease_config.get("default_ttl_seconds", 300)
        self.max_generation = self.lease_config.get("max_generation", 1000)
        self._watchdog_id = f"watchdog_{uuid.uuid4().hex[:8]}"
        self._current_generation = 0
        self._held_leases: Dict[str, str] = {}  # task_id -> action_id
    
    @property
    def watchdog_id(self) -> str:
        return self._watchdog_id
    
    def try_acquire_lease(self, task_id: str, action_id: str) -> bool:
        """Try to acquire exclusive lease for a task recovery."""
        # Check if we already hold a valid lease
        if task_id in self._held_leases:
            lease = self.store.get_lease(task_id)
            if lease and lease['recovery_owner'] == self._watchdog_id:
                if lease['lease_until'] > time.time():
                    return True
                # Lease expired, release it
                self.release_lease(task_id, self._held_leases[task_id])
                del self._held_leases[task_id]
        
        # Try to acquire new lease
        self._current_generation = (self._current_generation + 1) % self.max_generation
        acquired = self.store.acquire_lease(
            task_id=task_id,
            owner=self._watchdog_id,
            ttl_seconds=self.default_ttl,
            generation=self._current_generation,
            action_id=action_id
        )
        
        if acquired:
            self._held_leases[task_id] = action_id
            logger.info(f"Acquired lease for task {task_id}, action {action_id}")
        else:
            # Check who holds it
            lease = self.store.get_lease(task_id)
            if lease:
                logger.debug(f"Lease for {task_id} held by {lease['recovery_owner']} until {lease['lease_until']}")
        
        return acquired
    
    def release_lease(self, task_id: str, action_id: str) -> bool:
        """Release a held lease."""
        released = self.store.release_lease(task_id, self._watchdog_id, action_id)
        if released:
            self._held_leases.pop(task_id, None)
            logger.info(f"Released lease for task {task_id}, action {action_id}")
        return released
    
    def is_lease_valid(self, task_id: str) -> bool:
        """Check if we hold a valid lease for a task."""
        if task_id not in self._held_leases:
            return False
        return self.store.is_lease_valid(task_id, self._watchdog_id)
    
    def renew_lease(self, task_id: str, action_id: str) -> bool:
        """Renew an existing lease (extend TTL)."""
        if not self.is_lease_valid(task_id):
            return False
        
        # Acquire with same generation extends the lease
        return self.try_acquire_lease(task_id, action_id)
    
    def cleanup_expired_leases(self):
        """Clean up locally tracked leases that have expired."""
        expired = []
        for task_id, action_id in self._held_leases.items():
            if not self.store.is_lease_valid(task_id, self._watchdog_id):
                expired.append(task_id)
        
        for task_id in expired:
            del self._held_leases[task_id]
            logger.debug(f"Cleaned up expired lease for {task_id}")

class Scheduler:
    """Runs the watchdog scan cycle at configured intervals."""
    
    def __init__(self, config: Dict, scan_callback: Callable[[int], ScanCycleResult]):
        self.config = config
        self.scan_interval = config.get("scan_interval_seconds", 60)
        self.scan_callback = scan_callback
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._cycle_number = 0
        self._stop_event = threading.Event()
        self._last_run_time = 0
    
    def start(self):
        """Start the scheduler."""
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info(f"Watchdog scheduler started (interval: {self.scan_interval}s)")
    
    def stop(self):
        """Stop the scheduler."""
        if not self._running:
            return
        self._running = False
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=10)
        logger.info("Watchdog scheduler stopped")
    
    def _run_loop(self):
        """Main scheduler loop."""
        while self._running and not self._stop_event.is_set():
            cycle_start = time.time()
            
            # Run scan cycle
            self._cycle_number += 1
            try:
                result = self.scan_callback(self._cycle_number)
                self._last_run_time = time.time()
                
                # Log cycle result
                logger.info(
                    f"Scan cycle {self._cycle_number} complete: "
                    f"{result.tasks_discovered} tasks, {result.tasks_classified} classified, "
                    f"{result.recoveries_planned} planned, {result.recoveries_executed} executed"
                )
            except Exception as e:
                logger.error(f"Scan cycle {self._cycle_number} failed: {e}")
            
            # Calculate sleep time to maintain interval
            elapsed = time.time() - cycle_start
            sleep_time = max(0, self.scan_interval - elapsed)
            
            if sleep_time > 0:
                self._stop_event.wait(sleep_time)
    
    def trigger_scan(self) -> ScanCycleResult:
        """Manually trigger a scan cycle (for testing)."""
        self._cycle_number += 1
        return self.scan_callback(self._cycle_number)
    
    @property
    def cycle_number(self) -> int:
        return self._cycle_number
    
    @property
    def is_running(self) -> bool:
        return self._running