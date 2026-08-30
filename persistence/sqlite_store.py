"""
Hermes Watchdog V1 - SQLite Persistence Layer

Provides durable storage for:
- tasks: discovered task identities and metadata
- observations: scan observations per task
- error_fingerprints: classified error signatures
- recovery_attempts: attempted recoveries with outcomes
- recovery_actions: executed recovery actions with idempotency keys
- recovery_leases: exclusive recovery ownership
- promotion_state: watchdog rollout state
- watchdog_runs: scan cycle history
- configuration_version: config version tracking
"""

import json
import logging
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Iterator
from dataclasses import dataclass, asdict
from datetime import datetime

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 3

SCHEMA_SQL = """
-- Tasks discovered by the watchdog
CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    session_id TEXT,
    session_key TEXT,
    source TEXT,
    platform TEXT,
    cwd TEXT,
    git_repo_root TEXT,
    first_seen_at REAL NOT NULL,
    last_seen_at REAL NOT NULL,
    last_activity_at REAL,
    last_activity_description TEXT,
    structured_state TEXT,
    is_active INTEGER DEFAULT 1,
    metadata_json TEXT
);

-- Observations recorded each scan cycle
CREATE TABLE IF NOT EXISTS observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    scan_cycle INTEGER NOT NULL,
    observed_at REAL NOT NULL,
    structured_state TEXT,
    last_agent_event_at REAL,
    last_tool_start_at REAL,
    last_tool_end_at REAL,
    last_provider_request_at REAL,
    provider_request_state TEXT,
    subprocess_active INTEGER,
    session_state TEXT,
    worker_process_alive INTEGER,
    explicit_markers_json TEXT,
    raw_evidence_json TEXT,
    classification TEXT,
    classification_confidence REAL,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id)
);
CREATE INDEX IF NOT EXISTS idx_observations_task_scan ON observations(task_id, scan_cycle);

-- Error fingerprints for classification
CREATE TABLE IF NOT EXISTS error_fingerprints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    fingerprint_hash TEXT NOT NULL,
    error_class TEXT NOT NULL,
    error_message TEXT,
    structured_error_json TEXT,
    first_seen_at REAL NOT NULL,
    last_seen_at REAL NOT NULL,
    occurrence_count INTEGER DEFAULT 1,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_error_fp_task_hash ON error_fingerprints(task_id, fingerprint_hash);

-- Task checkpoints for context recovery (V2)
CREATE TABLE IF NOT EXISTS task_checkpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    checkpoint_generation INTEGER NOT NULL,
    checkpoint_hash TEXT NOT NULL,
    created_at REAL NOT NULL,
    master_task_id TEXT,
    session_id TEXT,
    task_title TEXT,
    current_goal TEXT,
    current_capability TEXT,
    first_unproven_boundary TEXT,
    accepted_baseline TEXT,
    accepted_markers_json TEXT,
    current_repo TEXT,
    branch TEXT,
    head_tree_hash TEXT,
    active_writer_identity TEXT,
    transaction_id TEXT,
    changed_paths_json TEXT,
    current_diff_hash TEXT,
    last_completed_action TEXT,
    pending_action TEXT,
    completed_boundaries TEXT,
    machine_evidence_json TEXT,
    open_invalidators_json TEXT,
    hard_safety_invariants_json TEXT,
    tool_process_states_json TEXT,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id)
);
CREATE INDEX IF NOT EXISTS idx_checkpoints_task_gen ON task_checkpoints(task_id, checkpoint_generation);

-- V3: Canonical task execution state machine
CREATE TABLE IF NOT EXISTS task_state_machine (
    task_id TEXT PRIMARY KEY,
    program_id TEXT NOT NULL,
    generation INTEGER NOT NULL DEFAULT 1,
    
    -- Goal and capability
    goal TEXT,
    capability TEXT,
    
    -- Boundaries
    first_unproven_boundary TEXT,
    accepted_baseline TEXT,
    completed_boundaries_json TEXT,
    
    -- Writer/transaction ownership
    active_writer_identity TEXT,
    active_transaction_id TEXT,
    
    -- Action tracking
    pending_action TEXT,
    last_completed_action TEXT,
    
    -- Side effect state: NONE, KNOWN_COMPLETE, KNOWN_FAILED, UNKNOWN
    side_effect_state TEXT NOT NULL DEFAULT 'NONE',
    
    -- State version for optimistic locking
    state_version INTEGER NOT NULL DEFAULT 1,
    
    -- Checkpoint verification
    checkpoint_hash TEXT,
    
    -- Timestamps
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    
    FOREIGN KEY (task_id) REFERENCES tasks(task_id)
);
CREATE INDEX IF NOT EXISTS idx_state_machine_program ON task_state_machine(program_id);

-- V3: Event journal for lifecycle transitions
CREATE TABLE IF NOT EXISTS task_execution_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    event_type TEXT NOT NULL,  -- TASK_STATE_CHANGED, WORKER_STARTED, WORKER_EXITED, CHECKPOINT_CREATED, RECOVERY_PLANNED, RECOVERY_STARTED, RECOVERY_COMPLETED, SIDE_EFFECT_UNKNOWN, SIDE_EFFECT_RECONCILED, TASK_COMPLETED
    event_data_json TEXT NOT NULL,
    event_identity TEXT NOT NULL UNIQUE,  -- Stable event identity for deduplication
    occurred_at REAL NOT NULL,
    source_component TEXT NOT NULL,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id)
);
CREATE INDEX IF NOT EXISTS idx_events_task_time ON task_execution_events(task_id, occurred_at);
CREATE INDEX IF NOT EXISTS idx_events_identity ON task_execution_events(event_identity);

-- Recovery attempts (planned + executed)
CREATE TABLE IF NOT EXISTS recovery_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    action_id TEXT NOT NULL UNIQUE,
    action_type TEXT NOT NULL,
    error_fingerprint_hash TEXT,
    planned_at REAL NOT NULL,
    executed_at REAL,
    lease_id TEXT,
    status TEXT NOT NULL,  -- planned | executing | executed | failed | skipped
    result_json TEXT,
    evidence_json TEXT,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id)
);
CREATE INDEX IF NOT EXISTS idx_recovery_attempts_task ON recovery_attempts(task_id);

-- Recovery actions executed (idempotency record)
CREATE TABLE IF NOT EXISTS recovery_actions (
    action_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    action_type TEXT NOT NULL,
    executed_at REAL NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    result_summary TEXT,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id)
);

-- Recovery leases for exclusive ownership
CREATE TABLE IF NOT EXISTS recovery_leases (
    task_id TEXT PRIMARY KEY,
    recovery_owner TEXT NOT NULL,
    lease_started_at REAL NOT NULL,
    lease_until REAL NOT NULL,
    generation INTEGER NOT NULL,
    action_id TEXT,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id)
);

-- Watchdog promotion state (OBSERVE -> ACTIVE_CANARY -> ACTIVE_GLOBAL)
CREATE TABLE IF NOT EXISTS promotion_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at REAL NOT NULL
);

-- Watchdog scan run history
CREATE TABLE IF NOT EXISTS watchdog_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cycle_number INTEGER NOT NULL UNIQUE,
    started_at REAL NOT NULL,
    completed_at REAL,
    tasks_discovered INTEGER DEFAULT 0,
    tasks_classified INTEGER DEFAULT 0,
    recoveries_planned INTEGER DEFAULT 0,
    recoveries_executed INTEGER DEFAULT 0,
    mode TEXT NOT NULL,
    error TEXT,
    metadata_json TEXT
);

-- Configuration version tracking
CREATE TABLE IF NOT EXISTS configuration_version (
    version INTEGER PRIMARY KEY,
    config_hash TEXT NOT NULL,
    applied_at REAL NOT NULL,
    metadata_json TEXT
);

-- WAL mode pragma (applied at connection time)
-- PRAGMA journal_mode=WAL;
-- PRAGMA synchronous=NORMAL;
-- PRAGMA busy_timeout=5000;
"""

@dataclass
class TaskRecord:
    task_id: str
    session_id: str
    session_key: str
    source: str
    platform: str
    cwd: str
    git_repo_root: str
    first_seen_at: float
    last_seen_at: float
    last_activity_at: float
    last_activity_description: str
    structured_state: str
    is_active: int
    metadata_json: str

@dataclass
class ObservationRecord:
    task_id: str
    scan_cycle: int
    observed_at: float
    structured_state: str
    last_agent_event_at: float
    last_tool_start_at: float
    last_tool_end_at: float
    last_provider_request_at: float
    provider_request_state: str
    subprocess_active: int
    session_state: str
    worker_process_alive: int
    explicit_markers_json: str
    raw_evidence_json: str
    classification: str
    classification_confidence: float
    id: int = 0

class WatchdogStore:
    """SQLite-backed persistence for Hermes Watchdog."""
    
    def __init__(self, db_path: str):
        self.db_path = Path(db_path).expanduser()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()
    
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        # Apply WAL mode and pragmas
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn
    
    def _init_db(self):
        with self._lock, self._connect() as conn:
            conn.executescript(SCHEMA_SQL)
            # Ensure schema version
            conn.execute(
                "INSERT OR IGNORE INTO configuration_version (version, config_hash, applied_at) VALUES (?, ?, ?)",
                (SCHEMA_VERSION, "initial", time.time())
            )
            conn.commit()
        logger.info(f"Watchdog store initialized at {self.db_path}")
    
    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            with conn:
                yield conn
        finally:
            conn.close()
    
    # Task operations
    def upsert_task(self, task: TaskRecord) -> None:
        with self._lock, self._transaction() as conn:
            conn.execute("""
                INSERT INTO tasks (task_id, session_id, session_key, source, platform, cwd, git_repo_root,
                                  first_seen_at, last_seen_at, last_activity_at, last_activity_description,
                                  structured_state, is_active, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    session_id=excluded.session_id,
                    session_key=excluded.session_key,
                    source=excluded.source,
                    platform=excluded.platform,
                    cwd=excluded.cwd,
                    git_repo_root=excluded.git_repo_root,
                    last_seen_at=excluded.last_seen_at,
                    last_activity_at=excluded.last_activity_at,
                    last_activity_description=excluded.last_activity_description,
                    structured_state=excluded.structured_state,
                    is_active=excluded.is_active,
                    metadata_json=excluded.metadata_json
            """, (
                task.task_id, task.session_id, task.session_key, task.source, task.platform,
                task.cwd, task.git_repo_root, task.first_seen_at, task.last_seen_at,
                task.last_activity_at, task.last_activity_description, task.structured_state,
                task.is_active, task.metadata_json
            ))
    
    def get_active_tasks(self) -> List[TaskRecord]:
        with self._lock, self._transaction() as conn:
            rows = conn.execute("SELECT * FROM tasks WHERE is_active=1").fetchall()
            return [TaskRecord(**dict(row)) for row in rows]
    
    def get_task(self, task_id: str) -> Optional[TaskRecord]:
        with self._lock, self._transaction() as conn:
            row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            return TaskRecord(**dict(row)) if row else None
    
    def mark_task_inactive(self, task_id: str) -> None:
        with self._lock, self._transaction() as conn:
            conn.execute("UPDATE tasks SET is_active=0 WHERE task_id=?", (task_id,))
    
    # Observation operations
    def record_observation(self, obs: ObservationRecord) -> int:
        with self._lock, self._transaction() as conn:
            cursor = conn.execute("""
                INSERT INTO observations (task_id, scan_cycle, observed_at, structured_state,
                                        last_agent_event_at, last_tool_start_at, last_tool_end_at,
                                        last_provider_request_at, provider_request_state,
                                        subprocess_active, session_state, worker_process_alive,
                                        explicit_markers_json, raw_evidence_json,
                                        classification, classification_confidence)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                obs.task_id, obs.scan_cycle, obs.observed_at, obs.structured_state,
                obs.last_agent_event_at, obs.last_tool_start_at, obs.last_tool_end_at,
                obs.last_provider_request_at, obs.provider_request_state,
                obs.subprocess_active, obs.session_state, obs.worker_process_alive,
                obs.explicit_markers_json, obs.raw_evidence_json,
                obs.classification, obs.classification_confidence
            ))
            return cursor.lastrowid
    
    def get_latest_observation(self, task_id: str) -> Optional[ObservationRecord]:
        with self._lock, self._transaction() as conn:
            row = conn.execute("""
                SELECT * FROM observations WHERE task_id=? ORDER BY scan_cycle DESC LIMIT 1
            """, (task_id,)).fetchone()
            return ObservationRecord(**dict(row)) if row else None
    
    def get_observations_since(self, task_id: str, scan_cycle: int) -> List[ObservationRecord]:
        with self._lock, self._transaction() as conn:
            rows = conn.execute("""
                SELECT * FROM observations WHERE task_id=? AND scan_cycle > ? ORDER BY scan_cycle
            """, (task_id, scan_cycle)).fetchall()
            return [ObservationRecord(**dict(row)) for row in rows]
    
    # Error fingerprint operations
    def record_error_fingerprint(self, task_id: str, fingerprint_hash: str, error_class: str,
                                  error_message: str, structured_error_json: str) -> None:
        with self._lock, self._transaction() as conn:
            now = time.time()
            conn.execute("""
                INSERT INTO error_fingerprints (task_id, fingerprint_hash, error_class, error_message,
                                               structured_error_json, first_seen_at, last_seen_at, occurrence_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                ON CONFLICT(task_id, fingerprint_hash) DO UPDATE SET
                    error_class=excluded.error_class,
                    error_message=excluded.error_message,
                    structured_error_json=excluded.structured_error_json,
                    last_seen_at=excluded.last_seen_at,
                    occurrence_count=occurrence_count + 1
            """, (task_id, fingerprint_hash, error_class, error_message, structured_error_json, now, now))
    
    def get_error_fingerprints(self, task_id: str) -> List[Dict]:
        with self._lock, self._transaction() as conn:
            rows = conn.execute("""
                SELECT * FROM error_fingerprints WHERE task_id=? ORDER BY last_seen_at DESC
            """, (task_id,)).fetchall()
            return [dict(row) for row in rows]
    
    # Recovery attempt operations
    def record_recovery_attempt(self, task_id: str, action_id: str, action_type: str,
                                 error_fingerprint_hash: Optional[str], lease_id: Optional[str]) -> None:
        with self._lock, self._transaction() as conn:
            now = time.time()
            conn.execute("""
                INSERT INTO recovery_attempts (task_id, action_id, action_type, error_fingerprint_hash,
                                             planned_at, lease_id, status)
                VALUES (?, ?, ?, ?, ?, ?, 'planned')
                ON CONFLICT(action_id) DO UPDATE SET
                    status='planned',
                    lease_id=excluded.lease_id
            """, (task_id, action_id, action_type, error_fingerprint_hash, now, lease_id))
    
    def update_recovery_attempt(self, action_id: str, status: str, result_json: str = "", evidence_json: str = "") -> None:
        with self._lock, self._transaction() as conn:
            now = time.time()
            conn.execute("""
                UPDATE recovery_attempts SET status=?, executed_at=?, result_json=?, evidence_json=?
                WHERE action_id=?
            """, (status, now, result_json, evidence_json, action_id))
    
    def get_recovery_attempt(self, action_id: str) -> Optional[Dict]:
        with self._lock, self._transaction() as conn:
            row = conn.execute("SELECT * FROM recovery_attempts WHERE action_id=?", (action_id,)).fetchone()
            return dict(row) if row else None
    
    def get_pending_recovery_attempts(self, task_id: str) -> List[Dict]:
        with self._lock, self._transaction() as conn:
            rows = conn.execute("""
                SELECT * FROM recovery_attempts WHERE task_id=? AND status IN ('planned', 'executing')
                ORDER BY planned_at
            """, (task_id,)).fetchall()
            return [dict(row) for row in rows]
    
    # Recovery action idempotency
    def record_recovery_action(self, action_id: str, task_id: str, action_type: str,
                                idempotency_key: str, result_summary: str) -> bool:
        """Record executed action. Returns False if idempotency key already exists."""
        with self._lock, self._transaction() as conn:
            try:
                conn.execute("""
                    INSERT INTO recovery_actions (action_id, task_id, action_type, executed_at, idempotency_key, result_summary)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (action_id, task_id, action_type, time.time(), idempotency_key, result_summary))
                return True
            except sqlite3.IntegrityError:
                return False
    
    def has_action_idempotency_key(self, idempotency_key: str) -> bool:
        with self._lock, self._transaction() as conn:
            row = conn.execute("SELECT 1 FROM recovery_actions WHERE idempotency_key=?", (idempotency_key,)).fetchone()
            return row is not None
    
    # Recovery lease operations
    def acquire_lease(self, task_id: str, owner: str, ttl_seconds: int, generation: int, action_id: str) -> bool:
        """Try to acquire exclusive recovery lease. Returns True if acquired."""
        with self._lock, self._transaction() as conn:
            now = time.time()
            lease_until = now + ttl_seconds
            cursor = conn.execute("""
                INSERT INTO recovery_leases (task_id, recovery_owner, lease_started_at, lease_until, generation, action_id)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    recovery_owner=excluded.recovery_owner,
                    lease_started_at=excluded.lease_started_at,
                    lease_until=excluded.lease_until,
                    generation=excluded.generation,
                    action_id=excluded.action_id
                WHERE recovery_leases.lease_until < ? OR recovery_leases.recovery_owner = ?
            """, (task_id, owner, now, lease_until, generation, action_id, now, owner))
            return cursor.rowcount > 0
    
    def release_lease(self, task_id: str, owner: str, action_id: str) -> bool:
        with self._lock, self._transaction() as conn:
            cursor = conn.execute("""
                DELETE FROM recovery_leases WHERE task_id=? AND recovery_owner=? AND action_id=?
            """, (task_id, owner, action_id))
            return cursor.rowcount > 0
    
    def get_lease(self, task_id: str) -> Optional[Dict]:
        with self._lock, self._transaction() as conn:
            row = conn.execute("SELECT * FROM recovery_leases WHERE task_id=?", (task_id,)).fetchone()
            return dict(row) if row else None
    
    def is_lease_valid(self, task_id: str, owner: str) -> bool:
        with self._lock, self._transaction() as conn:
            row = conn.execute("""
                SELECT 1 FROM recovery_leases WHERE task_id=? AND recovery_owner=? AND lease_until > ?
            """, (task_id, owner, time.time())).fetchone()
            return row is not None
    
    # Promotion state
    def set_promotion_state(self, key: str, value: str) -> None:
        with self._lock, self._transaction() as conn:
            conn.execute("""
                INSERT INTO promotion_state (key, value, updated_at) VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """, (key, value, time.time()))
    
    def get_promotion_state(self, key: str, default: str = "") -> str:
        with self._lock, self._transaction() as conn:
            row = conn.execute("SELECT value FROM promotion_state WHERE key=?", (key,)).fetchone()
            return row["value"] if row else default
    
    # Watchdog runs
    def record_run_start(self, cycle_number: int, mode: str) -> int:
        with self._lock, self._transaction() as conn:
            cursor = conn.execute("""
                INSERT INTO watchdog_runs (cycle_number, started_at, mode) VALUES (?, ?, ?)
                ON CONFLICT(cycle_number) DO UPDATE SET started_at=excluded.started_at, mode=excluded.mode
            """, (cycle_number, time.time(), mode))
            return cursor.lastrowid
    
    def record_run_complete(self, cycle_number: int, tasks_discovered: int, tasks_classified: int,
                             recoveries_planned: int, recoveries_executed: int, error: str = "", metadata_json: str = "{}") -> None:
        with self._lock, self._transaction() as conn:
            conn.execute("""
                UPDATE watchdog_runs SET completed_at=?, tasks_discovered=?, tasks_classified=?,
                recoveries_planned=?, recoveries_executed=?, error=?, metadata_json=?
                WHERE cycle_number=?
            """, (time.time(), tasks_discovered, tasks_classified, recoveries_planned, recoveries_executed, error, metadata_json, cycle_number))
    
    def get_recent_runs(self, limit: int = 20) -> List[Dict]:
        with self._lock, self._transaction() as conn:
            rows = conn.execute("""
                SELECT * FROM watchdog_runs ORDER BY cycle_number DESC LIMIT ?
            """, (limit,)).fetchall()
            return [dict(row) for row in rows]
    
    # Config version
    def record_config_version(self, version: int, config_hash: str, metadata_json: str = "{}") -> None:
        with self._lock, self._transaction() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO configuration_version (version, config_hash, applied_at, metadata_json)
                VALUES (?, ?, ?, ?)
            """, (version, config_hash, time.time(), metadata_json))
    
    def get_config_version(self) -> Optional[Dict]:
        with self._lock, self._transaction() as conn:
            row = conn.execute("SELECT * FROM configuration_version ORDER BY version DESC LIMIT 1").fetchone()
            return dict(row) if row else None
    
    # Cleanup
    def prune_old_data(self, retention_days: int = 30) -> None:
        cutoff = time.time() - (retention_days * 86400)
        with self._lock, self._transaction() as conn:
            conn.execute("DELETE FROM observations WHERE observed_at < ?", (cutoff,))
            conn.execute("DELETE FROM error_fingerprints WHERE last_seen_at < ?", (cutoff,))
            conn.execute("DELETE FROM recovery_attempts WHERE planned_at < ?", (cutoff,))
            conn.execute("DELETE FROM recovery_actions WHERE executed_at < ?", (cutoff,))
            conn.execute("DELETE FROM watchdog_runs WHERE started_at < ?", (cutoff,))
            # Don't prune tasks - they're reference data
            conn.execute("DELETE FROM promotion_state WHERE updated_at < ?", (cutoff,))
            logger.info(f"Pruned watchdog data older than {retention_days} days")

    # V3: Task State Machine operations
    def upsert_task_state_machine(self, state: Dict) -> None:
        """Upsert the canonical task execution state machine."""
        with self._lock, self._transaction() as conn:
            conn.execute("""
                INSERT INTO task_state_machine (
                    task_id, program_id, generation, goal, capability,
                    first_unproven_boundary, accepted_baseline, completed_boundaries_json,
                    active_writer_identity, active_transaction_id,
                    pending_action, last_completed_action, side_effect_state,
                    state_version, checkpoint_hash, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    program_id=excluded.program_id,
                    generation=excluded.generation,
                    goal=excluded.goal,
                    capability=excluded.capability,
                    first_unproven_boundary=excluded.first_unproven_boundary,
                    accepted_baseline=excluded.accepted_baseline,
                    completed_boundaries_json=excluded.completed_boundaries_json,
                    active_writer_identity=excluded.active_writer_identity,
                    active_transaction_id=excluded.active_transaction_id,
                    pending_action=excluded.pending_action,
                    last_completed_action=excluded.last_completed_action,
                    side_effect_state=excluded.side_effect_state,
                    state_version=state_version + 1,
                    checkpoint_hash=excluded.checkpoint_hash,
                    updated_at=excluded.updated_at
            """, (
                state.get("task_id"), state.get("program_id"), state.get("generation", 1),
                state.get("goal"), state.get("capability"),
                state.get("first_unproven_boundary"), state.get("accepted_baseline"),
                state.get("completed_boundaries_json"),
                state.get("active_writer_identity"), state.get("active_transaction_id"),
                state.get("pending_action"), state.get("last_completed_action"),
                state.get("side_effect_state", "NONE"),
                state.get("state_version", 1), state.get("checkpoint_hash"),
                state.get("created_at", time.time()), time.time()
            ))

    def get_task_state_machine(self, task_id: str) -> Optional[Dict]:
        """Get the canonical task execution state machine."""
        with self._lock, self._transaction() as conn:
            row = conn.execute(
                "SELECT * FROM task_state_machine WHERE task_id=?", (task_id,)
            ).fetchone()
            return dict(row) if row else None

    def get_task_state_machines_by_program(self, program_id: str) -> List[Dict]:
        """Get all task state machines for a program."""
        with self._lock, self._transaction() as conn:
            rows = conn.execute(
                "SELECT * FROM task_state_machine WHERE program_id=? ORDER BY updated_at DESC", 
                (program_id,)
            ).fetchall()
            return [dict(row) for row in rows]

    def increment_state_version(self, task_id: str, expected_version: int) -> bool:
        """Optimistically increment state version. Returns True if successful."""
        with self._lock, self._transaction() as conn:
            cursor = conn.execute("""
                UPDATE task_state_machine 
                SET state_version = state_version + 1, updated_at = ?
                WHERE task_id = ? AND state_version = ?
            """, (time.time(), task_id, expected_version))
            return cursor.rowcount > 0

    # V3: Event Journal operations
    def record_task_event(self, task_id: str, event_type: str, event_data: Dict, 
                          event_identity: str, source_component: str) -> bool:
        """Record a canonical lifecycle event. Returns False if event_identity already exists."""
        with self._lock, self._transaction() as conn:
            try:
                conn.execute("""
                    INSERT INTO task_execution_events (task_id, event_type, event_data_json, 
                                                      event_identity, occurred_at, source_component)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (task_id, event_type, json.dumps(event_data), event_identity, 
                      time.time(), source_component))
                return True
            except sqlite3.IntegrityError:
                return False

    def get_task_events(self, task_id: str, since: float = 0) -> List[Dict]:
        """Get all lifecycle events for a task since a timestamp."""
        with self._lock, self._transaction() as conn:
            rows = conn.execute("""
                SELECT * FROM task_execution_events 
                WHERE task_id=? AND occurred_at > ?
                ORDER BY occurred_at
            """, (task_id, since)).fetchall()
            return [dict(row) for row in rows]

    def get_recent_events(self, limit: int = 100) -> List[Dict]:
        """Get recent lifecycle events across all tasks."""
        with self._lock, self._transaction() as conn:
            rows = conn.execute("""
                SELECT * FROM task_execution_events 
                ORDER BY occurred_at DESC LIMIT ?
            """, (limit,)).fetchall()
            return [dict(row) for row in rows]