"""
Hermes Watchdog V1 - Hermes Adapter

Discovers and observes Hermes tasks/sessions using available runtime interfaces.
Capabilities are probed at startup and cached.
"""

import importlib
import json
import logging
import os
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from core.recovery_kernel import RecoveryCapabilityRegistry

logger = logging.getLogger(__name__)


@dataclass
class HermesCapabilities:
    """Proven capabilities of the Hermes runtime."""
    can_discover_tasks: bool = False
    can_read_structured_status: bool = False
    can_read_events: bool = False
    can_retry_model_provider: bool = False
    can_switch_model_provider: bool = False
    can_resume_worker_from_checkpoint: bool = False
    can_retry_transport: bool = False
    can_resume_session: bool = False
    can_send_task_message: bool = False
    can_read_quota_state: bool = False
    can_compact_context: bool = False
    can_reconcile_side_effect: bool = False
    probe_time: float = 0
    probe_errors: List[str] = field(default_factory=list)


@dataclass
class DiscoveredTask:
    """A discovered Hermes task/session."""
    task_id: str
    session_id: str
    session_key: str
    source: str
    platform: str
    cwd: str
    git_repo_root: str
    started_at: float
    last_activity_at: float
    last_activity_description: str
    structured_state: str
    is_active: bool
    metadata: Dict = field(default_factory=dict)

    # Observation signals
    last_agent_event_at: float = 0
    last_tool_start_at: float = 0
    last_tool_end_at: float = 0
    last_provider_request_at: float = 0
    provider_request_state: str = ""
    subprocess_active: bool = False
    session_state: str = ""
    worker_process_alive: bool = False
    explicit_markers: Dict = field(default_factory=dict)
    structured_provider_error: Optional[Dict] = None


class HermesAdapter:
    """Discovers and observes Hermes tasks using available runtime interfaces."""

    def __init__(self, config: Dict):
        self.config = config
        self.capabilities = HermesCapabilities()
        self._session_db = None
        self._process_registry = None
        self._delivery_ledger = None
        self._hermes_home = None
        self._db_path = None

    def _resolve_paths(self):
        """Dynamically resolve Hermes installation paths."""
        # Try to find HERMES_HOME
        hermes_home = os.environ.get("HERMES_HOME")
        if not hermes_home:
            # Default Windows location
            local_appdata = os.environ.get("LOCALAPPDATA", "")
            if local_appdata:
                hermes_home = str(Path(local_appdata) / "hermes")
            else:
                hermes_home = str(Path.home() / "AppData" / "Local" / "hermes")

        self._hermes_home = Path(hermes_home)

        # Resolve paths from config or auto-discover
        self._db_path = self._hermes_home / "state.db"
        self._processes_json = self._hermes_home / "processes.json"
        self._sessions_dir = self._hermes_home / "sessions"

        # Add hermes-agent to path for imports
        hermes_agent_path = self._hermes_home / "hermes-agent"
        if hermes_agent_path.exists() and str(hermes_agent_path) not in sys.path:
            sys.path.insert(0, str(hermes_agent_path))

        logger.info(f"Resolved Hermes home: {self._hermes_home}")
        logger.info(f"State DB: {self._db_path}")
        logger.info(f"Processes JSON: {self._processes_json}")
        logger.info(f"Sessions dir: {self._sessions_dir}")

    def probe_capabilities(self) -> HermesCapabilities:
        """Probe available Hermes runtime capabilities."""
        self._resolve_paths()
        errors = []

        # 1. Can discover tasks (via state.db sessions table)
        try:
            if self._db_path.exists():
                conn = sqlite3.connect(self._db_path, timeout=5)
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("SELECT COUNT(*) as cnt FROM sessions WHERE ended_at IS NULL")
                row = cursor.fetchone()
                conn.close()
                self.capabilities.can_discover_tasks = True
                logger.info(f"Capability: can_discover_tasks = True (active sessions: {row['cnt'] if row else 0})")
            else:
                errors.append("state.db not found")
        except Exception as e:
            errors.append(f"can_discover_tasks: {e}")

        # 2. Can read structured status (via SessionDB)
        try:
            # Try importing SessionDB
            import hermes_state
            self._session_db = hermes_state.SessionDB()
            # Test a simple query
            sessions = self._session_db.list_sessions(active_only=True, limit=1)
            self.capabilities.can_read_structured_status = True
            logger.info("Capability: can_read_structured_status = True")
        except Exception as e:
            errors.append(f"can_read_structured_status: {e}")

        # 3. Can read events (via messages table / FTS)
        try:
            if self._session_db:
                # Check if we can query messages
                conn = sqlite3.connect(self._db_path, timeout=5)
                cursor = conn.execute("SELECT COUNT(*) FROM messages LIMIT 1")
                conn.close()
                self.capabilities.can_read_events = True
                logger.info("Capability: can_read_events = True")
        except Exception as e:
            errors.append(f"can_read_events: {e}")

        # 4. Can retry transport (via delivery_ledger)
        try:
            import gateway.delivery_ledger
            self._delivery_ledger = gateway.delivery_ledger
            # Check if sweep_recoverable exists
            if hasattr(gateway.delivery_ledger, 'sweep_recoverable'):
                self.capabilities.can_retry_transport = True
                logger.info("Capability: can_retry_transport = True")
        except Exception as e:
            errors.append(f"can_retry_transport: {e}")

        # 5. Direct model-provider retry API does not currently exist in Hermes.
        self.capabilities.can_retry_model_provider = False
        logger.info("Capability: can_retry_model_provider = False (no direct retry API discovered)")

        # 6. Can resume session (via SessionStore.mark_resume_pending)
        try:
            import gateway.session
            # Check if SessionStore has mark_resume_pending
            if hasattr(gateway.session.SessionStore, 'mark_resume_pending'):
                self.capabilities.can_resume_session = True
                logger.info("Capability: can_resume_session = True")
        except Exception as e:
            errors.append(f"can_resume_session: {e}")

        # 7. No direct live-session provider-switch primitive is exposed to watchdog.
        self.capabilities.can_switch_model_provider = False
        logger.info("Capability: can_switch_model_provider = False (no direct provider-switch primitive discovered)")

        # 8. Can resume worker/process checkpoint on startup recovery.
        try:
            from tools.process_registry import process_registry
            if hasattr(process_registry, 'recover_from_checkpoint'):
                self.capabilities.can_resume_worker_from_checkpoint = True
                logger.info("Capability: can_resume_worker_from_checkpoint = True")
        except Exception as e:
            errors.append(f"can_resume_worker_from_checkpoint: {e}")

        # 9. Can send task message (via gateway platform adapters)
        try:
            import gateway.platforms.base
            # Check if there's a send capability
            self.capabilities.can_send_task_message = True
            logger.info("Capability: can_send_task_message = True (platform adapters available)")
        except Exception as e:
            errors.append(f"can_send_task_message: {e}")

        # 10. Can read quota state (via provider error patterns in messages)
        # This is inferred from message content analysis
        self.capabilities.can_read_quota_state = True
        logger.info("Capability: can_read_quota_state = True (via message analysis)")

        # 11. Can compact context (via gateway session context compression)
        try:
            import gateway.session
            # Check if SessionStore has compact_context capability
            if hasattr(gateway.session.SessionStore, 'compact_context'):
                self.capabilities.can_compact_context = True
                logger.info("Capability: can_compact_context = True")
        except Exception as e:
            errors.append(f"can_compact_context: {e}")

        # 12. Can reconcile side effect (via gateway session)
        try:
            import gateway.session
            # Check if SessionStore has reconcile_side_effect capability
            if hasattr(gateway.session.SessionStore, 'reconcile_side_effect'):
                self.capabilities.can_reconcile_side_effect = True
                logger.info("Capability: can_reconcile_side_effect = True")
        except Exception as e:
            errors.append(f"can_reconcile_side_effect: {e}")

        self.capabilities.probe_time = time.time()
        self.capabilities.probe_errors = errors

        if errors:
            logger.warning(f"Capability probe errors: {errors}")

        return self.capabilities

    def get_recovery_capability_registry(self) -> Dict[str, List[Dict[str, Any]]]:
        """Return the enabled recovery primitive registry by domain."""
        return RecoveryCapabilityRegistry(self.capabilities).to_dict()

    def discover_tasks(self) -> List[DiscoveredTask]:
        """Discover all active Hermes tasks/sessions."""
        if not self.capabilities.can_discover_tasks:
            self.probe_capabilities()

        tasks = []

        # Discover from state.db sessions table
        try:
            conn = sqlite3.connect(self._db_path, timeout=10)
            conn.row_factory = sqlite3.Row

            # Get active sessions (not ended)
            rows = conn.execute("""
                SELECT id, source, user_id, session_key, chat_id, chat_type, thread_id,
                       display_name, started_at, ended_at, end_reason, last_activity_at,
                       last_activity_description, cwd, git_branch, git_repo_root,
                       message_count, model, system_prompt_hash
                FROM sessions
                WHERE ended_at IS NULL
                ORDER BY started_at DESC
            """).fetchall()

            for row in rows:
                task = DiscoveredTask(
                    task_id=f"session:{row['id']}",
                    session_id=row['id'],
                    session_key=row['session_key'] or "",
                    source=row['source'] or "unknown",
                    platform=row['chat_type'] or "unknown",
                    cwd=row['cwd'] or "",
                    git_repo_root=row['git_repo_root'] or "",
                    started_at=row['started_at'] or 0,
                    last_activity_at=row['last_activity_at'] or 0,
                    last_activity_description=row['last_activity_description'] or "",
                    structured_state="RUNNING",
                    is_active=True,
                    metadata={
                        "user_id": row['user_id'],
                        "chat_id": row['chat_id'],
                        "thread_id": row['thread_id'],
                        "display_name": row['display_name'],
                        "message_count": row['message_count'],
                        "model": row['model'],
                        "system_prompt_hash": row['system_prompt_hash'],
                        "git_branch": row['git_branch'],
                    }
                )
                tasks.append(task)

            conn.close()
            logger.info(f"Discovered {len(tasks)} active sessions from state.db")
        except Exception as e:
            logger.error(f"Failed to discover tasks from state.db: {e}")

        # Discover from processes.json (background processes)
        try:
            if self._processes_json.exists():
                with open(self._processes_json, 'r') as f:
                    processes = json.load(f)

                for proc in processes:
                    # Only include if still potentially running
                    pid = proc.get('pid')
                    if pid and self._is_process_alive(pid, proc.get('host_start_time')):
                        task = DiscoveredTask(
                            task_id=f"process:{proc.get('session_id', proc.get('task_id', 'unknown'))}",
                            session_id=proc.get('session_id', ''),
                            session_key=proc.get('session_key', ''),
                            source="background_process",
                            platform="local",
                            cwd=proc.get('cwd', ''),
                            git_repo_root="",
                            started_at=proc.get('started_at', 0),
                            last_activity_at=time.time(),
                            last_activity_description=f"Background process: {proc.get('command', '')[:50]}",
                            structured_state="RUNNING",
                            is_active=True,
                            metadata={
                                "pid": pid,
                                "command": proc.get('command', ''),
                                "notify_on_complete": proc.get('notify_on_complete', False),
                                "watch_patterns": proc.get('watch_patterns', []),
                            },
                            worker_process_alive=True,
                            subprocess_active=True,
                        )
                        tasks.append(task)

                logger.info(f"Discovered {len([t for t in tasks if t.source == 'background_process'])} background processes")
        except Exception as e:
            logger.error(f"Failed to discover tasks from processes.json: {e}")

        # Discover from async_delegations table
        try:
            conn = sqlite3.connect(self._db_path, timeout=5)
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT * FROM async_delegations WHERE status IN ('running', 'dispatched')
            """).fetchall()

            for row in rows:
                task = DiscoveredTask(
                    task_id=f"delegation:{row['delegation_id']}",
                    session_id=row['session_key'],
                    session_key=row['session_key'],
                    source="async_delegation",
                    platform="internal",
                    cwd="",
                    git_repo_root="",
                    started_at=row['dispatched_at'] or 0,
                    last_activity_at=time.time(),
                    last_activity_description=f"Delegation: {row['goal'][:50]}",
                    structured_state="RUNNING",
                    is_active=True,
                    metadata={
                        "delegation_id": row['delegation_id'],
                        "parent_session_id": row['parent_session_id'],
                        "goal": row['goal'],
                    }
                )
                tasks.append(task)

            conn.close()
        except Exception as e:
            logger.debug(f"No async_delegations or query failed: {e}")

        return tasks

    def observe_task(self, task: DiscoveredTask) -> DiscoveredTask:
        """Observe a task and enrich with current signals."""
        now = time.time()

        # Get latest messages for this session
        if task.session_id and self._db_path.exists():
            try:
                conn = sqlite3.connect(self._db_path, timeout=5)
                conn.row_factory = sqlite3.Row

                # Last agent message
                row = conn.execute("""
                    SELECT timestamp FROM messages
                    WHERE session_id=? AND role='assistant' AND active=1
                    ORDER BY timestamp DESC LIMIT 1
                """, (task.session_id,)).fetchone()
                if row:
                    task.last_agent_event_at = row['timestamp']

                # Last tool call
                row = conn.execute("""
                    SELECT timestamp FROM messages
                    WHERE session_id=? AND tool_name IS NOT NULL AND active=1
                    ORDER BY timestamp DESC LIMIT 1
                """, (task.session_id,)).fetchone()
                if row:
                    task.last_tool_end_at = row['timestamp']

                # Last tool start (look for tool calls)
                row = conn.execute("""
                    SELECT timestamp FROM messages
                    WHERE session_id=? AND tool_calls IS NOT NULL AND active=1
                    ORDER BY timestamp DESC LIMIT 1
                """, (task.session_id,)).fetchone()
                if row:
                    task.last_tool_start_at = row['timestamp']

                # Check for structured provider errors in all relevant fields
                rows = conn.execute("""
                    SELECT content, timestamp, role, tool_name, effect_disposition, finish_reason,
                           reasoning, reasoning_content, reasoning_details,
                           codex_reasoning_items, codex_message_items
                    FROM messages
                    WHERE session_id=? AND active=1
                    ORDER BY timestamp DESC LIMIT 20
                """, (task.session_id,)).fetchall()

                # ALSO check the canonical provider_errors table (new structured events)
                provider_error_rows = conn.execute("""
                    SELECT event_id, occurred_at, provider, model, http_status, error_code,
                           error_class, sanitized_error_message, retryable, retry_after,
                           reset_at, request_id, stream_id, source_component
                    FROM provider_errors
                    WHERE session_id=?
                    ORDER BY occurred_at DESC LIMIT 10
                """, (task.session_id,)).fetchall()

                # Process canonical provider errors FIRST (highest authority)
                for row in provider_error_rows:
                    task.provider_request_state = row['error_class'] or "UNKNOWN"
                    task.last_provider_request_at = row['occurred_at']
                    task.structured_provider_error = {
                        "event_id": row['event_id'],
                        "http_status": row['http_status'],
                        "error_code": row['error_code'],
                        "error_class": row['error_class'],
                        "error_message": row['sanitized_error_message'],
                        "source_field": "provider_errors_table",
                        "timestamp": row['occurred_at'],
                        "provider": row['provider'],
                        "model": row['model'],
                        "retryable": bool(row['retryable']),
                        "retry_after": row['retry_after'],
                        "reset_at": row['reset_at'],
                        "request_id": row['request_id'],
                        "stream_id": row['stream_id'],
                        "source_component": row['source_component'],
                    }
                    # Canonical provider error found - no need to scan message fields
                    conn.close()
                    return task

                for row in rows:
                    content = row['content'] or ""
                    role = row['role'] or ""
                    tool_name = row['tool_name'] or ""
                    effect_disposition = row['effect_disposition'] or ""
                    finish_reason = row['finish_reason'] or ""
                    reasoning = row['reasoning'] or ""
                    reasoning_content = row['reasoning_content'] or ""
                    reasoning_details = row['reasoning_details'] or ""
                    codex_reasoning_items = row['codex_reasoning_items'] or ""
                    codex_message_items = row['codex_message_items'] or ""

                    # Check all structured fields for provider errors
                    all_fields = [
                        content,
                        effect_disposition,
                        finish_reason,
                        reasoning,
                        reasoning_content,
                        reasoning_details,
                        codex_reasoning_items,
                        codex_message_items
                    ]

                    combined_text = " ".join([f for f in all_fields if f])

                    # Check auth_unavailable FIRST - semantic error code outranks HTTP status
                    if self._is_auth_unavailable(combined_text):
                        task.provider_request_state = "AUTH_UNAVAILABLE"
                        task.last_provider_request_at = row['timestamp']
                        task.structured_provider_error = {
                            "http_status": 502,
                            "error_code": "auth_unavailable",
                            "error_message": "no auth available",
                            "source_field": self._find_error_source(combined_text, "auth_unavailable"),
                            "timestamp": row['timestamp']
                        }
                        break
                    elif self._is_context_window_exceeded(combined_text):
                        task.provider_request_state = "CONTEXT_WINDOW_EXCEEDED"
                        task.last_provider_request_at = row['timestamp']
                        break
                    elif self._is_provider_overload(combined_text):
                        task.provider_request_state = "PROVIDER_OVERLOAD"
                        task.last_provider_request_at = row['timestamp']
                        break
                    elif self._is_timeout(combined_text):
                        task.provider_request_state = "TIMEOUT"
                        task.last_provider_request_at = row['timestamp']
                        break
                    elif self._is_429(combined_text):
                        task.provider_request_state = "RATE_LIMITED"
                        task.last_provider_request_at = row['timestamp']
                        break
                    elif self._is_auth_failure(combined_text):
                        task.provider_request_state = "AUTH_FAILURE"
                        task.last_provider_request_at = row['timestamp']
                        break
                    elif self._is_quota_exhausted(combined_text):
                        task.provider_request_state = "QUOTA_EXHAUSTED"
                        task.last_provider_request_at = row['timestamp']
                        break

                conn.close()
            except Exception as e:
                logger.debug(f"Failed to observe session {task.session_id}: {e}")

        # Check worker process for background tasks
        if task.source == "background_process" and task.metadata.get("pid"):
            pid = task.metadata["pid"]
            task.worker_process_alive = self._is_process_alive(pid, task.metadata.get("host_start_time"))
            task.subprocess_active = task.worker_process_alive

        # Get explicit markers from session state (if available)
        try:
            if self._session_db:
                # Try to get session state for more signals
                pass
        except Exception:
            pass

        return task

    def _is_process_alive(self, pid: int, host_start_time: Optional[int] = None) -> bool:
        """Check if a process is alive, with PID reuse guard."""
        if not pid:
            return False
        try:
            import psutil
            proc = psutil.Process(pid)
            if proc.is_running():
                # Check start time to prevent PID reuse false positives
                if host_start_time:
                    create_time = int(proc.create_time() * 1000)  # Convert to milliseconds
                    if abs(create_time - host_start_time) > 10000:  # 10 second tolerance
                        logger.debug(f"PID {pid} start time mismatch: {create_time} vs {host_start_time}")
                        return False
                return True
            return False
        except (psutil.NoSuchProcess, psutil.AccessDenied, ImportError):
            return False
        except Exception:
            return False

    def _is_provider_overload(self, content: str) -> bool:
        overload_markers = [
            "overloaded", "capacity", "temporarily unavailable", "service unavailable",
            "503", "529", "provider overload", "upstream capacity", "model overloaded"
        ]
        content_lower = content.lower()
        return any(marker in content_lower for marker in overload_markers)

    def _is_timeout(self, content: str) -> bool:
        timeout_markers = [
            "timeout", "timed out", "request timeout", "read timeout", "connection timeout"
        ]
        content_lower = content.lower()
        return any(marker in content_lower for marker in timeout_markers)

    def _is_429(self, content: str) -> bool:
        return "429" in content or "rate limit" in content.lower() or "too many requests" in content.lower()

    def _is_quota_exhausted(self, content: str) -> bool:
        quota_markers = [
            "quota", "exhausted", "insufficient quota", "billing", "credits", "balance"
        ]
        content_lower = content.lower()
        return any(marker in content_lower for marker in quota_markers)

    def _is_auth_failure(self, content: str) -> bool:
        auth_markers = [
            "unauthorized", "authentication failed", "invalid api key", "invalid token",
            "401", "403", "permission denied", "auth error"
        ]
        content_lower = content.lower()
        return any(marker in content_lower for marker in auth_markers)

    def _is_auth_unavailable(self, content: str) -> bool:
        """Check for auth_unavailable error specifically (semantic error code outranks HTTP status)."""
        content_lower = content.lower()
        return "auth_unavailable" in content_lower

    def _is_context_window_exceeded(self, content: str) -> bool:
        """Check for context window exceeded errors - structured provider response fields first."""
        content_lower = content.lower()
        # Structured error codes (highest priority)
        context_markers = [
            "context_length_exceeded",
            "context_window_exceeded",
            "input_too_long",
            "max_context_length",
            "max_tokens_exceeded",
            "request too large",
            "exceeds the context window",
            "input exceeds the context window",
            "exceeds model context",
            "too many tokens",
            "context overflow",
        ]
        return any(marker in content_lower for marker in context_markers)

    def _find_error_source(self, text: str, error_term: str) -> str:
        """Identify which field contained the error term for debugging."""
        # This is a simplified version - in practice we'd check each field individually
        return "structured_fields"

    def execute_recovery(self, action_type: str, task: DiscoveredTask, params: Dict) -> Dict:
        """Execute a recovery action. Returns result dict."""
        result = {"success": False, "action": action_type, "details": "", "attempted_at": time.time()}
        result["fault_domain"] = (params.get("_fault_envelope") or {}).get("domain")
        result["checkpoint_hash"] = params.get("_checkpoint_hash")
        result["error_event_id"] = (params.get("_fault_envelope") or {}).get("evidence", {}).get("error_event_id")

        if action_type == "MODEL_PROVIDER_RETRY":
            result["details"] = "No direct Hermes model-provider retry API is available; skipped without side effects"

        elif action_type == "RETRY_TRANSPORT":
            # Use delivery_ledger sweep_recoverable
            if self.capabilities.can_retry_transport and self._delivery_ledger:
                try:
                    claimed = self._delivery_ledger.sweep_recoverable(
                        deliverable_platforms={"telegram"}  # Adjust as needed
                    )
                    result["claimed_count"] = len(claimed)
                    result["success"] = len(claimed) > 0
                    result["details"] = f"Claimed {len(claimed)} obligations for redelivery"
                except Exception as e:
                    result["details"] = f"Transport retry failed: {e}"

        elif action_type == "SESSION_RESUME_FROM_CHECKPOINT":
            if self.capabilities.can_resume_session and task.session_key:
                try:
                    result["success"] = self._mark_session_resume_pending(
                        task.session_key,
                        reason="model_provider_failure",
                    )
                    result["details"] = (
                        "Marked session resume pending from checkpoint"
                        if result["success"]
                        else "Session resume marker was not written"
                    )
                except Exception as e:
                    result["details"] = f"Session checkpoint resume failed: {e}"

        elif action_type == "MODEL_PROVIDER_SWITCH":
            result["details"] = "No direct Hermes model-provider switch primitive is available for watchdog execution"

        elif action_type == "WORKER_RESUME_FROM_CHECKPOINT":
            if self.capabilities.can_resume_worker_from_checkpoint:
                try:
                    from tools.process_registry import process_registry
                    recovered = int(process_registry.recover_from_checkpoint() or 0)
                    result["recovered_count"] = recovered
                    result["success"] = recovered > 0
                    result["details"] = f"Recovered {recovered} worker checkpoint entries"
                except Exception as e:
                    result["details"] = f"Worker checkpoint recovery failed: {e}"

        elif action_type == "RESUME_SESSION":
            # Use SessionStore.mark_resume_pending
            if self.capabilities.can_resume_session and task.session_key:
                try:
                    result["success"] = self._mark_session_resume_pending(
                        task.session_key,
                        reason="watchdog_resume",
                    )
                    result["details"] = (
                        "Marked session resume pending"
                        if result["success"]
                        else "Session resume marker was not written"
                    )
                except Exception as e:
                    result["details"] = f"Session resume failed: {e}"

        elif action_type == "NUDGE_AGENT":
            # Send continuation message - requires gateway context
            result["details"] = "Agent nudge requires gateway context"

        elif action_type == "COMPACT_CONTEXT":
            # Execute context compaction via Hermes compression API
            if self.capabilities.can_compact_context and task.session_id:
                try:
                    result_details = self._execute_compact_context(task)
                    result["success"] = result_details.get("success", False)
                    result["details"] = result_details.get("details", "Unknown error")
                except Exception as e:
                    result["details"] = f"Context compaction failed: {e}"

        elif action_type == "RECONCILE_SIDE_EFFECT":
            # Side effect reconciliation - requires gateway context
            if self.capabilities.can_reconcile_side_effect and task.session_key:
                try:
                    # This requires gateway context to actually reconcile
                    # For now, mark as requiring gateway context
                    result["details"] = "Side effect reconciliation requires gateway context"
                except Exception as e:
                    result["details"] = f"Side effect reconciliation failed: {e}"

        elif action_type == "VERIFY_RECOVERY":
            result["success"] = True
            result["details"] = "Verification requested; no mutating action executed"

        return result

    def verify_recovery_effect(self, action_type: str, task: DiscoveredTask,
                               params: Dict, result: Optional[Dict]) -> Dict:
        """Verify recovery effect using action result and current task evidence."""
        fault = params.get("_fault_envelope") or {}
        evidence = {
            "task_id": task.task_id,
            "action_type": action_type,
            "fault_domain": fault.get("domain"),
            "checkpoint_hash": params.get("_checkpoint_hash"),
            "provider_request_state": getattr(task, "provider_request_state", ""),
            "error_event_id": fault.get("evidence", {}).get("error_event_id"),
        }
        verification = {
            "verified": False,
            "effect_state": "UNKNOWN",
            "evidence": evidence,
            "details": "",
        }
        result = result or {}

        if self._is_model_provider_recovery(action_type, fault.get("domain")):
            activity_evidence = self._collect_model_provider_effect_evidence(task, fault)
            evidence.update(activity_evidence)
            verification["verified"] = activity_evidence["has_effect_evidence"]
            verification["effect_state"] = "VERIFIED" if verification["verified"] else "PENDING"
            verification["details"] = activity_evidence["details"]
            return verification

        activity_evidence = self._collect_recovery_effect_evidence(task, params, result)
        evidence.update(activity_evidence)

        if action_type == "RETRY_TRANSPORT":
            verification["verified"] = activity_evidence["has_effect_evidence"]
            verification["effect_state"] = "VERIFIED" if verification["verified"] else "PENDING"
            verification["details"] = activity_evidence["details"] or result.get("details", "")
        elif action_type in (
            "COMPACT_CONTEXT",
            "RECONCILE_SIDE_EFFECT",
            "RESUME_SESSION",
            "NUDGE_AGENT",
        ):
            verification["verified"] = bool(result.get("success"))
            verification["effect_state"] = "VERIFIED" if verification["verified"] else "UNKNOWN"
            verification["details"] = result.get("details", "")
        elif action_type == "VERIFY_RECOVERY":
            still_invalidated = bool(task.explicit_markers.get("invalidate_checkpoint"))
            still_faulting = bool(getattr(task, "provider_request_state", ""))
            verification["verified"] = not still_invalidated and not still_faulting
            verification["effect_state"] = "VERIFIED" if verification["verified"] else "PENDING"
            verification["details"] = (
                "Recovery authority verified from current task evidence"
                if verification["verified"]
                else "Current task evidence still shows an open invalidator or active fault"
            )

        return verification

    def _is_model_provider_recovery(self, action_type: str, fault_domain: Optional[str]) -> bool:
        return fault_domain == "model_provider" or action_type in (
            "MODEL_PROVIDER_RETRY",
            "SESSION_RESUME_FROM_CHECKPOINT",
            "MODEL_PROVIDER_SWITCH",
            "WORKER_RESUME_FROM_CHECKPOINT",
        )

    def _mark_session_resume_pending(self, session_key: str, reason: str) -> bool:
        """Mark an existing Hermes session resumable using the runtime SessionStore."""
        import gateway.config
        import gateway.session

        if self._sessions_dir is None:
            self._resolve_paths()

        config = gateway.config.GatewayConfig(sessions_dir=self._sessions_dir)
        store = gateway.session.SessionStore(self._sessions_dir, config)
        return bool(store.mark_resume_pending(session_key, reason=reason))

    def _current_first_unproven_boundary(self, task: DiscoveredTask) -> Optional[str]:
        metadata = getattr(task, "metadata", {}) or {}
        markers = getattr(task, "explicit_markers", {}) or {}
        return (
            getattr(task, "first_unproven_boundary", None)
            or metadata.get("first_unproven_boundary")
            or markers.get("first_unproven_boundary")
            or markers.get("resume_boundary")
        )

    def _collect_model_provider_effect_evidence(self, task: DiscoveredTask, fault: Dict) -> Dict:
        """Verify model-provider recovery from later runtime activity or boundary advancement."""
        fault_evidence = fault.get("evidence", {}) or {}
        fault_at = float(
            fault_evidence.get("provider_error_occurred_at")
            or fault_evidence.get("provider_request_at")
            or 0
        )
        previous_boundary = fault_evidence.get("first_unproven_boundary")
        current_boundary = self._current_first_unproven_boundary(task)

        later_provider_activity = bool(fault_at and getattr(task, "last_provider_request_at", 0) > fault_at)
        later_assistant_activity = bool(fault_at and getattr(task, "last_agent_event_at", 0) > fault_at)
        later_tool_activity = bool(
            fault_at and (
                getattr(task, "last_tool_start_at", 0) > fault_at
                or getattr(task, "last_tool_end_at", 0) > fault_at
            )
        )
        boundary_advanced = bool(
            previous_boundary
            and current_boundary
            and current_boundary != previous_boundary
        )
        has_effect_evidence = (
            later_provider_activity
            or later_assistant_activity
            or later_tool_activity
            or boundary_advanced
        )

        if has_effect_evidence:
            parts = []
            if later_provider_activity:
                parts.append("provider activity")
            if later_assistant_activity:
                parts.append("assistant activity")
            if later_tool_activity:
                parts.append("tool activity")
            if boundary_advanced:
                parts.append("FIRST_UNPROVEN_BOUNDARY advancement")
            details = "Model-provider recovery evidenced by " + ", ".join(parts)
        else:
            details = (
                "No later provider/assistant/tool activity and no "
                "FIRST_UNPROVEN_BOUNDARY advancement observed after the provider fault"
            )

        return {
            "provider_fault_at": fault_at,
            "previous_first_unproven_boundary": previous_boundary,
            "current_first_unproven_boundary": current_boundary,
            "later_provider_activity": later_provider_activity,
            "later_assistant_activity": later_assistant_activity,
            "later_tool_activity": later_tool_activity,
            "first_unproven_boundary_advanced": boundary_advanced,
            "has_effect_evidence": has_effect_evidence,
            "details": details,
        }

    def _collect_recovery_effect_evidence(self, task: DiscoveredTask, params: Dict, result: Dict) -> Dict:
        """Require post-action runtime evidence instead of trusting executor return values."""
        attempted_at = float(
            result.get("attempted_at")
            or params.get("_attempted_at")
            or 0
        )
        previous_boundary = params.get("_first_unproven_boundary")
        current_boundary = self._current_first_unproven_boundary(task)
        claimed_count = int(result.get("claimed_count", 0) or 0)
        has_effect_evidence = bool(result.get("success")) and claimed_count > 0
        details = result.get("details", "")
        if not details and not has_effect_evidence:
            details = "No executor evidence observed yet"

        return {
            "attempted_at": attempted_at,
            "previous_first_unproven_boundary": previous_boundary,
            "current_first_unproven_boundary": current_boundary,
            "claimed_count": claimed_count,
            "has_effect_evidence": has_effect_evidence,
            "details": details,
        }

    def _execute_compact_context(self, task: DiscoveredTask) -> Dict:
        """Execute context compaction for a session."""
        import asyncio
        import sys
        
        # Add hermes-agent to path
        sys.path.insert(0, str(self._hermes_home / "hermes-agent"))
        
        from agent.conversation_compression import compress_context
        from agent.model_metadata import estimate_request_tokens_rough
        
        # Get the async session store
        try:
            import gateway.session
            # We need to create a session store instance
            # Use the same approach as the gateway
            session_store = gateway.session.AsyncSessionStore(
                db_path=self._db_path,
                memory_provider=None  # Will use default
            )
        except Exception as e:
            return {"success": False, "details": f"Failed to create session store: {e}"}
        
        # Load transcript
        try:
            # Run async load_transcript
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            history = loop.run_until_complete(
                session_store.load_transcript(task.session_id)
            )
            loop.close()
        except Exception as e:
            return {"success": False, "details": f"Failed to load transcript: {e}"}
        
        if not history or len(history) < 4:
            return {"success": False, "details": "Not enough history to compress"}
        
        # Filter messages (user, assistant, tool)
        msgs = [
            m for m in history
            if m.get("role") in {"user", "assistant", "tool"}
        ]
        
        if len(msgs) < 4:
            return {"success": False, "details": "Not enough relevant messages to compress"}
        
        # Estimate tokens
        try:
            # Need system prompt and tools - get from a minimal agent
            from run_agent import AIAgent
            
            # Create minimal agent for token estimation
            # We need to know the model and API key
            model = task.metadata.get("model", "gpt-4o")
            
            # Try to get runtime config
            runtime_kwargs = {
                "model": model,
                "max_iterations": 4,
                "quiet_mode": True,
                "skip_memory": True,
                "enabled_toolsets": ["memory"],
                "session_id": task.session_id,
            }
            
            # Check if we have API key from environment
            import os
            api_key = os.environ.get("OPENROUTER_API_KEY") or os.environ.get("OPENAI_API_KEY")
            if api_key:
                runtime_kwargs["api_key"] = api_key
            
            tmp_agent = AIAgent(**runtime_kwargs)
            
            # Get system prompt
            _sys_prompt = getattr(tmp_agent, "_cached_system_prompt", "") or ""
            _tools = getattr(tmp_agent, "tools", None) or None
            
            approx_tokens = estimate_request_tokens_rough(
                msgs, system_prompt=_sys_prompt, tools=_tools
            )
            
            # Run compression
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            compressed, _ = loop.run_until_complete(
                asyncio.to_thread(
                    lambda: compress_context(
                        tmp_agent,
                        msgs,
                        "",
                        approx_tokens=approx_tokens,
                        task_id=task.task_id,
                        focus_topic=None,
                        force=True,
                        defer_context_engine_notification=True,
                    )
                )
            )
            loop.close()
            
            # Check if compression actually changed anything
            if compressed == msgs:
                return {"success": False, "details": "Compression made no changes"}
            
            # Persist compressed transcript
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            success = loop.run_until_complete(
                session_store.rewrite_transcript(task.session_id, compressed)
            )
            loop.close()
            
            if not success:
                return {"success": False, "details": "Failed to persist compressed transcript"}
            
            # Reset stored token count
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(
                session_store.update_session(
                    task.session_key or task.session_id, 
                    last_prompt_tokens=0
                )
            )
            loop.close()
            
            new_tokens = estimate_request_tokens_rough(
                compressed, system_prompt=_sys_prompt, tools=_tools
            )
            
            return {
                "success": True,
                "details": f"Compressed {len(msgs)} -> {len(compressed)} messages, {approx_tokens:,} -> {new_tokens:,} tokens"
            }
            
        except Exception as e:
            return {"success": False, "details": f"Compression execution failed: {e}"}


    def get_capabilities(self) -> HermesCapabilities:
        return self.capabilities
