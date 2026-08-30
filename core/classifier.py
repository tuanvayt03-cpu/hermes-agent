"""
Hermes Watchdog V1 - Lifecycle Classifier

Deterministic classification of task lifecycle states based on structured evidence.
No LLM calls - purely rule-based classification.
"""

import hashlib
import json
import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

logger = logging.getLogger(__name__)

# Lifecycle states (from spec)
class LifecycleState:
    HEALTHY = "HEALTHY"
    BUSY = "BUSY"
    SUSPECTED_STALL = "SUSPECTED_STALL"
    TRANSIENT_FAILURE = "TRANSIENT_FAILURE"
    RECOVERY_PENDING = "RECOVERY_PENDING"
    RECOVERING = "RECOVERING"
    WAITING_EXTERNAL = "WAITING_EXTERNAL"
    TERMINAL_COMPLETE = "TERMINAL_COMPLETE"
    TERMINAL_BLOCKED = "TERMINAL_BLOCKED"
    NEEDS_ATTENTION = "NEEDS_ATTENTION"

# Transient failure classes
class FailureClass:
    PROVIDER_OVERLOAD = "PROVIDER_OVERLOAD"
    NETWORK_TRANSIENT = "NETWORK_TRANSIENT"
    HTTP_429_TEMP = "HTTP_429_TEMP"
    QUOTA_EXHAUSTED = "QUOTA_EXHAUSTED"
    AUTH_FAILURE = "AUTH_FAILURE"
    AUTH_UNAVAILABLE = "AUTH_UNAVAILABLE"
    CONTEXT_WINDOW_EXCEEDED = "CONTEXT_WINDOW_EXCEEDED"
    UNKNOWN = "UNKNOWN"

# Recovery action types
class RecoveryAction:
    RETRY_TRANSPORT = "RETRY_TRANSPORT"
    RESUME_SESSION = "RESUME_SESSION"
    NUDGE_AGENT = "NUDGE_AGENT"
    NO_ACTION = "NO_ACTION"
    COMPACT_CONTEXT = "COMPACT_CONTEXT"
    RECONCILE_SIDE_EFFECT = "RECONCILE_SIDE_EFFECT"
    VERIFY_RECOVERY = "VERIFY_RECOVERY"

@dataclass
class ClassificationResult:
    state: str
    confidence: float
    failure_class: Optional[str] = None
    recovery_action: Optional[str] = None
    evidence: Dict = None
    reasoning: str = ""

class LifecycleClassifier:
    """Deterministic lifecycle classifier based on structured evidence."""
    
    def __init__(self, config: Dict):
        self.config = config
        self.classification_config = config.get("classification", {})
        self.suspected_stall_seconds = self.classification_config.get("suspected_stall_seconds", 300)
        self.busy_tool_max_seconds = self.classification_config.get("busy_tool_max_seconds", 1800)
        self.max_task_age_seconds = self.classification_config.get("max_task_age_seconds", 86400)
        self.require_structured_evidence = self.classification_config.get("require_structured_evidence", True)
        
        # Evidence hierarchy weights (higher = more authoritative)
        self.evidence_weights = {
            "structured_task_state": 100,
            "structured_runtime_events": 90,
            "session_process_tool_state": 80,
            "explicit_worker_marker": 70,
            "structured_provider_error": 60,
            "log_inference": 30,
            "nl_heuristic": 10,
        }
    
    def classify(self, task: 'DiscoveredTask', previous_state: Optional[str] = None) -> ClassificationResult:
        """Classify task lifecycle state."""
        now = time.time()
        evidence = {}
        
        # Collect evidence signals
        evidence["task_age"] = now - (task.started_at if task.started_at is not None else now)
        evidence["time_since_activity"] = now - (task.last_activity_at if task.last_activity_at is not None else now)
        evidence["time_since_agent_event"] = now - (task.last_agent_event_at if task.last_agent_event_at is not None else now)
        evidence["time_since_tool_start"] = now - (task.last_tool_start_at if task.last_tool_start_at is not None else now)
        evidence["time_since_tool_end"] = now - (task.last_tool_end_at if task.last_tool_end_at is not None else now)
        evidence["time_since_provider_request"] = now - (task.last_provider_request_at if task.last_provider_request_at is not None else now)
        evidence["provider_request_state"] = task.provider_request_state
        evidence["subprocess_active"] = task.subprocess_active
        evidence["worker_process_alive"] = task.worker_process_alive
        evidence["structured_state"] = task.structured_state
        evidence["session_state"] = task.session_state
        evidence["explicit_markers"] = task.explicit_markers
        
        # 1. Check for explicit terminal states (highest priority)
        terminal_result = self._check_terminal_states(task, evidence)
        if terminal_result:
            return terminal_result
        
        # 2. Check for waiting states
        waiting_result = self._check_waiting_states(task, evidence)
        if waiting_result:
            return waiting_result
        
        # 3. Check for transient failures
        transient_result = self._check_transient_failures(task, evidence)
        if transient_result:
            return transient_result
        
        # 4. Check for stall/suspected stall
        stall_result = self._check_stall_states(task, evidence)
        if stall_result:
            return stall_result
        
        # 5. Check for busy state
        busy_result = self._check_busy_state(task, evidence)
        if busy_result:
            return busy_result
        
        # 6. Default to healthy
        return ClassificationResult(
            state=LifecycleState.HEALTHY,
            confidence=0.8,
            evidence=evidence,
            reasoning="No adverse signals detected; task appears healthy"
        )
    
    def _check_terminal_states(self, task, evidence) -> Optional[ClassificationResult]:
        """Check for TERMINAL_COMPLETE or TERMINAL_BLOCKED."""
        # Explicit completion markers
        markers = task.explicit_markers or {}
        if markers.get("completion_marker") or markers.get("status") == "COMPLETE":
            return ClassificationResult(
                state=LifecycleState.TERMINAL_COMPLETE,
                confidence=0.95,
                evidence=evidence,
                reasoning="Explicit completion marker detected"
            )
        
        # Structured state says complete
        if task.structured_state in ("COMPLETE", "FINISHED", "SUCCESS"):
            return ClassificationResult(
                state=LifecycleState.TERMINAL_COMPLETE,
                confidence=0.9,
                evidence=evidence,
                reasoning=f"Structured state: {task.structured_state}"
            )
        
        # Explicit blocked markers
        if markers.get("blocked") or markers.get("status") == "BLOCKED":
            return ClassificationResult(
                state=LifecycleState.TERMINAL_BLOCKED,
                confidence=0.9,
                evidence=evidence,
                reasoning="Explicit blocked marker detected"
            )
        
        # Structured state says blocked/failed terminally
        if task.structured_state in ("BLOCKED", "FAILED", "TERMINAL", "ERROR"):
            # But check if it's a recoverable error first
            if not self._is_recoverable_error(task):
                return ClassificationResult(
                    state=LifecycleState.TERMINAL_BLOCKED,
                    confidence=0.85,
                    evidence=evidence,
                    reasoning=f"Structured state: {task.structured_state} (non-recoverable)"
                )
        
        return None
    
    def _check_waiting_states(self, task, evidence) -> Optional[ClassificationResult]:
        """Check for WAITING_EXTERNAL states."""
        # Quota exhausted - must wait for reset
        if task.provider_request_state == "QUOTA_EXHAUSTED":
            return ClassificationResult(
                state=LifecycleState.WAITING_EXTERNAL,
                confidence=0.9,
                failure_class=FailureClass.QUOTA_EXHAUSTED,
                recovery_action=RecoveryAction.NO_ACTION,
                evidence=evidence,
                reasoning="Quota exhausted - waiting for reset"
            )
        
        # Auth failure - needs human intervention
        if task.provider_request_state == "AUTH_FAILURE":
            return ClassificationResult(
                state=LifecycleState.WAITING_EXTERNAL,
                confidence=0.9,
                failure_class=FailureClass.AUTH_FAILURE,
                recovery_action=RecoveryAction.NO_ACTION,
                evidence=evidence,
                reasoning="Authentication failure - needs credential update"
            )
        
        # Auth unavailable - semantic error code outranks HTTP status
        if task.provider_request_state == "AUTH_UNAVAILABLE":
            return ClassificationResult(
                state=LifecycleState.WAITING_EXTERNAL,
                confidence=0.95,
                failure_class=FailureClass.AUTH_UNAVAILABLE,
                recovery_action=RecoveryAction.NO_ACTION,
                evidence=evidence,
                reasoning="Auth unavailable - semantic error code outranks HTTP 502, no blind retry"
            )
        
        # Explicit waiting markers
        markers = task.explicit_markers or {}
        if markers.get("waiting_for") in ("user_input", "approval", "quota_reset", "external_dependency"):
            return ClassificationResult(
                state=LifecycleState.WAITING_EXTERNAL,
                confidence=0.85,
                evidence=evidence,
                reasoning=f"Explicit wait marker: {markers.get('waiting_for')}"
            )
        
        # Session state indicates waiting
        if task.session_state in ("WAITING_USER", "WAITING_APPROVAL", "WAITING_QUOTA"):
            return ClassificationResult(
                state=LifecycleState.WAITING_EXTERNAL,
                confidence=0.8,
                evidence=evidence,
                reasoning=f"Session state: {task.session_state}"
            )
        
        return None
    
    def _check_transient_failures(self, task, evidence) -> Optional[ClassificationResult]:
        """Check for known transient failure classes."""
        provider_state = task.provider_request_state

        if provider_state == "PROVIDER_OVERLOAD":
            return ClassificationResult(
                state=LifecycleState.TRANSIENT_FAILURE,
                confidence=0.85,
                failure_class=FailureClass.PROVIDER_OVERLOAD,
                recovery_action=RecoveryAction.RETRY_TRANSPORT,
                evidence=evidence,
                reasoning="Provider overload detected"
            )

        if provider_state == "TIMEOUT":
            return ClassificationResult(
                state=LifecycleState.TRANSIENT_FAILURE,
                confidence=0.8,
                failure_class=FailureClass.NETWORK_TRANSIENT,
                recovery_action=RecoveryAction.RETRY_TRANSPORT,
                evidence=evidence,
                reasoning="Request timeout detected"
            )

        if provider_state == "RATE_LIMITED":
            return ClassificationResult(
                state=LifecycleState.TRANSIENT_FAILURE,
                confidence=0.85,
                failure_class=FailureClass.HTTP_429_TEMP,
                recovery_action=RecoveryAction.RETRY_TRANSPORT,
                evidence=evidence,
                reasoning="Rate limit (429) detected"
            )

        # Network transient (DNS, connection errors) - retryable
        if provider_state == "NETWORK_TRANSIENT":
            return ClassificationResult(
                state=LifecycleState.TRANSIENT_FAILURE,
                confidence=0.8,
                failure_class=FailureClass.NETWORK_TRANSIENT,
                recovery_action=RecoveryAction.RETRY_TRANSPORT,
                evidence=evidence,
                reasoning="Network/DNS transient error detected"
            )

        # Context window exceeded - NOT a transient failure, requires compaction
        if provider_state == "CONTEXT_WINDOW_EXCEEDED":
            return ClassificationResult(
                state=LifecycleState.RECOVERY_PENDING,
                confidence=0.95,
                failure_class=FailureClass.CONTEXT_WINDOW_EXCEEDED,
                recovery_action=RecoveryAction.COMPACT_CONTEXT,
                evidence=evidence,
                reasoning="Context window exceeded - requires deterministic compaction, NOT blind retry"
            )

        # Check error fingerprints from recent messages
        if hasattr(task, 'recent_errors'):
            for error in task.recent_errors:
                error_class = self._classify_error(error)
                if error_class in (FailureClass.PROVIDER_OVERLOAD, FailureClass.NETWORK_TRANSIENT, FailureClass.HTTP_429_TEMP):
                    return ClassificationResult(
                        state=LifecycleState.TRANSIENT_FAILURE,
                        confidence=0.75,
                        failure_class=error_class,
                        recovery_action=RecoveryAction.RETRY_TRANSPORT,
                        evidence=evidence,
                        reasoning=f"Error classified as {error_class}"
                    )

        return None
    
    def _check_stall_states(self, task, evidence) -> Optional[ClassificationResult]:
        """Check for SUSPECTED_STALL."""
        time_since_activity = evidence["time_since_activity"]
        time_since_agent_event = evidence["time_since_agent_event"]
        time_since_tool_end = evidence["time_since_tool_end"]
        
        # Only suspect stall if no subprocess is active
        if task.subprocess_active or task.worker_process_alive:
            return None
        
        # No agent event for stall threshold
        if time_since_agent_event > self.suspected_stall_seconds:
            # But check if there was a recent tool that might still be running
            # If last_tool_end_at is 0 (unknown), tool might still be running
            if task.last_tool_end_at == 0:
                return None  # Tool end time unknown, might still be running
            if time_since_tool_end < self.busy_tool_max_seconds:
                return None  # Tool might still be running
            
            return ClassificationResult(
                state=LifecycleState.SUSPECTED_STALL,
                confidence=0.7,
                evidence=evidence,
                reasoning=f"No agent event for {time_since_activity:.0f}s, no active subprocess"
            )
        
        return None
    
    def _check_busy_state(self, task, evidence) -> Optional[ClassificationResult]:
        """Check for BUSY state (long-running tool/operation)."""
        # Active subprocess
        if task.subprocess_active or task.worker_process_alive:
            return ClassificationResult(
                state=LifecycleState.BUSY,
                confidence=0.85,
                evidence=evidence,
                reasoning="Active subprocess/worker process detected"
            )
        
        # Recent tool activity but no agent event yet
        time_since_tool_start = evidence["time_since_tool_start"]
        time_since_tool_end = evidence["time_since_tool_end"]
        
        # Tool started recently (within busy threshold)
        if time_since_tool_start < self.busy_tool_max_seconds:
            # If tool hasn't ended (last_tool_end_at == 0 meaning unknown)
            # Or tool ended very recently (within a short grace period)
            if task.last_tool_end_at == 0 or time_since_tool_end < 60:
                return ClassificationResult(
                    state=LifecycleState.BUSY,
                    confidence=0.8,
                    evidence=evidence,
                    reasoning="Recent tool activity detected"
                )
        
        return None
    
    def _is_recoverable_error(self, task) -> bool:
        """Check if structured error is recoverable."""
        provider_state = task.provider_request_state
        return provider_state in ("PROVIDER_OVERLOAD", "TIMEOUT", "RATE_LIMITED")
    
    def _classify_error(self, error_text: str) -> str:
        """Classify error text into failure class."""
        error_lower = error_text.lower()
        
        if any(m in error_lower for m in ["overloaded", "capacity", "503", "529", "unavailable"]):
            return FailureClass.PROVIDER_OVERLOAD
        if any(m in error_lower for m in ["timeout", "timed out", "connection reset", "network"]):
            return FailureClass.NETWORK_TRANSIENT
        if "429" in error_lower or "rate limit" in error_lower:
            return FailureClass.HTTP_429_TEMP
        if any(m in error_lower for m in ["quota", "exhausted", "billing", "credits"]):
            return FailureClass.QUOTA_EXHAUSTED
        if any(m in error_lower for m in ["unauthorized", "401", "403", "auth", "invalid key"]):
            return FailureClass.AUTH_FAILURE
        return FailureClass.UNKNOWN
    
    def compute_error_fingerprint(self, error_text: str, structured_error: Dict = None) -> str:
        """Compute deterministic fingerprint for an error."""
        content = error_text
        if structured_error:
            content += json.dumps(structured_error, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()[:16]