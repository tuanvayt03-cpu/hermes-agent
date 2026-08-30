"""
Generic recovery kernel primitives for Hermes Watchdog.

Normalizes classified faults into domain-scoped recovery envelopes, exposes a
capability-backed primitive registry, and computes deterministic checkpoints
for checkpoint-first recovery execution.
"""

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

from core.classifier import FailureClass, LifecycleState, RecoveryAction


FAULT_DOMAIN_MODEL_PROVIDER = "model_provider"
FAULT_DOMAIN_TELEGRAM_DELIVERY = "telegram_delivery"
FAULT_DOMAIN_CONTEXT = "context"
FAULT_DOMAIN_SIDE_EFFECT = "side_effect"
FAULT_DOMAIN_SESSION = "session"
FAULT_DOMAIN_TOOL = "tool"
FAULT_DOMAIN_PROCESS = "process"
FAULT_DOMAIN_GATEWAY = "gateway"
FAULT_DOMAIN_GIT = "git"
FAULT_DOMAIN_UNKNOWN = "unknown"
FAULT_DOMAIN_VERIFY = "verify"


CHECKPOINT_OPTIONAL_DOMAINS = {
    FAULT_DOMAIN_MODEL_PROVIDER,
    FAULT_DOMAIN_TELEGRAM_DELIVERY,
}


FAILURE_CLASS_TO_DOMAIN = {
    FailureClass.PROVIDER_OVERLOAD: FAULT_DOMAIN_MODEL_PROVIDER,
    FailureClass.NETWORK_TRANSIENT: FAULT_DOMAIN_MODEL_PROVIDER,
    FailureClass.HTTP_429_TEMP: FAULT_DOMAIN_MODEL_PROVIDER,
    FailureClass.CONTEXT_WINDOW_EXCEEDED: FAULT_DOMAIN_CONTEXT,
    FailureClass.UNKNOWN: FAULT_DOMAIN_UNKNOWN,
}

ACTION_TYPE_TO_DOMAIN = {
    RecoveryAction.MODEL_PROVIDER_RETRY: FAULT_DOMAIN_MODEL_PROVIDER,
    RecoveryAction.MODEL_PROVIDER_SWITCH: FAULT_DOMAIN_MODEL_PROVIDER,
    RecoveryAction.SESSION_RESUME_FROM_CHECKPOINT: FAULT_DOMAIN_SESSION,
    RecoveryAction.WORKER_RESUME_FROM_CHECKPOINT: FAULT_DOMAIN_PROCESS,
    RecoveryAction.RETRY_TRANSPORT: FAULT_DOMAIN_TELEGRAM_DELIVERY,
    RecoveryAction.RESUME_SESSION: FAULT_DOMAIN_SESSION,
    RecoveryAction.NUDGE_AGENT: FAULT_DOMAIN_PROCESS,
    RecoveryAction.COMPACT_CONTEXT: FAULT_DOMAIN_CONTEXT,
    RecoveryAction.RECONCILE_SIDE_EFFECT: FAULT_DOMAIN_SIDE_EFFECT,
    RecoveryAction.VERIFY_RECOVERY: FAULT_DOMAIN_VERIFY,
}


@dataclass(frozen=True)
class FaultEnvelope:
    """Canonical system-level recovery fault description."""

    fault_id: str
    task_id: str
    domain: str
    kind: str
    lifecycle_state: str
    recovery_action: Optional[str]
    requires_checkpoint: bool
    checkpoint_hash: Optional[str] = None
    invalidators: Tuple[str, ...] = ()
    evidence: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "fault_id": self.fault_id,
            "task_id": self.task_id,
            "domain": self.domain,
            "kind": self.kind,
            "lifecycle_state": self.lifecycle_state,
            "recovery_action": self.recovery_action,
            "requires_checkpoint": self.requires_checkpoint,
            "checkpoint_hash": self.checkpoint_hash,
            "invalidators": list(self.invalidators),
            "evidence": self.evidence,
        }


@dataclass(frozen=True)
class RecoveryPrimitive:
    """A recovery primitive gated by proven runtime capability."""

    action_type: str
    domains: Tuple[str, ...]
    capability_flag: Optional[str]
    priority: int
    requires_checkpoint: bool = False
    invasive: bool = False

    def supports(self, domain: str) -> bool:
        return domain in self.domains


def _capability_value(capabilities: Any, name: str) -> bool:
    if isinstance(capabilities, dict):
        return bool(capabilities.get(name))
    return bool(getattr(capabilities, name, False))


class RecoveryCapabilityRegistry:
    """Domain-scoped recovery primitive registry."""

    DEFAULT_PRIMITIVES: Tuple[RecoveryPrimitive, ...] = (
        RecoveryPrimitive(
            action_type=RecoveryAction.MODEL_PROVIDER_RETRY,
            domains=(FAULT_DOMAIN_MODEL_PROVIDER,),
            capability_flag="can_retry_model_provider",
            priority=0,
            invasive=False,
        ),
        RecoveryPrimitive(
            action_type=RecoveryAction.SESSION_RESUME_FROM_CHECKPOINT,
            domains=(FAULT_DOMAIN_MODEL_PROVIDER,),
            capability_flag="can_resume_session",
            priority=1,
            requires_checkpoint=True,
            invasive=True,
        ),
        RecoveryPrimitive(
            action_type=RecoveryAction.MODEL_PROVIDER_SWITCH,
            domains=(FAULT_DOMAIN_MODEL_PROVIDER,),
            capability_flag="can_switch_model_provider",
            priority=2,
            requires_checkpoint=True,
            invasive=True,
        ),
        RecoveryPrimitive(
            action_type=RecoveryAction.WORKER_RESUME_FROM_CHECKPOINT,
            domains=(FAULT_DOMAIN_MODEL_PROVIDER,),
            capability_flag="can_resume_worker_from_checkpoint",
            priority=3,
            requires_checkpoint=True,
            invasive=True,
        ),
        RecoveryPrimitive(
            action_type=RecoveryAction.RETRY_TRANSPORT,
            domains=(FAULT_DOMAIN_TELEGRAM_DELIVERY,),
            capability_flag="can_retry_transport",
            priority=1,
            invasive=False,
        ),
        RecoveryPrimitive(
            action_type=RecoveryAction.RESUME_SESSION,
            domains=(FAULT_DOMAIN_SESSION,),
            capability_flag="can_resume_session",
            priority=2,
            requires_checkpoint=True,
            invasive=True,
        ),
        RecoveryPrimitive(
            action_type=RecoveryAction.NUDGE_AGENT,
            domains=(FAULT_DOMAIN_PROCESS,),
            capability_flag="can_send_task_message",
            priority=3,
            requires_checkpoint=True,
            invasive=True,
        ),
        RecoveryPrimitive(
            action_type=RecoveryAction.COMPACT_CONTEXT,
            domains=(FAULT_DOMAIN_CONTEXT,),
            capability_flag="can_compact_context",
            priority=0,
            requires_checkpoint=True,
            invasive=True,
        ),
        RecoveryPrimitive(
            action_type=RecoveryAction.RECONCILE_SIDE_EFFECT,
            domains=(FAULT_DOMAIN_SIDE_EFFECT,),
            capability_flag="can_reconcile_side_effect",
            priority=0,
            requires_checkpoint=True,
            invasive=False,
        ),
        RecoveryPrimitive(
            action_type=RecoveryAction.VERIFY_RECOVERY,
            domains=(
                FAULT_DOMAIN_MODEL_PROVIDER,
                FAULT_DOMAIN_TELEGRAM_DELIVERY,
                FAULT_DOMAIN_CONTEXT,
                FAULT_DOMAIN_SIDE_EFFECT,
                FAULT_DOMAIN_SESSION,
                FAULT_DOMAIN_TOOL,
                FAULT_DOMAIN_PROCESS,
                FAULT_DOMAIN_GATEWAY,
                FAULT_DOMAIN_GIT,
                FAULT_DOMAIN_UNKNOWN,
                FAULT_DOMAIN_VERIFY,
            ),
            capability_flag=None,
            priority=10,
            invasive=False,
        ),
    )

    def __init__(self, capabilities: Any):
        self.capabilities = capabilities or {}

    def primitives_for_domain(self, domain: str) -> List[RecoveryPrimitive]:
        primitives = [
            primitive
            for primitive in self.DEFAULT_PRIMITIVES
            if primitive.supports(domain) and self._enabled(primitive)
        ]
        return sorted(primitives, key=lambda primitive: primitive.priority)

    def has_non_invasive_primitive(self, domain: str) -> bool:
        return any(not primitive.invasive for primitive in self.primitives_for_domain(domain))

    def _enabled(self, primitive: RecoveryPrimitive) -> bool:
        if primitive.capability_flag is None:
            return True
        return _capability_value(self.capabilities, primitive.capability_flag)

    def to_dict(self) -> Dict[str, List[Dict[str, Any]]]:
        registry: Dict[str, List[Dict[str, Any]]] = {}
        for primitive in self.DEFAULT_PRIMITIVES:
            for domain in primitive.domains:
                if primitive.supports(domain) and self._enabled(primitive):
                    registry.setdefault(domain, []).append(
                        {
                            "action_type": primitive.action_type,
                            "priority": primitive.priority,
                            "requires_checkpoint": primitive.requires_checkpoint,
                            "invasive": primitive.invasive,
                        }
                    )
        for domain in registry:
            registry[domain] = sorted(registry[domain], key=lambda item: item["priority"])
        return registry


def build_fault_envelope(task: Any, classification: Any, durable_state: Optional[Dict] = None,
                         checkpoint_valid: bool = True,
                         invalidators: Optional[Sequence[str]] = None) -> Optional[FaultEnvelope]:
    """Normalize the current task/classification into a generic fault envelope."""
    durable_state = durable_state or {}
    invalidators = tuple(invalidators or ())

    if durable_state.get("side_effect_state") == "UNKNOWN" or getattr(task, "_v3_reconcile_side_effect", False):
        domain = FAULT_DOMAIN_SIDE_EFFECT
        kind = "SIDE_EFFECT_UNKNOWN"
        recovery_action = RecoveryAction.RECONCILE_SIDE_EFFECT
        requires_checkpoint = True
    elif classification.state == LifecycleState.SUSPECTED_STALL:
        domain = FAULT_DOMAIN_PROCESS
        kind = "SUSPECTED_STALL"
        recovery_action = RecoveryAction.NUDGE_AGENT
        requires_checkpoint = True
    elif classification.failure_class:
        domain = FAILURE_CLASS_TO_DOMAIN.get(classification.failure_class)
        if not domain:
            return None
        kind = classification.failure_class
        recovery_action = classification.recovery_action
        requires_checkpoint = domain not in CHECKPOINT_OPTIONAL_DOMAINS
    elif classification.recovery_action:
        domain = ACTION_TYPE_TO_DOMAIN.get(classification.recovery_action)
        if not domain:
            return None
        kind = classification.recovery_action
        recovery_action = classification.recovery_action
        requires_checkpoint = domain not in CHECKPOINT_OPTIONAL_DOMAINS
    else:
        return None

    if not checkpoint_valid:
        invalidators = tuple(list(invalidators or ()) + ["CHECKPOINT_MISMATCH"])

    evidence = {
        "provider_request_state": getattr(task, "provider_request_state", ""),
        "structured_state": getattr(task, "structured_state", ""),
        "session_state": getattr(task, "session_state", ""),
        "reasoning": getattr(classification, "reasoning", ""),
        "error_event_id": ((getattr(task, "structured_provider_error", None) or {}).get("event_id")),
        "provider_http_status": ((getattr(task, "structured_provider_error", None) or {}).get("http_status")),
        "provider_error_occurred_at": (
            (getattr(task, "structured_provider_error", None) or {}).get("timestamp")
            or getattr(task, "last_provider_request_at", 0)
        ),
        "provider_request_at": getattr(task, "last_provider_request_at", 0),
        "pending_action": durable_state.get("pending_action"),
        "last_completed_action": durable_state.get("last_completed_action"),
        "first_unproven_boundary": durable_state.get("first_unproven_boundary"),
        "recovery_generation": durable_state.get("generation", 1),
    }
    fault_key = {
        "task_id": getattr(task, "task_id", ""),
        "domain": domain,
        "kind": kind,
        "state": classification.state,
        "error_event_id": evidence["error_event_id"],
        "invalidators": list(invalidators),
    }
    fault_id = hashlib.sha256(
        json.dumps(fault_key, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:16]

    return FaultEnvelope(
        fault_id=fault_id,
        task_id=getattr(task, "task_id", ""),
        domain=domain,
        kind=kind,
        lifecycle_state=classification.state,
        recovery_action=recovery_action,
        requires_checkpoint=requires_checkpoint,
        checkpoint_hash=durable_state.get("checkpoint_hash"),
        invalidators=invalidators,
        evidence=evidence,
    )


def build_checkpoint_snapshot(task: Any, durable_state: Optional[Dict] = None,
                              fault: Optional[FaultEnvelope] = None) -> Tuple[str, Dict[str, Any]]:
    """Build the deterministic checkpoint snapshot used for recovery gating."""
    durable_state = durable_state or {}
    snapshot = {
        "task_id": getattr(task, "task_id", ""),
        "session_id": getattr(task, "session_id", ""),
        "session_key": getattr(task, "session_key", ""),
        "platform": getattr(task, "platform", ""),
        "cwd": getattr(task, "cwd", ""),
        "structured_state": getattr(task, "structured_state", ""),
        "provider_request_state": getattr(task, "provider_request_state", ""),
        "worker_process_alive": getattr(task, "worker_process_alive", False),
        "explicit_markers": getattr(task, "explicit_markers", {}),
        "program_id": durable_state.get("program_id"),
        "generation": durable_state.get("generation", 1),
        "capability": durable_state.get("capability"),
        "first_unproven_boundary": durable_state.get("first_unproven_boundary"),
        "pending_action": durable_state.get("pending_action"),
        "last_completed_action": durable_state.get("last_completed_action"),
        "side_effect_state": durable_state.get("side_effect_state", "NONE"),
        "active_writer_identity": durable_state.get("active_writer_identity"),
        "active_transaction_id": durable_state.get("active_transaction_id"),
        "fault": fault.to_dict() if fault else None,
    }
    encoded = json.dumps(snapshot, sort_keys=True, default=str, separators=(",", ":"))
    checkpoint_hash = f"sha256:{hashlib.sha256(encoded.encode('utf-8')).hexdigest()}"
    return checkpoint_hash, snapshot
