"""Agent-agnostic continuous project completion primitives.

This module intentionally contains no model/provider/runtime wiring. It is a
small deterministic state layer that can be reused by Hermes, Codex wrappers,
or other agent adapters without creating a second project authority.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

VALID_STATES = {"DONE", "READY", "RUNNING", "BLOCKED", "FUTURE", "INVALIDATED"}


class CompletionStateError(ValueError):
    pass


@dataclass(frozen=True)
class Node:
    id: str
    state: str = "FUTURE"
    deps: tuple[str, ...] = ()
    risk: str = "medium"
    weight: int = 1

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "Node":
        node_id = str(value.get("id") or "").strip()
        if not node_id:
            raise CompletionStateError("NODE_ID_REQUIRED")
        state = str(value.get("state") or "FUTURE").upper()
        if state not in VALID_STATES:
            raise CompletionStateError(f"INVALID_STATE:{state}")
        deps = tuple(str(x) for x in value.get("deps", ()))
        risk = str(value.get("risk") or "medium").lower()
        weight = int(value.get("weight", 1))
        if weight <= 0:
            raise CompletionStateError("POSITIVE_WEIGHT_REQUIRED")
        return cls(node_id, state, deps, risk, weight)


def _node_map(nodes: Iterable[Node]) -> dict[str, Node]:
    result: dict[str, Node] = {}
    for node in nodes:
        if node.id in result:
            raise CompletionStateError(f"DUPLICATE_NODE:{node.id}")
        result[node.id] = node
    for node in result.values():
        missing = [dep for dep in node.deps if dep not in result]
        if missing:
            raise CompletionStateError(f"MISSING_DEP:{node.id}:{','.join(missing)}")
    return result


def validate_dag(nodes: Iterable[Node]) -> dict[str, Node]:
    graph = _node_map(nodes)
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node_id: str) -> None:
        if node_id in visited:
            return
        if node_id in visiting:
            raise CompletionStateError(f"DEPENDENCY_CYCLE:{node_id}")
        visiting.add(node_id)
        for dep in graph[node_id].deps:
            visit(dep)
        visiting.remove(node_id)
        visited.add(node_id)

    for node_id in graph:
        visit(node_id)
    return graph


def ready_frontier(nodes: Iterable[Node]) -> list[str]:
    graph = validate_dag(nodes)
    ready: list[str] = []
    for node in graph.values():
        if node.state in {"DONE", "RUNNING", "BLOCKED", "INVALIDATED"}:
            continue
        if all(graph[dep].state == "DONE" for dep in node.deps):
            ready.append(node.id)
    return sorted(ready)


def critical_path(nodes: Iterable[Node]) -> list[str]:
    """Return a deterministic longest remaining dependency path.

    DONE nodes contribute zero weight but remain in the path only when needed
    as ancestry. This is a planning hint, never an authority decision.
    """
    graph = validate_dag(nodes)
    children: dict[str, list[str]] = {node_id: [] for node_id in graph}
    for node in graph.values():
        for dep in node.deps:
            children[dep].append(node.id)

    memo: dict[str, tuple[int, list[str]]] = {}

    def score(node_id: str) -> tuple[int, list[str]]:
        if node_id in memo:
            return memo[node_id]
        node = graph[node_id]
        own = 0 if node.state == "DONE" else node.weight
        best_score = 0
        best_path: list[str] = []
        for child in sorted(children[node_id]):
            child_score, child_path = score(child)
            if child_score > best_score:
                best_score, best_path = child_score, child_path
        value = (own + best_score, [node_id] + best_path)
        memo[node_id] = value
        return value

    roots = [node.id for node in graph.values() if not node.deps]
    candidates = [score(root) for root in sorted(roots)]
    if not candidates:
        return []
    return max(candidates, key=lambda item: (item[0], tuple(reversed(item[1]))))[1]


def classify_parity(
    *,
    local_head: str | None,
    remote_head: str | None,
    local_ahead: bool = False,
    remote_ahead: bool = False,
    relevant_dirty: bool = False,
    unpushed_completed: bool = False,
) -> dict[str, bool | str]:
    """Classify local/remote project authority without pretending ancestry."""
    if not local_head or not remote_head:
        return {
            "LOCAL_REMOTE_PARITY": False,
            "PARITY_CLASS": "UNPROVEN",
            "REMOTE_CURRENT_DURABLE_TRUTH": False,
        }
    if local_head == remote_head and not relevant_dirty and not unpushed_completed:
        return {
            "LOCAL_REMOTE_PARITY": True,
            "PARITY_CLASS": "PARITY",
            "REMOTE_CURRENT_DURABLE_TRUTH": True,
        }
    if local_ahead or unpushed_completed:
        cls = "REMOTE_STALE_RELATIVE_TO_LOCAL"
    elif remote_ahead:
        cls = "LOCAL_BASELINE_STALE"
    elif local_head == remote_head and relevant_dirty:
        cls = "GITHUB_INCOMPLETE_RELATIVE_TO_LOCAL"
    else:
        cls = "DIVERGED_OR_UNPROVEN"
    return {
        "LOCAL_REMOTE_PARITY": False,
        "PARITY_CLASS": cls,
        "REMOTE_CURRENT_DURABLE_TRUTH": False,
    }


def durable_pass_gate(markers: dict[str, Any], *, high_risk: bool = False) -> dict[str, Any]:
    required = [
        "CAPABILITY_MACHINE_EVIDENCE_PASS",
        "CLAIM_RELEASED",
        "EFFECTIVE_PRIMARY_WRITER_COUNT_ZERO",
        "REMOTE_READBACK_PASS",
        "NO_UNPUSHED_COMPLETED_TRANSACTION",
    ]
    if high_risk:
        required.append("INDEPENDENT_VALIDATION_PASS")
    missing = [name for name in required if markers.get(name) is not True]
    return {
        "DURABLE_PASS": not missing,
        "MISSING_PREDICATES": missing,
    }
