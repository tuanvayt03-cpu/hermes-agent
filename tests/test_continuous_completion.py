from agent.continuous_completion import (
    CompletionStateError,
    Node,
    classify_parity,
    critical_path,
    durable_pass_gate,
    ready_frontier,
)


def test_ready_frontier_respects_dependencies():
    nodes = [Node("A", "DONE"), Node("B", deps=("A",)), Node("C", deps=("A",))]
    assert ready_frontier(nodes) == ["B", "C"]


def test_critical_path_prefers_remaining_weight():
    nodes = [
        Node("A", "DONE"),
        Node("B", deps=("A",), weight=2),
        Node("C", deps=("A",), weight=1),
        Node("D", deps=("B",), weight=3),
    ]
    assert critical_path(nodes) == ["A", "B", "D"]


def test_cycle_rejected():
    try:
        ready_frontier([Node("A", deps=("B",)), Node("B", deps=("A",))])
    except CompletionStateError as exc:
        assert "DEPENDENCY_CYCLE" in str(exc)
    else:
        raise AssertionError("cycle should fail")


def test_parity_requires_clean_and_no_unpushed_completion():
    clean = classify_parity(local_head="x", remote_head="x")
    assert clean["LOCAL_REMOTE_PARITY"] is True
    dirty = classify_parity(local_head="x", remote_head="x", relevant_dirty=True)
    assert dirty["PARITY_CLASS"] == "GITHUB_INCOMPLETE_RELATIVE_TO_LOCAL"
    unpushed = classify_parity(local_head="x", remote_head="x", unpushed_completed=True)
    assert unpushed["PARITY_CLASS"] == "REMOTE_STALE_RELATIVE_TO_LOCAL"


def test_high_risk_gate_requires_independent_validation():
    base = {
        "CAPABILITY_MACHINE_EVIDENCE_PASS": True,
        "CLAIM_RELEASED": True,
        "EFFECTIVE_PRIMARY_WRITER_COUNT_ZERO": True,
        "REMOTE_READBACK_PASS": True,
        "NO_UNPUSHED_COMPLETED_TRANSACTION": True,
    }
    assert durable_pass_gate(base)["DURABLE_PASS"] is True
    result = durable_pass_gate(base, high_risk=True)
    assert result["DURABLE_PASS"] is False
    assert result["MISSING_PREDICATES"] == ["INDEPENDENT_VALIDATION_PASS"]
