from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from agent.conversation_loop import _restore_or_build_system_prompt
from agent.system_prompt import build_system_prompt
from hermes_state import SessionDB


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _make_agent(**overrides):
    base = dict(
        load_soul_identity=False,
        skip_context_files=False,
        valid_tool_names=["read_file"],
        _task_completion_guidance=False,
        _parallel_tool_call_guidance=False,
        _tool_use_enforcement=False,
        _environment_probe=False,
        _kanban_worker_guidance="",
        _memory_store=None,
        _memory_manager=None,
        _emit_status=lambda *_args, **_kwargs: None,
        _cached_system_prompt=None,
        _cached_system_prompt_static=None,
        _use_prompt_caching=True,
        _static_rebuild_failed_for=None,
        model="openai/gpt-5.5",
        provider="openrouter",
        platform="cli",
        pass_session_id=True,
        session_id="agent-os-native-session",
        enabled_toolsets=None,
        disabled_toolsets=None,
        tools=[],
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def test_new_session_prompt_exposes_agent_os_semantics(monkeypatch, tmp_path):
    repo_root = _repo_root()
    hermes_home = tmp_path / ".hermes"
    hermes_home.mkdir()

    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    monkeypatch.setenv("TERMINAL_CWD", str(repo_root))

    prompt = build_system_prompt(_make_agent())

    assert "AGENT_OS_RULE_VERSION:" in prompt
    assert "GOAL -> CAPABILITY_ID -> PRIOR_WORK_LOCATE" in prompt
    assert "READY_FRONTIER" in prompt
    assert "CRITICAL_PATH" in prompt
    assert "PARALLEL_SAFE_FRONTIER" in prompt
    assert "BROKER_UNKNOWN" in prompt
    assert "SignalOps" in prompt
    assert "AGENT_OS_MANAGED_BLOCK_BEGIN" not in prompt


def test_resumed_session_reuses_agent_os_prompt_without_duplicate_block(
    monkeypatch, tmp_path
):
    repo_root = _repo_root()
    hermes_home = tmp_path / ".hermes"
    hermes_home.mkdir()
    db_path = tmp_path / "state.db"

    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    monkeypatch.setenv("TERMINAL_CWD", str(repo_root))

    initial_agent = _make_agent(_session_db=None, session_id="resume-agent-os")
    stored_prompt = build_system_prompt(initial_agent)

    db = SessionDB(db_path=db_path)
    try:
        db.create_session(
            "resume-agent-os",
            source="cli",
            system_prompt=stored_prompt,
            model=initial_agent.model,
        )

        resumed_agent = _make_agent(_session_db=db, session_id="resume-agent-os")
        resumed_agent._build_system_prompt = MagicMock(
            side_effect=AssertionError("resume path must reuse stored prompt")
        )

        _restore_or_build_system_prompt(
            resumed_agent,
            None,
            [{"role": "user", "content": "resume"}],
        )

        assert resumed_agent._cached_system_prompt == stored_prompt
        assert resumed_agent._cached_system_prompt.count("AGENT_OS_RULE_VERSION:") == 1
        assert resumed_agent._cached_system_prompt.count("READY_FRONTIER") == 1
        assert resumed_agent._cached_system_prompt.count(
            "Writer rule: Keep one canonical writer."
        ) == 1
    finally:
        db.close()


def test_agent_os_prompt_growth_stays_compact(monkeypatch, tmp_path):
    repo_root = _repo_root()
    hermes_home = tmp_path / ".hermes"
    hermes_home.mkdir()

    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    monkeypatch.setenv("TERMINAL_CWD", str(repo_root))

    integrated_prompt = build_system_prompt(_make_agent())
    from unittest.mock import patch

    with patch("agent.prompt_builder._load_agent_os_core", return_value=""):
        baseline_prompt = build_system_prompt(_make_agent())

    growth_bytes = len(integrated_prompt.encode("utf-8")) - len(
        baseline_prompt.encode("utf-8")
    )

    assert growth_bytes > 0
    assert growth_bytes < 2_000


def test_repeated_build_does_not_duplicate_agent_os_section(monkeypatch, tmp_path):
    repo_root = _repo_root()
    hermes_home = tmp_path / ".hermes"
    hermes_home.mkdir()

    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    monkeypatch.setenv("TERMINAL_CWD", str(repo_root))

    agent = _make_agent()
    first = build_system_prompt(agent)
    second = build_system_prompt(agent)

    assert first == second
    assert second.count("AGENT_OS_RULE_VERSION:") == 1
