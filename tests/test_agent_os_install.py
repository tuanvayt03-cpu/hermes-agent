from pathlib import Path

from scripts.agent_os_install import BEGIN, END, install


def test_audit_does_not_write(tmp_path: Path):
    result = install(tmp_path, ["codex", "claude", "cursor", "gemini"], apply=False)
    assert result["changes_required"] == 4
    assert not any(tmp_path.rglob("*"))


def test_apply_is_idempotent_and_preserves_existing_content(tmp_path: Path):
    agents = tmp_path / "AGENTS.md"
    agents.write_text("# Existing\nkeep me\n", encoding="utf-8")
    first = install(tmp_path, ["codex"], apply=True)
    assert first["changes_required"] == 1
    text = agents.read_text(encoding="utf-8")
    assert text.startswith("# Existing\nkeep me")
    assert text.count(BEGIN) == 1 and text.count(END) == 1
    second = install(tmp_path, ["codex"], apply=True)
    assert second["changes_required"] == 0


def test_cursor_uses_dedicated_global_rule(tmp_path: Path):
    install(tmp_path, ["cursor"], apply=True)
    path = tmp_path / ".cursor" / "rules" / "continuous-completion-control-plane.mdc"
    text = path.read_text(encoding="utf-8")
    assert "alwaysApply: true" in text
    assert "Continuous Completion Control Plane" in text
