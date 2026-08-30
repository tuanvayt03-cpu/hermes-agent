#!/usr/bin/env python3
"""Safely install the Continuous Completion Control Plane as user-level rules.

The installer is deliberately conservative: audit is default, writes are
idempotent, existing files are preserved, and only a bounded managed block or
one dedicated Cursor rule is touched.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

BEGIN = "<!-- AGENT_OS_CONTINUOUS_COMPLETION:BEGIN -->"
END = "<!-- AGENT_OS_CONTINUOUS_COMPLETION:END -->"
SUPPORTED = ("codex", "claude", "cursor", "gemini")


def core_path() -> Path:
    return Path(__file__).resolve().parents[1] / "AGENT_OS_CORE.md"


def core_text() -> str:
    return core_path().read_text(encoding="utf-8").strip()


def managed_payload() -> str:
    return f"{BEGIN}\n{core_text()}\n{END}\n"


def upsert_managed_block(path: Path, *, apply: bool) -> dict:
    before = path.read_text(encoding="utf-8") if path.exists() else ""
    payload = managed_payload()
    if BEGIN in before or END in before:
        if before.count(BEGIN) != 1 or before.count(END) != 1 or before.index(BEGIN) > before.index(END):
            raise RuntimeError(f"MALFORMED_MANAGED_BLOCK:{path}")
        start = before.index(BEGIN)
        end = before.index(END) + len(END)
        after = before[:start] + payload.rstrip("\n") + before[end:]
    else:
        prefix = before.rstrip()
        after = (prefix + "\n\n" if prefix else "") + payload
    changed = after != before
    if apply and changed:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(after, encoding="utf-8")
    return {"path": str(path), "changed": changed, "mode": "managed_block"}


def cursor_rule() -> str:
    return "---\ndescription: Continuous project completion control plane\nalwaysApply: true\n---\n\n" + core_text() + "\n"


def upsert_cursor(path: Path, *, apply: bool) -> dict:
    before = path.read_text(encoding="utf-8") if path.exists() else ""
    after = cursor_rule()
    changed = before != after
    if apply and changed:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(after, encoding="utf-8")
    return {"path": str(path), "changed": changed, "mode": "dedicated_rule"}


def targets(home: Path, agents: Iterable[str]) -> list[tuple[str, Path]]:
    out: list[tuple[str, Path]] = []
    for agent in agents:
        if agent == "codex":
            out.append((agent, home / "AGENTS.md"))
        elif agent == "claude":
            out.append((agent, home / ".claude" / "CLAUDE.md"))
        elif agent == "cursor":
            out.append((agent, home / ".cursor" / "rules" / "continuous-completion-control-plane.mdc"))
        elif agent == "gemini":
            out.append((agent, home / ".gemini" / "GEMINI.md"))
        else:
            raise ValueError(f"UNSUPPORTED_AGENT:{agent}")
    return out


def install(home: Path, agents: list[str], *, apply: bool) -> dict:
    results = []
    for agent, path in targets(home, agents):
        result = upsert_cursor(path, apply=apply) if agent == "cursor" else upsert_managed_block(path, apply=apply)
        result["agent"] = agent
        results.append(result)
    return {
        "apply": apply,
        "home": str(home),
        "agents": agents,
        "changes_required": sum(1 for r in results if r["changed"]),
        "results": results,
    }


def parse_agents(value: str) -> list[str]:
    values = [part.strip().lower() for part in value.split(",") if part.strip()]
    if value.strip().lower() == "all":
        values = list(SUPPORTED)
    bad = [x for x in values if x not in SUPPORTED]
    if bad:
        raise argparse.ArgumentTypeError("unsupported agent(s): " + ",".join(bad))
    return values


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("audit", "apply"), nargs="?", default="audit")
    parser.add_argument("--home", type=Path, default=Path.home())
    parser.add_argument("--agents", type=parse_agents, default=list(SUPPORTED))
    args = parser.parse_args()
    result = install(args.home.resolve(), args.agents, apply=args.mode == "apply")
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
