# Continuous Completion Control Plane

## Purpose

Turn preflight from a one-shot preparation job into a persistent, agent-agnostic project completion function. The control plane should help work that is not started, about to start, already running, interrupted, or missing any prior draft/preflight.

The design deliberately keeps the global prompt small. Detailed state and evidence stay in project files and are loaded only when relevant, avoiding a permanent token tax on every agent turn.

## Architecture

1. **Independent completion instrument**: objective predicates that define done before or independently of the writer.
2. **Rolling capability DAG**: DONE/READY/RUNNING/BLOCKED/FUTURE/INVALIDATED plus dependency edges, READY_FRONTIER and CRITICAL_PATH.
3. **Continuous preflight**: current boundary detailed; next boundary almost executable; later work capability-level only.
4. **Durable execution journal**: append facts/events and track proven vs pending outputs so crash/resume starts at the first unproven boundary.
5. **Risk-adaptive validator**: focused validation for low-risk work, independent validation for high-risk boundaries.

## Modes

- **Bootstrap**: no accepted preflight exists. Locate prior work, derive accepted baseline, build a bounded completion roadmap, and prepare the first executable packet.
- **Prepare**: job not started. Build exact files/symbols/tests/invalidators/writer/closeout packet.
- **Refresh**: job about to start. Refresh volatile predicates only.
- **Shadow**: job running. Do not interfere with the writer; prepare independent next-boundary work read-only.
- **Advance**: job completed. Absorb machine evidence, update the DAG, promote ready work, continue automatically.
- **Recover**: interrupted. Reuse proven outputs and resume from the first unproven boundary.

## Efficiency laws

- Global rules stay concise; project detail is lazy-loaded.
- Do not create a preflight worker unless the task is non-trivial enough to benefit.
- Do not maintain a detailed plan more than one useful step ahead unless independent dependencies justify it.
- Do not spawn scouts with overlapping questions.
- Do not re-run accepted tests or discovery when no relevant invalidator fired.
- Full project replan is forbidden unless a project-level invalidator invalidates the accepted roadmap.
- High-risk validation is stronger; low-risk work should not pay high-risk ceremony.

## Authority and closeout

Remote Git is the last published durable truth, not automatically the freshest project truth. A closeout must compare fresh local and remote identities and relevant dirty/unpushed state.

Preflight must also plan the landing path: writer identity, acceptance witnesses, commit, receipt/evidence binding target, release/runtime impact and remote readback. Capability identity and governance closeout identity are distinct, preventing recursive receipt-rebind loops.

## Cross-agent deployment

The canonical global rule is `AGENT_OS_CORE.md`. `scripts/agent_os_install.py` installs that logic through documented global instruction surfaces:

- Codex: `~/AGENTS.md`
- Claude Code: `~/.claude/CLAUDE.md`
- Cursor: `~/.cursor/rules/continuous-completion-control-plane.mdc`
- Gemini CLI: `~/.gemini/GEMINI.md`

The installer preserves pre-existing content using one bounded managed block and is idempotent. `audit` is non-mutating and should be run before `apply`.

Hermes runtime integration should consume the same canonical rule and state primitives rather than copy the rules into a second authority. The existing prior-work-first implementation remains the locator/resume owner; this layer adds rolling DAG, completion and closeout semantics above it.

## Rollout

1. Audit all target user-level instruction files.
2. Canary apply to one agent and use it on a medium-risk repository task.
3. Verify rule loading, no prompt bloat/regressions, correct prior-work reuse, and no extra ceremony on simple tasks.
4. Apply to the remaining agents.
5. Wire Hermes controller to the deterministic state primitives only after canary evidence; do not hot-reload a busy Hermes runtime.

A successful system rollout requires effectiveness evidence, not merely files existing: lower phase-transition latency, fewer rediscovery cycles, fewer stale-baseline/receipt/PID mistakes, no increase in duplicate writers, and no material token inflation on simple tasks.
