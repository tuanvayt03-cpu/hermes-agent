# Continuous Completion Control Plane

Use this for non-trivial project execution. For tiny one-shot tasks, add no planning ceremony.

1. **Converge the project, not the plan.** Optimize for minimum time to durable, independently verifiable completion.
2. **Prior-work first.** Before rediscovery or implementation, locate accepted packets, handoffs, receipts, drafts, branches/worktrees, ledgers and focused tests. Reuse accepted work unless an explicit invalidator fires. RAG/chat/history are locators only.
3. **Preflight is continuous.** If no preflight exists, bootstrap a bounded completion roadmap. Keep CURRENT detailed, NEXT nearly executable, later work capability-level. While CURRENT runs, prepare useful independent NEXT work read-only. Refresh only invalidated slices; full replan requires a project-level invalidator.
4. **Use a capability DAG.** Track DONE/READY/RUNNING/BLOCKED/FUTURE/INVALIDATED plus READY_FRONTIER and CRITICAL_PATH. Parallelize only independent useful work.
5. **One pen.** At most one canonical primary writer per authority. Parallel agents may scout, validate, prepare evidence or the next boundary read-only. Stale candidates never auto-merge after canonical HEAD changes.
6. **Independent completion instrument.** Define objective PASS predicates before or independently of implementation. Writers may not weaken them. `true` requires an exact machine witness; otherwise use `unproven`.
7. **Resume proven work.** Track PROVEN vs PENDING outputs and resume from the first unproven boundary. Do not rerun accepted work without invalidation. External side effects must be idempotent or explicitly at-most-once/fail-closed.
8. **Plan the landing path.** Before writing, know writer identity, acceptance witnesses, commit plan, evidence/receipt binding target, release/runtime impact and remote readback. Keep capability commit identity distinct from later governance closeout commits.
9. **Remote is published truth, not automatically freshest truth.** Before durable PASS classify fresh local/remote HEAD/TREE, relevant dirty state, active transaction and unpushed completed work. Require remote readback and no unpushed completed transaction for remote-backed closeout.
10. **Risk-adaptive rigor.** Low risk gets focused validation; medium risk gets an independent boundary check; high-risk release/auth/money/destructive/reconciliation work gets strong independent fail-closed validation. Do not spawn agents, scan broadly or rerun regressions just because parallelism is available.
11. **Report durable milestones or genuine hard blockers.** Waiting is not completion when monitoring or next-boundary preparation can continue.
