# Continuous Completion Control Plane

Apply this to non-trivial project execution. For tiny one-shot tasks, keep the overhead proportional.

## Core objective
Finish the project safely with minimum time to durable, independently verifiable completion. Do not optimize for producing a plan; optimize for converging the project.

## Continuous planning
Preflight is not a one-shot task. Maintain a rolling completion map while work proceeds.

- If no preflight/draft exists, bootstrap a bounded completion roadmap from current project truth.
- Keep CURRENT detailed, NEXT nearly executable, and later work capability-level only.
- While CURRENT is running, prepare useful independent NEXT work read-only.
- After CURRENT passes, promote NEXT immediately and refresh only invalidated slices.
- Full-project replanning requires a project-level invalidator.

## Prior-work first
Before discovery or implementation, locate accepted handoffs, packets, receipts, drafts, branches/worktrees, ledgers and focused tests. Reuse accepted work unless an explicit invalidator fires. RAG/chat/history are locators, not implementation or PASS authority.

## Capability DAG
Represent remaining work as a dependency DAG with states: DONE, READY, RUNNING, BLOCKED, FUTURE, INVALIDATED. Maintain READY_FRONTIER and CRITICAL_PATH. Parallelize only independent useful work.

## One pen
There is at most one canonical primary writer per capability/repository authority. Parallel agents may scout, validate, prepare evidence, or prepare the next boundary read-only. Stale candidates never auto-merge after canonical HEAD changes.

## Completion instrument
Define objective completion predicates before or independently of implementation. The writer may not weaken them to make its patch pass. Claims marked true require an exact machine witness; otherwise use unproven.

## Durable execution
Track proven outputs separately from pending outputs. Resume from the first unproven boundary. Do not rerun already-proven work unless invalidated.

For mutating lifecycles, record durable events such as STARTED, WRITER_ACQUIRED, PATCH_CREATED, TEST_PASS, COMMIT_CREATED, PUSH_PASS, REMOTE_READBACK_PASS, RECEIPT_BOUND, WRITER_RELEASED, PASS. External side effects must be idempotent or explicitly at-most-once/fail-closed.

## Local/remote authority
GitHub/remote is LAST_PUBLISHED_DURABLE_TRUTH until fresh local/remote parity is proven.

Before durable PASS classify fresh LOCAL_HEAD/TREE, REMOTE_HEAD/TREE, relevant dirty state, active transaction and unpushed completed work. Require REMOTE_READBACK_PASS and NO_UNPUSHED_COMPLETED_TRANSACTION for remote-backed closeout.

## Closeout planning
Preflight must include the landing path, not only implementation:

- exact writer identity and ownership
- acceptance witnesses
- commit plan
- receipt/evidence binding target
- release/runtime impact
- remote parity plan

Distinguish capability commit identity from later governance/evidence closeout commits to avoid recursive rebind loops.

## Risk-adaptive rigor
Use the cheapest validation that preserves confidence.

- Low risk: focused tests and compact validator.
- Medium risk: independent boundary validator.
- High risk (release, auth, money/broker, destructive actions, reconciliation): strong independent validation and fail-closed authority checks.

Do not spawn agents, scan repos, reread design docs, or rerun regressions merely because parallelism is available.

## Reporting
Report durable milestones or genuine hard blockers. A waiting state is not completion when useful monitoring or next-boundary preparation can continue.
