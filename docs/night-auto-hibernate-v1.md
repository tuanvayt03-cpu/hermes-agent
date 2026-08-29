# Hermes Night Auto-Hibernate V1 — candidate handoff

Status: implementation candidate only. It is not connected to the live slash
command registry, does not activate Notification V3, and contains no real
hibernate implementation.

## Phase 0 authority resolution

Baseline inspected before any implementation change:

- Hermes version: `hermes_cli.__version__` in `hermes_cli/__init__.py` (`0.20.5`).
- Base repository: branch `main`, commit
  `08b4875f4a8af7a2162666fc0de0043b2db7ff5d`.
- Night command surface owner: `hermes_cli/commands.py::COMMAND_REGISTRY`; no
  `/night` entry exists at the baseline.
- Master-task and kanban state owner: `hermes_cli/kanban_db.py::Task` and its
  lifecycle writers (`create_task`, `claim_task`, `heartbeat_claim`,
  `complete_task`, `block_task`, `request_review`, `request_changes`, and
  `reconcile_orphaned_running`).
- Process registry owner: `tools/process_registry.py::ProcessRegistry` and
  `ProcessSession`.
- Watchdog owner: `gateway/shutdown_watchdog.py` for process/event-loop
  liveness, with task-stall observation in `gateway/session_stall.py`.
- Notification router owner: dependency worktree
  `gateway/master_task_notifications.py::CanonicalNotificationRouter` at
  `f44b86616442ef8058a7110122cf8f5775d5c57e`.
- Consent owner: `tools/approval.py::_ApprovalEntry`,
  `has_blocking_approval`, `get_pending_gateway_approval`, and
  `resolve_gateway_approval`.
- Existing platform suspend owner: `gateway/scale_to_zero.py::suspend_self`, a
  Fly-only scale-to-zero primitive. It is not used by Night V1.
- Existing session resume owners: `gateway/session.py` (`resume_pending`),
  `gateway/run.py` (startup resume recovery), and the CLI/gateway `/resume`
  paths. Night recovery instead delegates exact master-task continuation to
  the existing master-task lifecycle adapter.

`FIRST_UNPROVEN_BOUNDARY` was the absence of an explicit `/night` admission
owner, unified quiescence proof, dedicated durable semantic resume manifest,
`NIGHT_HIBERNATION_READY` support, safe local hibernate abstraction, and exact
master-task wake reconciler. Existing task/process/consent/watchdog/notification
owners remain dependencies; none is duplicated.

## Candidate ownership

- Command boundary: `hermes_cli/night/command.py`.
- Admission and quiescence contracts: `hermes_cli/night/contracts.py`.
- Durable manifest/session receipts: `hermes_cli/night/manifest.py`.
- Notification V3 event and exact-signature adapter:
  `hermes_cli/night/notification.py`.
- Dry-run-only platform executor: `hermes_cli/night/executor.py`.
- Canonical request/wake state machine: `hermes_cli/night/workflow.py`.

The namespace exposes no task creation method and no Telegram sender. The
Notification adapter delegates to V3's exact `route(event, telegram_sender)`
signature; the existing gateway delivery owner must inject the sender at the
future join boundary.

## Fail-closed lifecycle

1. Accept only the exact `/night` command with
   `NIGHT_REQUESTED_BY_OPERATOR=true`.
2. Refuse unresolved consent, unknown task/safety state, ambiguous writer
   ownership, unavailable persistence, a non-completed/non-parked task, or
   unknown repository/runtime identity.
3. Write and read back a dedicated manifest using atomic replace, file fsync,
   sorted-key canonical JSON, and SHA-256. The hash covers every manifest field
   except `MANIFEST_HASH` itself. Secret-like material is rejected.
4. Prove `NIGHT_SAFE_TO_HIBERNATE`, including no mutation, tool call, required
   child process, pending consent, unbound writer, in-flight notification, or
   Watchdog repair. Watchdog must remain available.
5. Persist `NOTIFICATION_IN_FLIGHT` before calling Notification V3. An
   ambiguous outcome is never resent or converted into a replacement event.
6. Re-read every admission/task/quiescence predicate after notification. Any
   change aborts before the executor.
7. Accept only an executor whose `dry_run` flag and returned receipt are both
   true. V1 ships only `DryRunHibernateExecutor`.
8. On wake, validate the manifest hash, stored link, task/repository/runtime
   identity, worker/process ownership, notification route, and Watchdog
   availability. Watchdog revalidates the task before a durable
   `RESUME_IN_FLIGHT` claim.
9. Resume through `resume_existing(master_task_id, boundary, instruction)`.
   There is no blind tool replay and no new engineering task API. A second wake
   returns `ALREADY_RESUMED` without invoking the lifecycle again.

Night always records `WATCHDOG_RECOVERY_ACTION=NOOP`; it never disables or
logically hibernates Watchdog. If Watchdog is repairing, Night waits.

## Durable schema

Default integration should place the store in a new Night-specific runtime
directory, for example `<HERMES_HOME>/runtime/night-v1/`. Tests use a temporary
directory. The implementation never opens or mutates `state.db`, `kanban.db`,
or a Watchdog database.

Manifest fields are:

`MASTER_TASK_ID`, `MASTER_TASK_NAME`, `TASK_STATUS`, `CURRENT_CAPABILITY`,
`FIRST_UNPROVEN_BOUNDARY`, `LAST_DURABLE_CHECKPOINT`, `ACTIVE_WORKERS`,
`PROCESS_IDENTITIES_IF_REQUIRED`, `PENDING_OPERATOR_ACTION`,
`RESUME_INSTRUCTION`, `CREATED_AT`, `MANIFEST_ID`, and `MANIFEST_HASH`.

Repository and runtime identity are also persisted as recovery invariants.
Night session IDs use:

`night-<UTC YYYYMMDDTHHMMSSffffffZ>-<12 lowercase UUID hex>`.

Notification dedup keys use:

`night:<NIGHT_SESSION_ID>:NIGHT_HIBERNATION_READY`.

## Live join gate (deferred)

Do not register `/night`, import the dependency into the running runtime, send
a live Telegram message, or add a real hibernate executor until all of these
are proven at the current machine state:

1. Notification V3 Phase 10 passes and its canonical formatter accepts
   `NIGHT_HIBERNATION_READY`.
2. SignalOps effective primary writer count is zero.
3. No SignalOps mutation is active.
4. No SignalOps worker depends on a Hermes restart.

The join must occur without restarting the currently running Hermes process as
part of this candidate task. Production promotion remains forbidden.

## Focused validation

Run offline:

```powershell
python -m pytest tests/night/test_night_workflow.py -q -s --basetemp <temp-path>
```

The test matrix covers required cases A–J plus final-predicate race rejection,
non-dry-run refusal, explicit operator intent, secret rejection, and the exact
Notification V3 adapter signature.
