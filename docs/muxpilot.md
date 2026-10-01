# Muxpilot integration protocol v1

Multica retains its board, issues, agent dispatch, runtime profiles, runs and
history. Muxpilot adds one scoped external coordinator per registered project.
The paired Muxdeck project service owns the local journal and terminal links;
the Multica daemon owns provider process stdin/stdout and cancellation.

## Bootstrap and authorization

Use the managed checkout environment (`make worktree-env`, `make up`), not a
copy of another checkout's `.env`. PostgreSQL migrations 564–567 install the
opt-in coordinator, issue/run mappings, operation receipts and durable events.
`BIND_HOST=127.0.0.1` limits the backend listener to loopback. Preserve the existing
authenticated access boundary when proxying the web app or backend.

A human Multica session or PAT with `X-Workspace-ID` calls these routes:

- `POST /api/muxpilot/projects/{project_uuid}/register` with `operation_id`,
  `name`, `goal`, `repo_root`, and `daemon_id`. Registration creates/reuses the
  exact project UUID and a native `local_directory` resource in `worktree` mode.
  The registered daemon must belong to the operator and advertise worktree support.
- `POST /api/muxpilot/projects/{project_uuid}/lease` with `operation_id`, optional
  `expected_generation`, `worker_limit` (1–64), and `runtime_profile_id`. First
  acquisition returns generation 1; an explicit replacement increments it and
  holds dispatch. A repeated operation returns the same capability.
- `GET /api/muxpilot/projects/{project_uuid}/recovery` reads persisted ownership,
  expiry, dispatch hold, control reservations and receipt IDs without renewing
  or adopting an expired coordinator.

The approved runtime profile must be an enabled `muxpilot-worker` or
`muxpilot-worker-fake` wrapper in the same workspace. Unconfigured projects may
create unowned planning issues but cannot dispatch agent workers. A task's native
runtime must match the approved profile and configured daemon/resource. Profiles
that change to an arbitrary command, become disabled, or omit the wrapper fail
closed at the dispatch/start receiver.

Lease responses contain an `mxpc_` bearer capability, generation and expiry. The
capability authenticates only the scoped routes below, never ordinary Multica or
account APIs. Tokens and their raw hashes do not enter events or operation
responses persisted for replay. Membership removal revokes both command and
provider-input authority. Workers retain their existing task-scoped credentials.

## Coordinator routes

Use `Authorization: Bearer <capability>` and `X-Muxpilot-Generation: <generation>`.
Every mutation locks and validates the current epoch in its transaction. Every
command includes a UUID `operation_id`; exact retries return the committed
receipt and changed-body reuse returns `operation_conflict`.

`POST /api/muxpilot/projects/{project_uuid}/commands` accepts:

| action | Fields and result |
| --- | --- |
| `create_task` | `title`, `description`, `acceptance`, `stage`, optional `parent_issue_id`/`agent_id`; always creates Backlog/no-start |
| `activate_stage` | `stage`, exact `base_sha`; verifies prior nonzero stages were accepted as Done, activates an eligible batch and records the run pin |
| `hold` | `held`; controls future dispatch while retaining running workers |
| `renew` | extends the current lease without replacing its token or generation |
| `update_task` | `issue_id`, `status` (`done`, `backlog`, `cancelled`); task acceptance remains a coordinator decision |
| `scope_remove` | `issue_id`, `content` decision; explicitly excludes work from stage prerequisites |
| `supplement` | exact native run `task_id`, `content`; returns pending delivery, never an invented acknowledgement |
| `cancel` | exact native run `task_id`; requests cancellation and explicitly does not claim provider termination |
| `continue` | ended exact `task_id`; creates a distinct queued attempt with lineage; accepted work must be reopened first |
| `adopt` / `adopt_run` | reconciled exact `task_id`; updates its receiver epoch, preserving the same attempt |
| `bind_terminal` | exact `task_id`, HTTP(S) `terminal_url`, full `session_id` identity, `terminal_state` (`live`, `history`, `unavailable`) |
| `remap_repository` | `repo_root`, `daemon_id`, `content` decision; requires all attempts to have ended and holds future dispatch |
| `revoke` | expires authority and holds dispatch; in-flight input reservations must be resolved first |

Stage pins are full lowercase 40/64-character commit IDs. Native isolated task
worktrees start at the exact commit, never silently at a later source HEAD.
Source staged/unstaged/untracked edits remain in the source directory; the
coordinator must snapshot any intended dirty baseline into its owned commit
before selecting that pin. Verification stages receive the accepted integration
commit. Native mention, wakeup, rerun and claim paths cannot bypass stage,
revision, hold, epoch, wrapper profile or project worker-limit gates. Stage 0
planning epics are excluded from prior-stage prerequisites. Cancelled work does
not satisfy prerequisites without an explicit recorded scope removal.

`GET /snapshot` returns current issues, separate run attempts, approved agent
roster, dispatch/lease state, terminal associations and control reservations.
`GET /operations/{operation_uuid}` reconciles a lost receipt.

## Durable event pages

`GET /events?after=<cursor>&limit=<1..1000>` returns `events`, `cursor`,
`prev_cursor`, `page_complete`, `has_more`, and `retention_gap`. Source mutations
and events commit together through PostgreSQL triggers. Pages include every
retained project event after the supplied cursor up to the stated limit; no
retention deletion is implemented, so `retention_gap` is false.

Sequence numbers are global and therefore sparse for an individual project
(other projects and rolled-back sequence allocations create gaps). Consumers
must use the authoritative predecessor/completeness contract, strictly ordered
source IDs, and deduplication; a numeric gap alone is not missing history.
Events include stable ID, project/workspace, actor, UTC occurrence, type and an
allowlisted payload. Provider credentials, runtime configuration, prompts and
unbounded execution output are excluded. Native human issue edits retain their
trusted transaction actor; coordinator tasks use `external_coordinator`
attribution and guidance is visibly identified as coordinator input.

## Input reservation and recovery

Coordinator guidance is associated with the exact attempt and epoch. Claiming
it locks the coordinator before the receipt and commits a `delivery_active`
reservation. Lease replacement and revocation return `delivery_in_progress`
until that reservation has a factual daemon acknowledgement. Codex, Claude and
Grok revalidate authority at their actual outbound input boundary; Claude keeps
the gate through queued hook delivery. Ordinary human guidance retains its
existing semantics.

A typed pre-write authority denial can safely report failure. A provider transport
error after input was attempted is recorded as `outcome_unknown`; it retains the
reservation. Cancellation, task completion, backend restart, coordinator expiry
or a lost receipt never silently release it. A later factual delivered/failed
acknowledgement settles it. An unavailable provider outcome requires operator
reconciliation through readonly recovery; new ownership is blocked while it is
ambiguous. There is no raw-keystroke injection or native interactive worker
handoff in this protocol.

## Validation and rollout

Default tests use temporary PostgreSQL schemas and fake provider transports.
Focused tests cover staged/held claims, exact pins and source preservation,
project wrapper enforcement, generation changes, durable rollback/replay,
concurrent capacity, claim/takeover locking, lost-input uncertainty, late receipts,
actor attribution and safe run links. Real provider smoke tests require explicit
authorization, the `agentintegration` tag and `MULTICA_RUN_REAL_AGENT_SMOKE=1`.
Deploy both pinned repositories with activation disabled until capabilities and
the paired isolated scenario pass. Preserve the previous pair and data, use
SQLite-aware backups in the project service, and never restart tmux or terminate
unrelated sessions as part of installing this integration.
