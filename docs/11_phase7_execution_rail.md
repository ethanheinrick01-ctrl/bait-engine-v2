# Phase 7 — Execution Rail (First Cut)

Phase 7 starts by solving the first honest problem after Phase 6 closure:

**approved emits were still dead letters.**

They could be reviewed, ranked, staged, filtered, and annotated — but not actually handed off through a durable execution rail.

This document records the first Phase 7 cut: a manual local dispatch layer that executes approved outbox items into auditable artifacts without pretending real transport adapters already exist.

## What this cut adds

### Dispatch persistence
- new `emit_dispatches` table
- dispatch history attached to each run
- persisted dispatch request + response payloads
- optional operator notes per dispatch event
- explicit lifecycle statuses: `dispatched`, `acknowledged`, `delivered`, `failed`

### Manual execution driver
- `manual_copy` driver
- writes one JSON artifact per dispatched emit
- stores artifacts under `.data/dispatches/` by default
- leaves a durable handoff payload for later transport workers or human operators

### CLI surface
- `outbox [--status ...]`
- `dispatch-emit <emit_id> [--driver manual_copy] [--out-dir PATH] [--notes ...] [--force]`
- `dispatch-approved [--limit N] [--driver manual_copy] [--out-dir PATH] [--notes ...]`
- `dispatch-status <dispatch_id> <dispatched|acknowledged|delivered|failed> [--notes ...] [--notes-mode append|replace]`
- `redrive-dispatch <dispatch_id> [--driver manual_copy] [--out-dir PATH] [--notes ...]`
- `worker-cycle [--dispatch-limit N] [--driver manual_copy] [--out-dir PATH] [--notes ...] [--include-failed-redrive] [--redrive-limit N] [--min-failed-age-seconds S]`
- `worker-run [--dispatch-limit N] [--driver manual_copy] [--out-dir PATH] [--notes ...] [--interval-seconds S] [--max-cycles N] [--include-failed-redrive] [--redrive-limit N] [--min-failed-age-seconds S]`

### Dashboard surface
- approved outbox entries expose a **Dispatch** button
- dashboard exposes **Dispatch approved** for batch handoff
- dashboard exposes **Run worker cycle** for an explicit operator-fired pass
- dashboard exposes worker failed-redrive controls (`include_failed_redrive`, `redrive_limit`, `min_failed_age_seconds`) for cockpit-driven retry policy
- dashboard daemon controls can now install either `panel` or `worker` LaunchAgent modes
- worker-mode daemon controls also surface retry policy defaults (`include_failed_redrive`, `redrive_limit`, `min_failed_age_seconds`) so LaunchAgent installs can preserve the same worker behavior the cockpit previews
- dashboard one-shot worker controls and worker-daemon install controls mirror the same retry-policy values, keeping operator defaults aligned across manual cycles and login workers
- daemon card now summarizes both worker retry policy and worker launch config (`dispatch_limit`, `driver`, `interval_seconds`, `max_cycles`, `out_dir`) so operators can verify behavior without reading raw plist XML
- worker-mode daemon installs can now carry optional `max_cycles` for bounded staging/test workers without lying that every LaunchAgent must be infinite
- dashboard daemon controls now expose `max_cycles` directly, keeping bounded worker staging visible in the cockpit instead of CLI-only
- dashboard daemon controls also expose `dispatch_limit`, `driver`, `out_dir`, and `interval_seconds`, so worker LaunchAgent install shape is fully adjustable in-browser
- worker driver selection in the daemon cockpit is now constrained to the registered driver list (currently `manual_copy` only) instead of a free-text field that could silently install garbage
- daemon card surfaces worker-config warnings when settings are operationally suspicious (for example bounded `max_cycles` under `KeepAlive`, very tight intervals, oversized batch limits, zero-cooldown failed redrive, or an unknown worker driver)
- dispatch history exposes one-click lifecycle actions for acknowledge / deliver / fail
- failed dispatches expose a **Redrive** action
- dashboard shows dispatch count
- dashboard shows manual artifact directory
- dashboard shows recent dispatch history
- dashboard POST surface now includes `/api/dispatch-emit`, `/api/dispatch-approved`, `/api/dispatch-status`, `/api/dispatch-redrive`, `/api/worker-cycle`, and `/api/daemon`

## Truthfulness boundary

This is **not** a real posting adapter yet.

What it does:
- validates the outbox state machine enough to require `approved` before dispatch by default
- writes a durable artifact containing the selected envelope + emit request
- records that handoff in storage
- marks the outbox item as `dispatched`
- allows later lifecycle truth updates to `acknowledged`, `delivered`, or `failed`
- optionally lets worker passes redrive currently-live failed dispatch heads without reviving stale historical failures
- can hold failed heads behind a minimum-age cooldown before they become eligible for worker redrive

What it does **not** do yet:
- call Reddit/X/Discord/web APIs
- guarantee delivery to a real external surface
- retry failed network deliveries automatically
- auto-heal or supervise a real transport adapter beyond the local worker/manual_copy seam

That restraint matters. Fake delivery semantics would poison later phases.

## Default artifact shape

Each manual dispatch artifact contains:
- dispatch driver
- dispatch status
- emit id
- artifact path
- full payload bundle
  - envelope
  - emit request
  - transport metadata
  - selection metadata
  - stored notes

## Minimal operator flow

1. review candidate in panel
2. stage to local outbox
3. approve the emit
4. click **Dispatch** in dashboard or run `dispatch-emit <id>`
5. inspect the artifact in `.data/dispatches/`
6. hand it to the next real transport layer or external operator workflow

## Why this is the right Phase 7 opening

It solves the gap between "approved" and "actually handed off" without prematurely welding in platform-specific posting code.

That gives the project:
- a real state transition after approval
- durable artifacts for audits and replay
- a stable seam for later transport-specific workers
- zero lies about external delivery

## Likely next steps

1. add explicit success/failure/ack lifecycle beyond `dispatched`
2. add real transport drivers behind the same dispatch seam
3. add retry/error metadata once real delivery exists
4. add an autonomous worker that reuses `dispatch-approved` instead of inventing a second path
