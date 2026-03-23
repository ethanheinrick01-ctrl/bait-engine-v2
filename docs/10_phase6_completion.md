# Phase 6 Completion — Browser-First Cockpit Sealed

Phase 6 is the point where Bait Engine stopped being a CLI necromancy kit and became a persistent local operator surface.

This document is the closure record: what was delivered, how to smoke-test it, and what remains for later phases.

## Final verdict

**Phase 6 is complete.**

Not in the fake "done enough for chat" sense.
Done in the sense that the adapter/UI seam, local review loop, outbox staging, and daemon persistence all exist in the actual codebase with test coverage.

## What Phase 6 delivered

### Adapter seam
- transport-neutral reply envelope
- adapter registry with capability flags
- inbound thread-context contracts
- target normalization
- capability enforcement
- selection presets per surface
- inspectable preset recommendation metrics

### Preview + panel surface
- `adapter-preview`
- `emit-preview`
- `panel-preview`
- static HTML preview with client-side variant switching
- served localhost panel via `panel-serve`
- optional auto-open into browser

### Cockpit/dashboard surface
- `/dashboard`
- `/api/runs`
- latest-run default behavior
- recent-run switching
- quick draft creation from browser via `/api/draft`
- sticky local browser state for draft fields and last run id

### Variant inspection and ranking
- precomputed variants
- strategy/preset/filter controls
- historical outcome overlays
- operator review overlays
- deduped equivalent envelopes
- ranked variants by historical support
- explicit winner vs runner-up comparison
- dominant-advantage + per-metric delta display
- compact engaged / no_bite / pending badges

### Review + outbox loop
- direct review submission bridge
- persisted `promote` / `favorite` / `avoid` reviews
- local outbox staging via `/api/stage-emit`
- outbox filtering via `/api/outbox`
- outbox state transitions via `/api/outbox-status`
- outbox note editing in-browser

### Persistence / daemon surface
- CLI daemon command: `daemon {status|install|uninstall}`
- dashboard daemon controls
- `/api/daemon`
- LaunchAgent plist generation with:
  - `PYTHONPATH`
  - working directory
  - stdout/stderr log paths
  - `KeepAlive`
  - `RunAtLoad`

## Operator smoke path

This is the shortest real-body verification path for Phase 6.

### 1. Start the cockpit
```bash
python3 -m bait_engine.cli.main --db /tmp/bait.db panel-serve --open
```

Expected:
- browser opens
- dashboard renders
- latest run or empty-state dashboard loads

### 2. Create a run from the dashboard
- paste source text
- choose persona/platform/count
- click **Draft new run**

Expected:
- new run saved
- browser redirects into the run panel
- panel JSON reflects the new run id

### 3. Inspect variant ranking
- confirm variant buttons render
- confirm history/review rationale appears
- confirm winner vs runner-up comparison deltas render
- confirm outcome badges render

Expected:
- strongest variant surfaces first
- comparison section explains why it won

### 4. Submit a review
- click or submit a `promote`, `favorite`, or `avoid` action
- include notes if useful

Expected:
- review persists
- refreshed panel reflects stored operator signal

### 5. Stage to local outbox
- stage current emit locally

Expected:
- outbox entry appears on dashboard
- entry carries status + notes

### 6. Triage the outbox
- approve / restage / archive an entry
- edit the entry note body and save it

Expected:
- status updates persist
- note edits replace the stored note body cleanly

### 7. Verify daemon surface
- inspect daemon state from dashboard or CLI
- install daemon
- refresh state
- uninstall daemon

Expected:
- plist renders
- install/remove flows work without manual plist editing

## Test state at closure

At Phase 6 closure, the suite passes in full.

Reference checkpoint:
- `4fbe44c` — dashboard daemon persistence controls
- `d5f00d1` — tighten variant deltas and outbox note editing

## What is intentionally not part of Phase 6

These belong to later phases and should not be smuggled backward into closure criteria:
- real posting/delivery adapters
- richer per-platform API integrations
- execution workers that actually fire outbox items
- deeper author/thread metadata heuristics beyond the current inspectable recommendation layer
- broader multi-surface operational automation

## Phase 7 starting line

Phase 7 should begin from this assumption:

**The browser-first local cockpit is solved enough.**

So the next layer should focus on one of these, explicitly and without reopening Phase 6 architecture:
1. real transport delivery adapters
2. execution layer for staged outbox items
3. richer surface-aware recommendation heuristics
4. operational packaging / deployment hardening beyond the current local daemon rail

Phase 7 began with option **2**. See `docs/11_phase7_execution_rail.md`.
