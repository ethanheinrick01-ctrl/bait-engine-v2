# Bait Engine v2

A context-safe rebuild of the project's hardest layer first: the cognitive core.

This repo starts with the parts that are easiest to lose in chat and hardest to improvise later:
- domain models
- analysis pipeline
- decision engine
- branch prediction contracts
- storage-ready types

## Build doctrine

1. Lock the schemas before writing clever code.
2. Separate analysis from generation.
3. Make every decision inspectable.
4. Prefer deterministic scoring around the model, not model-only magic.
5. Keep work chunked into small, durable files so session loss does not rot the project.

## Phase order

- Phase 0: architecture + schemas
- Phase 1: analysis engine
- Phase 2: decision engine + branch prediction
- Phase 3: generation shell + CLI
- Phase 4: persistence + replay
- Phase 5: provider-backed generation
- Phase 6: adapters and UI ✅ complete
- Phase 7: execution rail + transport dispatch seam ✅ complete
- Phase 8: rhetoric scoring + persona reputation ✅ complete
- Phase 9: hunt intake rail + target discovery ✅ complete
- Phase 10: bite gate + mutation loop rail ✅ complete
- Phase 11: optimization rail ✅ complete (arteries 1–6 landed)
- Phase 12: auto persona router ✅ complete (arteries 1–4 landed)
- Phase 13: autopilot hardening ✅ complete (arteries 1–7 landed)

## Test status

**145 tests passing**

## Current CLI surface

### Core operations
- `analyze` — analyze a comment for signals, axes, archetypes, contradictions
- `plan` — build a tactical plan from analysis
- `draft` — generate candidate replies
- `runs` — list stored runs
- `personas` — list available personas
- `show-run` — display a specific run
- `replay` — replay a stored run with new parameters

### Assessment & reporting
- `autopsy` — inspect a single run outcome
- `autopsy-many` — summarize recent runs
- `scoreboard` — roll up engagement outcomes across stored runs
- `report` — bundle scoreboard with best bites, flops, watchlist
- `report-markdown` — render report as human-readable markdown
- `report-csv` — export report as CSV
- `record-outcome` — record engagement outcome for a run

### Hunt intake (Phase 9+)
- `hunt-preview` — preview targets from a hunt source
- `hunt-list` — list available intake targets
- `hunt-promote` — promote a target to active processing
- `hunt-cycle` — run a staged hunt cycle
- `hunt-run` — execute full hunt with dispatch

### Mutation & evolution (Phase 10+)
- `mutate-run` — mutate winners from a specific run
- `mutate-winners` — batch mutate recent winners
- `mutation-report` — inspect mutation inventory by persona/platform/tactic

### Adapter & dispatch
- `adapters` — list registered platform adapters
- `adapter` — show one registered adapter
- `adapter-preview` — compile a stored run into a transport-neutral reply envelope
- `context-preview` — validate inbound thread context payloads
- `target-preview` — normalize platform-specific target identifiers
- `recommend-preset` — recommend a platform preset from live thread context
- `emit-preview` — render a transport-specific dry-run request
- `dispatch-emit` — dispatch an approved outbox entry
- `dispatch-approved` — batch-fire approved backlog
- `dispatch-status` — check dispatch lifecycle status
- `redrive-dispatch` — retry failed dispatches

### Panel & dashboard
- `panel-preview` — build a local inspection panel payload
- `panel-serve` — host the panel as a local HTTP app with full cockpit
- `record-panel-review` — record operator review as ranking bias
- `outbox` — inspect the emit outbox

### Worker & daemon
- `worker-cycle` — run one worker pass (approval → dispatch)
- `worker-run` — run bounded worker cycles
- `daemon` — install/inspect/remove LaunchAgent-backed login daemon

## Generation controls

- `draft` supports `--persona auto` for deterministic router-based persona selection
- `draft` and `replay` support `--heuristic-only` for deterministic local drafting
- `draft` and `replay` support `--model`, `--base-url`, and `--timeout-seconds`
- `draft` and `replay` support `--mutation-source` (`auto` or `none`) to control mutation carryover
- Provider-backed generation falls back cleanly when credentials are absent or the provider fails

### Hunt intake controls
- `hunt-preview` supports `--persona auto` (Phase 12 Artery 1 scaffold)
- `hunt-preview`, `hunt-cycle`, and `hunt-run` support `--persona` and `--prior-days` to tune lane prior lookback
- Intake targets are auto-scored with effective_score (base_score + lane_prior boost)
- Deep/fast lane resolver uses effective_score with bite gate enforcement

### Assessment & scoring
- `autopsy-many` summarizes recent runs and supports `--persona`, `--platform`, and `--verdict`
- `scoreboard` rolls up engagement outcomes by persona, platform, objective, tactic, and exit state
- `report` bundles scoreboard with best bites, highest-ranked flops, and pending watchlist
- `report-markdown` renders as human-readable markdown with `--out`
- `report-csv` exports as CSV with `--out`

### Adapter & selection
- `adapter-preview` supports selection strategies: `rank`, `top_score`, `highest_bite`, `highest_audience`, `lowest_penalty`, `auto_best`
- `adapter-preview` supports surface presets via `--selection-preset` (`engage`, `audience`, `safe`, `default`)
- `adapter-preview` supports candidate filters: `--tactic`, `--objective`
- `recommend-preset` inspects thread/message metadata and returns inspectable recommendation metrics
- `target-preview` enforces adapter capabilities before returning a valid target

### Panel & review
- `panel-serve` exposes `/dashboard` and `/api/runs` for cockpit behavior
- `panel-serve` exposes `/api/draft` for direct run creation
- `panel-serve` exposes `/api/outbox`, `/api/stage-emit`, `/api/outbox-status` for outbox management
- `panel-serve` exposes `/api/dispatch-emit`, `/api/dispatch-approved`, `/api/dispatch-status`, `/api/dispatch-redrive` for dispatch control
- `panel-serve` exposes `/api/worker-cycle` for worker execution
- `panel-serve` exposes `/api/mutate-run`, `/api/mutate-winners` for mutation from cockpit
- `panel-serve` exposes `/api/daemon` for LaunchAgent control
- Outcome overlays support `--include-outcome-overlay` / `--no-include-outcome-overlay` and `--history-limit`
- Variant generation supports `--include-all-presets`, `--include-strategy-variants`, `--include-filter-variants`, `--variant-limit`
- Operator reviews persist as ranking bias via `record-panel-review`

### Worker & daemon
- `worker-cycle` and `worker-run` support `--include-failed-redrive`, `--redrive-limit`, `--min-failed-age-seconds`
- `daemon` supports `panel` and `worker` modes
- `daemon` supports `--max-cycles`, `--dispatch-limit`, `--driver`, `--out-dir`, `--interval-seconds`

## Adapter seam

Phase 6 starts with a neutral reply envelope instead of direct posting.
That envelope carries:
- selected candidate text
- platform + thread/reply identifiers
- run metadata for inspection and auditability

A registry layer exposes named adapters plus capability flags for:
- reddit
- x
- discord
- web

Inbound thread-context contracts describe what the engine is replying into:
- thread id
- subject
- recent messages
- root author handle

## Phase documentation

- Phase 6 closure: `docs/10_phase6_completion.md`
- Phase 7 closure: `docs/13_phase7_completion.md`
- Phase 9 closure: `docs/14_phase9_hunt_intake.md`
- Phase 10 closure: `docs/15_phase10_bite_evolution.md`
- Phase 11 optimization rail: `docs/16_phase11_optimization_rail.md`
- Phase 12 auto persona router: `docs/18_phase12_auto_persona_router.md`
- Phase 13 autopilot hardening: `docs/19_phase13_autopilot_hardening.md`
- Build order: `docs/00_build_order.md`

## Quick start

```bash
# Set up
python3 -m venv .venv
source .venv/bin/activate
pip install -e .

# Analyze a comment
python3 -m bait_engine.cli.main analyze --text "Your take is garbage and you should feel bad"

# Draft a reply
python3 -m bait_engine.cli.main draft --text "Your take is garbage" --persona dry_midwit_savant --platform reddit --save

# Hunt for targets
python3 -m bait_engine.cli.main hunt-preview --source reddit --persona dry_midwit_savant --limit 10

# Run the dashboard
python3 -m bait_engine.cli.main panel-serve --open
```

See `docs/00_build_order.md` for full architecture.
