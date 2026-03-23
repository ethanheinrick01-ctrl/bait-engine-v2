# Phase 10 Progress — Bite Gate + Mutation Loop Rail

Phase 10 targets the missing spine after intake:

> the system could discover and dispatch,
> but it still lacked a hardened **winner evolution loop**.

This checkpoint locks in the first real mutation rail.

## Delivered in Phase 10 so far

### 1) Bite qualification gate is now first-class

Run scoring now carries explicit bite detection primitives (`score.bite_detection`) and deep-lane routing requires bite qualification before escalation.

Outcome:
- weak/no-bite targets are de-prioritized early
- deep/provider spend is gated by actual bite signals

### 2) Persona pressure profiles are wired end-to-end

Persona pressure behavior is now threaded through:

- planner
- prompt payload
- provider writer prompt
- heuristic writer behavior

This prevents personas from being cosmetic skins and makes them apply distinct pressure signatures.

### 3) Mutation context now feeds drafting/replay

`DraftRequest` now includes mutation context fields:

- `mutation_context`
- `winner_anchors`
- `avoid_patterns`

These are derived from selected mutation seeds and passed into prompt payload + writer logic.

Heuristic writer now applies:
- anchor hints (periodic winner-style lexical hooks)
- stale-pattern stripping (e.g., hedge language cleanup)

### 4) Mutation source controls added

`draft` and `replay` now support:

- `--mutation-source auto` (default)
- `--mutation-source none`

This allows strict A/B runs with mutation disabled.

### 5) New run-level mutation commands

Added CLI surfaces:

- `mutate-run <run_id>`
- `mutation-report`

These enable:
- mutating a specific run winner into variant families
- reporting mutation inventory by persona/platform/tactic/objective/status

### 6) Repository mutation internals hardened

Added:

- reusable `_mutate_winner_record(...)`
- `mutate_run(...)`
- `mutation_report(...)`
- richer mutation variant listing joins with family metadata

## Operator smoke paths

### Draft without mutation carryover

```bash
PYTHONPATH=src python3 -m bait_engine.cli.main draft \
  --text "Your certainty is louder than your evidence." \
  --persona dry_midwit_savant \
  --platform reddit \
  --save \
  --heuristic-only \
  --mutation-source none
```

### Replay run with mutation disabled

```bash
PYTHONPATH=src python3 -m bait_engine.cli.main replay 42 \
  --heuristic-only \
  --mutation-source none
```

### Mutate a single run winner

```bash
PYTHONPATH=src python3 -m bait_engine.cli.main mutate-run 42 \
  --variants-per-winner 5 \
  --strategy controlled_v1
```

### Inspect mutation inventory

```bash
PYTHONPATH=src python3 -m bait_engine.cli.main mutation-report \
  --persona dry_midwit_savant \
  --platform reddit \
  --limit 100
```

## Tests at this checkpoint

Coverage added/extended for:

- mutation source off path in replay payloads
- run-level mutate command flow
- mutation report filters and summary counters
- mutation context + anchors propagation assertions

Validation command:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests
```

Result at this checkpoint: **90 tests passing**.

## Phase 10 closure strike

Completed continuation targets:

1. ✅ fed mutation performance back into seed selection weights (status/quality/recency/diversity scoring)
2. ✅ moved from static transforms to adaptive transform policy (history-weighted ordering)
3. ✅ added persona pressure adaptation from observed reply tone shifts (rebuild + draft context cues)
4. ✅ mutation seed lifecycle now tracks selection state (`drafted` → `selected` on use)

Deferred to Phase 11 (optimization rail):

- fast-lane escalation tuned directly by mutation family outcome aggregates at intake-time (cross-run lane prior)
