# Phase 12 — Auto Persona Router

## Status

**In progress**
- Artery 1 ✅ complete (`9ab3157`)
- Arteries 2–4 ⏳ pending

## Artery 1 (landed)

Commit: `9ab3157`

Delivered:
- `--persona auto` routing scaffold for:
  - `draft` (save + non-save)
  - `hunt-preview`
  - hunt promote/cycle wiring
- New deterministic router module:
  - `src/bait_engine/planning/router.py`
- Inspectable routing outputs:
  - `selected_persona`
  - `persona_scores`
  - `confidence`
  - `why_selected`
- Router metadata persisted in saved runs via `plan.persona_router`
- Historical priors blended through repository reputation lookup (confidence-gated)
- Test suite checkpoint at completion: **114 tests passing**

## Remaining arteries

### Artery 2 — Router calibration from outcomes
- Learn/update persona routing weights from historical outcomes by segment (platform/objective)
- Confidence gating for sparse segments
- Deterministic fallback to scaffold/static behavior when history is weak
- Persist calibration metadata (version/timestamp/confidence)

### Artery 3 — Close-score duel logic
- If top persona scores are within threshold, run top-2 duel path
- Select winner by deterministic tie-break and inspectable rationale
- Preserve cost controls and deterministic mode behavior

### Artery 4 — Reporting + operator controls
- Add router observability to reports/dashboard:
  - auto-pick accuracy
  - confidence distribution
  - per-persona drift over time
- Add explicit override audit trail (forced persona vs auto)

## Done criteria for Phase 12
- Auto persona selection is reliable, inspectable, and confidence-aware
- Sparse-data fallback is deterministic and stable
- Close-score behavior is explicit (not arbitrary)
- Reporting surfaces routing quality and drift
- Full test suite remains green
