# Phase 11 — Optimization Rail (In Progress)

Phase 11 is the post-Phase-10 optimization pass: use live outcomes to improve routing, mutation quality, and persona pressure realism.

## Scope

### Arteries 1–2 (Completed)

1. ✅ Mutation-family lane prior scoring at intake (`RunRepository.get_lane_prior(...)`)
2. ✅ Effective score routing (`score.effective_score`, `score.lane_prior`) in hunt lane resolver

Operational knobs now live in CLI:
- `hunt-preview --persona --prior-days`
- `hunt-cycle --prior-days`
- `hunt-run --prior-days`

Checkpoint: **97/97 tests passing**.

---

## Added to Phase 11 backlog (requested)

### Artery 3 — Generation anti-fingerprint hardening

Goal: reduce repeated phrasing signatures in deterministic mutation transforms.

Planned work:
- Replace fixed `_sharpen` tail (`"— pick one claim and defend it."`) with a rotating template bank
- Add transform-level cooldown/penalty for recently used phrase endings
- Add repeat-distance guardrails (n-gram and suffix-level) at run and cross-run scope
- Add tests verifying phrase diversity over repeated mutation cycles

Success criteria:
- No single sharpen tail dominates mutation output over rolling N runs
- Mutation lineage remains objective-bounded while phrase surface entropy increases

### Artery 4 — Persona pressure depth expansion

Goal: make persona output distribution broader and less templatic while preserving pressure profile identity.

Planned work:
- Expand `PERSONA_FLAVOR` inventories substantially (target: 50+ surface variants per persona)
- Add persona-conditioned lexical banks (openers, pivots, closers, cadence toggles)
- Add anti-repetition selection policy (recent-output suppression)
- Add tests for persona distinguishability and repetition-rate ceilings

Success criteria:
- Persona outputs remain recognizable by profile while reducing detectable fixed signatures
- Repetition rate across adjacent generations drops below threshold

### Artery 5 — Outcome-trained archetype weighting

Goal: replace static archetype blend constants with data-calibrated weights from stored outcomes.

Planned work:
- Introduce offline calibration routine from run/outcome history
- Learn/update archetype weight vectors by objective/platform/persona segments
- Store and version weight sets with rollback support
- Add confidence gating so sparse segments fall back to default constants

Success criteria:
- Calibrated weights outperform static baseline on bite-rate and audience score lift
- Fallback behavior is deterministic and audit-visible for low-sample regimes

### Artery 6 — Semantic signal layer augmentation

Goal: move beyond lexical marker matching for sarcasm, irony, and polarity inversion handling.

Planned work:
- Add semantic pass after lexical extraction (lightweight classifier or provider-assisted labeler)
- Introduce sarcasm/inversion indicators into signal report and axis computation
- Add contradiction-context sensitivity (quoted text vs asserted text separation)
- Extend tests with adversarial examples (surface certainty + inverse intent)

Success criteria:
- Reduced false-positive certainty/aggression in sarcastic and ironic inputs
- Measurable improvement in downstream archetype assignment stability

---

## Phase 11 execution order

1. Artery 3 (anti-fingerprint hardening)
2. Artery 4 (persona depth expansion)
3. Artery 5 (outcome-trained archetype weights)
4. Artery 6 (semantic signal augmentation)

Rationale: harden output surface first, then improve adaptation logic, then deepen semantic understanding.

## Notes

- Keep mutation objective guardrails (`OBJECTIVE_DELTA_BOUNDS`) in force during all expansions.
- Preserve inspectability: every learned/semantic adjustment must remain observable in reports.
- Do not regress deterministic `--heuristic-only` operator path.
