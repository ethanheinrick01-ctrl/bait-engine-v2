# Phase 11 — Artery 6 Spec: Semantic Signal Augmentation

## Objective
Add a semantic pass on top of current lexical signal extraction so sarcasm/irony/polarity-inversion cases stop poisoning axis/archetype scoring.

## Why
Current `analysis/signals.py` is lexical-marker heavy. It over-trusts surface certainty/aggression markers and under-detects inverted intent (e.g., sarcastic praise).

## Scope (in)
1. Add semantic annotations to the analysis pipeline (post-lexical, pre-axes/archetypes).
2. Detect likely inversion/sarcasm and reduce confidence of literal lexical cues.
3. Preserve inspectability in `AnalysisResult.notes` and reason traces.
4. Keep deterministic behavior in heuristic mode.

## Scope (out)
- No full LLM dependency for core analyzer path.
- No breaking schema changes to persisted run rows.
- No mutation/generation changes in this artery.

## Design

### 1) New semantic layer module
Add module: `src/bait_engine/analysis/semantics.py`

Proposed dataclass/model:
- `SemanticReport`
  - `sarcasm_probability: float [0..1]`
  - `irony_probability: float [0..1]`
  - `polarity_inversion_probability: float [0..1]`
  - `quoted_text_ratio: float [0..1]`
  - `literal_confidence: float [0..1]`
  - `reasons: list[str]`

Entry point:
- `infer_semantics(text: str) -> SemanticReport`

Implementation strategy (deterministic first pass):
- Heuristic cue fusion from punctuation patterns, discourse markers, hedge/reversal phrases, quote framing, and emoji/text mismatch.
- Weighted bounded score with explicit reasons.

### 2) Analyzer integration
In `analysis/analyzer.py`:
- Run `semantic = infer_semantics(payload.text)` after `extract_signals`.
- Add semantic-aware adjustment step before axes:
  - If `polarity_inversion_probability` high, down-weight literal aggression/certainty cues.
  - If heavy quoted-text framing, reduce confidence of direct-assertion cues.
- Keep original lexical values available; apply adjustments as confidence scaling, not destructive overwrite.

### 3) Axis/archetype interaction
- Keep existing formulas intact.
- Feed adjusted/confidence-scaled signal map into `score_axes` and `blend_archetypes`.
- Add reason strings to axes when semantic attenuation is applied.

### 4) Inspectability
- Append semantic summary to `AnalysisResult.notes`, e.g.:
  - `"semantic_inversion_detected:0.72"`
  - `"quoted_frame_ratio:0.41"`
- Ensure output remains deterministic for same input.

## Acceptance tests (explicit)

Add tests in `tests/test_analysis.py` (or `tests/test_analysis_semantic.py`).

### A. Pass/fail: sarcasm attenuation
**Input:**
`"oh brilliant take champ, totally foolproof lol"`

**Pass criteria:**
- `semantic_inversion_detected` note present (or equivalent reason marker)
- certainty/aggression axis confidence is reduced versus literal baseline text
- analyzer still returns bounded scores [0,1]

**Fail if:**
- no semantic flag appears
- confidence unchanged from literal baseline despite strong sarcasm cues

### B. Pass/fail: quoted-text separation
**Input:**
`"You said \"I never do that\" and then did exactly that."`

**Pass criteria:**
- non-zero `quoted_text_ratio`
- contradiction signal remains, but certainty inflation from quoted content is damped

**Fail if:**
- quoted framing has no measurable effect

### C. Pass/fail: literal control stability
**Input:**
`"Your argument is inconsistent because point A contradicts point B."`

**Pass criteria:**
- semantic inversion stays low
- lexical behavior remains near current baseline

**Fail if:**
- semantic layer suppresses valid literal contradiction handling

### D. Pass/fail: deterministic repeatability
Run analyzer N=10 on identical text.

**Pass criteria:**
- semantic probabilities and downstream axis/archetype outputs are identical each run

**Fail if:**
- outputs drift across identical runs

### E. Pass/fail: boundedness + schema safety
For adversarial fixtures, validate all semantic and downstream scores stay in [0,1].

**Pass criteria:**
- no schema errors
- all score fields bounded

**Fail if:**
- any out-of-range score or model validation error

## Edge-case matrix
1. **Sarcastic praise with softener**: "great job genius lol"
2. **Ironic concession**: "sure, because that always works"
3. **Quoted accusation rebuttal**: "you called it 'objective' lol"
4. **Emoji inversion**: positive words + mocking emoji patterns
5. **Question-stack irony**: rhetorical questions that look literal
6. **Short low-information jab**: ensure no overfitting from minimal text

## Delivery checklist
- [ ] `analysis/semantics.py` added
- [ ] analyzer integration complete
- [ ] notes/reasons expose semantic adjustments
- [ ] tests added for A–E above
- [ ] full suite green

## Definition of done
Artery 6 is done when semantic inversion cases materially reduce false literal certainty/aggression scoring, without regressing literal-case behavior or determinism.