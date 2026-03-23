# Core Domain Model

This is the canonical vocabulary of the engine.
Everything downstream must conform to these shapes.

## 1. Rhetorical axes

All target analysis begins with bounded scores in `[0.0, 1.0]`.

### Primary axes
- `ego_fragility`
- `verbosity`
- `certainty`
- `aggression`
- `curiosity`
- `self_awareness`
- `audience_consciousness`
- `jargon_fluency`
- `contradiction_susceptibility`
- `moralizing_tendency`
- `bait_hunger`
- `reply_stamina`

### Optional derived axes
- `humor_receptivity`
- `status_defensiveness`
- `pedantry`
- `abstraction_level`
- `irony_detection`

## 2. Archetype blend

Archetypes are not exclusive labels. They are a probability-like blend.

### Initial archetypes
- `spectral`
- `confident_idiot`
- `aggressive_poster`
- `conspiracy_counterparty`
- `sealion`
- `naive_literalist`
- `overextender`

Example:
```json
{
  "spectral": 0.71,
  "sealion": 0.58,
  "overextender": 0.41
}
```

## 3. Contradiction taxonomy

Contradictions are structural exploit classes, not just "gotchas."

### Core contradiction types
- `mechanism_vs_necessity`
- `utility_vs_truth`
- `description_vs_normativity`
- `correlation_vs_causation`
- `scope_shift`
- `hidden_premise_dependency`
- `definition_evasion`
- `confidence_evidence_mismatch`
- `equivocation`
- `frame_drift`

Each contradiction record has:
- `type`
- `severity`
- `exploitability`
- `evidence_spans`
- `recommended_label`

## 4. Thread phases

Every live thread should be assigned one dominant phase.

- `hook`
- `inflation`
- `exposure`
- `combustion`
- `closure`
- `dead`

## 5. Tactical objectives

These are intent labels for move selection.

- `hook`
- `inflate`
- `tilt`
- `misframe`
- `collapse`
- `audience_win`
- `stall`
- `exit_on_top`
- `resurrect`
- `branch_split`

## 6. Tactic families

These are the major schools of response generation.

- `essay_collapse`
- `burden_reversal`
- `agree_and_accelerate`
- `calm_reduction`
- `fake_clarification`
- `absurdist_derail`
- `scholar_hex`
- `label_and_leave`
- `reverse_interrogation`
- `concession_magnifier`

## 7. Branch outcome classes

Predicted next-turn opponent responses.

- `denial`
- `clarification`
- `anger`
- `sarcasm`
- `concession`
- `deflection`
- `question_spam`
- `essay_defense`
- `silence`

## 8. Opportunity scoring

Each candidate target should receive these scores:
- `engagement_value`
- `reply_probability`
- `essay_probability`
- `audience_value`
- `human_plausibility_window`
- `risk_of_boringness`
- `overplay_risk`

## 9. Analysis object

The analyzer should emit one durable object containing:
- normalized input metadata
- rhetorical axes
- archetype blend
- contradiction records
- phase estimate
- opportunity scores
- vulnerabilities
- recommended objectives
- recommended tactic shortlist
- confidence summary

## 10. Design constraint

Generation is downstream.
The engine must be able to stop after analysis and still provide value.
That is how we keep the system inspectable, debuggable, and resistant to prompt drift.
