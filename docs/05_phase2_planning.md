# Phase 2 — Planning Layer

Phase 2 converts `AnalysisResult` into a constrained `DecisionPlan`.

## Core doctrine

The planner does not write the final reply.
It chooses:
- objective
- tactic family
- alternates
- branch map
- tone cage
- exit condition

This keeps generation subordinate to strategy.

## Modules

### `planning/personas.py`
Structured persona definitions.
Each persona encodes:
- typical length band
- tone tags
- jargon ceiling
- absurdity tolerance
- calmness preference
- punctuation style
- forbidden tactics

### `planning/objectives.py`
Selects the dominant objective from:
- opportunity scores
- contradictions
- aggression level
- audience value
- phase

### `planning/tactics.py`
Maps objective + analysis profile to tactic family and alternates.
Includes deterministic rules and persona compatibility penalties.

### `planning/branches.py`
Predicts top likely opponent reply classes and maps recommended next-step reactions.

### `planning/exits.py`
Determines whether to:
- exit now
- one more spike
- stall lightly
- abandon

### `planning/planner.py`
Top-level orchestration.
Input: `AnalysisResult` + persona.
Output: `DecisionPlan`.

## Planner principles

1. `do_not_engage` must survive into planning.
2. Persona mismatch should veto tactics.
3. High overplay risk should narrow the tactic set.
4. Every plan must include an exit state.
5. Every plan must include a branch forecast, even if minimal.

## Testing goals

- aggressive targets route toward calm reduction or exit states
- contradiction-rich inputs prefer collapse / burden reversal families
- question-heavy sealion targets prefer reverse interrogation
- low-value inputs remain `do_not_engage`
- persona restrictions actually remove forbidden tactics
