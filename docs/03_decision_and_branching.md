# Decision Engine and Branch Prediction

The decision engine converts structured analysis into a constrained plan.
It does not write the final line. It chooses the battlefield shape.

## Inputs
- `AnalysisResult`
- active persona config
- platform policy
- thread history
- memory about the target, if any

## Outputs
A `DecisionPlan` object containing:
- selected objective
- selected tactic family
- allowed alternates
- risk gates
- length band
- tone constraints
- branch forecast
- exit conditions

## Objective selection

Objective selection should be rule-guided first.
Examples:
- high `reply_probability` + low thread depth -> prefer `hook`
- high `verbosity` + high `certainty` -> prefer `inflate` or `collapse`
- high `aggression` -> prefer `calm_reduction` or `exit_on_top`
- high `audience_value` -> prefer portable, screenshotable tactics
- high `overplay_risk` -> prefer `label_and_leave` or `do_not_engage`

## Tactic selection

Map objective + analysis profile to a shortlist.

Examples:
- `utility_vs_truth` contradiction + strong jargon fluency -> `scholar_hex` or `essay_collapse`
- aggressive low-verbosity target -> `calm_reduction`
- question-heavy sealion profile -> `reverse_interrogation`
- partial concession detected -> `concession_magnifier`

## Persona compatibility

A tactic may be valid in abstract but wrong for the current persona.
The planner must reject combinations that break voice.

Examples:
- absurdist tactic with formal persona: penalize
- dense jargon with low-ceiling persona: penalize
- long explanatory move when persona prefers fragments: reject

## Branch prediction

Every chosen tactic should emit top likely opponent reply classes with probabilities.

Example:
```json
[
  {"branch": "denial", "probability": 0.36},
  {"branch": "essay_defense", "probability": 0.31},
  {"branch": "anger", "probability": 0.18}
]
```

## Branch mapping

For each branch, define a next-step recommendation:
- follow-up objective
- likely tactic family
- whether to disengage

Example:
- denial -> burden reversal
- essay defense -> essay collapse
- anger -> calm reduction
- concession -> concession magnifier or exit
- silence -> mark as dormant and stop

## Exit doctrine

The planner must always return an exit recommendation.
Possible states:
- `exit_now`
- `one_more_spike`
- `stall_lightly`
- `abandon`

## Risk gates

Hard planner stops:
- insufficient context
- incoherent target
- repetitive tactic use on same target
- low audience value and low bite probability
- style drift beyond persona tolerance

## Implementation principle

The decision engine should be deterministic wherever possible.
Use a model only for narrow ambiguity resolution, not for the entire plan.
