# Analysis Pipeline

The analysis engine converts raw text and thread context into a structured tactical picture.

## Input contract

### Minimum input
- comment text

### Optional input
- parent comment
- thread title
- sibling replies
- author comment history
- platform metadata
- timestamps
- engagement counts

## Pipeline stages

### Stage 0 — Normalize
Produce a common input object:
- strip platform-specific noise
- preserve quotations
- preserve reply nesting
- mark URLs, emojis, and formatting

### Stage 1 — Signal extraction
Extract deterministic features before any model call.

Examples:
- token count
- question density
- insult density
- hedge density
- certainty markers
- jargon markers
- second-person density
- list/explanation structure
- quote/restate behavior
- moral language markers
- absolutist language markers

### Stage 2 — Axis scoring
Map signals into rhetorical axis scores.

Method:
- deterministic heuristic score
- optional model-adjusted score
- calibration merge

Output:
- axis name
- score
- confidence
- reasons

### Stage 3 — Archetype blending
Use the axis profile and salient signals to assign archetype weights.

Rules:
- no forced single-label classification
- preserve top 3 blends
- include confidence and evidence list

### Stage 4 — Contradiction mining
Detect likely structural weaknesses.

Algorithm shape:
1. identify explicit claims
2. infer implied premises
3. check for common contradiction classes
4. assign exploitability
5. produce a recommended label when possible

Output example:
```json
{
  "type": "utility_vs_truth",
  "severity": 0.83,
  "exploitability": 0.91,
  "recommended_label": "instrumentalism"
}
```

### Stage 5 — Phase estimation
Estimate the thread phase from:
- turn count
- average response length
- emotional trend
- whether your frame is active
- whether outside spectators have joined

### Stage 6 — Opportunity scoring
Compute whether the thread is worth touching.

Key rule:
The analyzer must support a verdict of `do_not_engage`.
Selectivity is part of the architecture.

### Stage 7 — Tactical recommendation shell
The analyzer may suggest:
- likely objectives
- likely tactic families
- likely branch outcomes

But it does not choose the final move. That belongs to the decision engine.

## Output contract

The analyzer returns one `AnalysisResult` object.
It must be serializable and stable enough to save to SQLite or JSON.

## Testing doctrine

The analysis layer should be tested against fixed fixtures.
Use archived comments and expected structured outputs.
Success means consistency and interpretability, not poetic elegance.
