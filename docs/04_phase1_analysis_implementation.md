# Phase 1 — Analysis Implementation

This phase implements the deterministic analysis layer before any model-assisted writing.

## Modules

### `analysis/signals.py`
Extracts low-level textual features:
- token counts
- punctuation ratios
- question density
- insult density
- hedge density
- certainty markers
- moralizing markers
- jargon markers
- absolutist markers
- concession markers
- all-caps intensity
- quote/restate hints

### `analysis/axes.py`
Maps extracted signals into bounded rhetorical axis scores.
Each axis score includes:
- score
- confidence
- reasons

### `analysis/archetypes.py`
Converts axis profile into weighted archetype blends.
Deterministic rules first, with normalized outputs.

### `analysis/contradictions.py`
Performs lightweight structural mining for common exploit classes:
- utility vs truth
- mechanism vs necessity
- description vs normativity
- confidence/evidence mismatch
- definition evasion
- scope shift

### `analysis/phases.py`
Estimates thread phase from turn-level metadata when available.
Defaults sanely when only a single comment is present.

### `analysis/analyzer.py`
Top-level orchestrator.
Input: raw text + optional context.
Output: `AnalysisResult`.

## Fixtures

The fixture set should include:
- spectral/jargon type
- confident idiot
- aggressive poster
- sealion
- overextender
- moralizer
- naive literalist
- low-value dead comment

## Testing goals

1. Stable output shape
2. Score ranges remain bounded
3. Contradiction detector fires on obvious cases
4. Archetype blender produces plausible top labels
5. `do_not_engage` recommendation appears for sludge/low-yield input

## Non-goals for Phase 1

- final reply generation
- live Reddit integration
- SQLite persistence
- autonomous posting

This phase is about turning text into a tactical map.