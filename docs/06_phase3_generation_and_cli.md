# Phase 3 — Generation Shell and CLI

Phase 3 adds the controlled writing shell around the analysis + planning core.

## Doctrine

Generation is not a free-for-all.
It must consume a `DecisionPlan` and write inside the cage.

The shell has four jobs:
1. create candidate replies from structured constraints
2. critique them for botness, overplay, and persona drift
3. rank them by tactical usefulness
4. expose the whole flow through a replayable CLI

## Modules

### `generation/contracts.py`
Canonical request/response objects for generation.

### `generation/prompts.py`
Builds compact prompt payloads for future model use.
Even if the first writer is heuristic, the prompt contract must be stable.

### `generation/writer.py`
Produces candidate replies from a `DecisionPlan`.
For now this can be template/heuristic driven with persona-conditioned phrasing.
Later it can wrap GPT-5.4 or another model.

### `generation/critic.py`
Scores each candidate for:
- bot symmetry
- over-explanation
- excessive polish
- persona mismatch
- length drift
- tactical drift

### `generation/ranker.py`
Combines tactical score, critic score, and readability into a sorted candidate list.

### `cli/main.py`
Replayable commands:
- `analyze`
- `plan`
- `draft`

All commands should accept raw text input and emit JSON-friendly output.

## Non-goals

- live posting
- Reddit API integration
- persistent storage

Those belong to later phases.

## Quality gates

- every draft must be traceable back to objective + tactic
- every candidate must have critic notes
- every CLI command must work with no external services
- generation should degrade gracefully if no LLM is wired in
