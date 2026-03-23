# Phase 5 — Provider-Backed Generation

Phase 5 upgrades generation from heuristic-only drafting to a provider-backed synthesis layer.

## Doctrine

The model does not replace the architecture.
It writes inside a deterministic cage built by:
- analysis
- planning
- persona constraints
- critic/ranker

## Goals

1. Add a provider abstraction for text generation.
2. Support GPT-backed generation when credentials are available.
3. Preserve a clean heuristic fallback when they are not.
4. Keep model output inspectable and critic-scored.

## Modules

### `providers/base.py`
Abstract provider interface.

### `providers/openai_compatible.py`
OpenAI-compatible chat completion adapter.
Uses:
- `OPENAI_API_KEY`
- `OPENAI_BASE_URL` optional
- `BAIT_ENGINE_MODEL` optional

This allows standard OpenAI or compatible endpoints.

### `generation/llm_writer.py`
Takes a `DraftRequest`, builds a prompt payload, calls the provider, and parses short candidate lines.

### `generation/provider_pipeline.py`
Orchestrates:
- try provider-backed synthesis
- if provider unavailable/fails, fall back to heuristic writer
- critique + rank either way

## Output guarantees

Whether candidates came from the model or heuristics, downstream shape stays identical.
That is critical.

## Parsing rules

Provider output should be requested as newline-separated candidates.
The parser should:
- trim bullets/numbers
- reject empty lines
- deduplicate near-identical lines
- respect candidate count

## Safety / style constraints

The provider prompt must explicitly require:
- short outputs
- human plausibility
- no AI tone
- no explanatory essays
- no policy chatter

## Testing goals

- provider-disabled path falls back cleanly
- parser extracts candidates from numbered output
- ranked results still return valid `DraftResult`
