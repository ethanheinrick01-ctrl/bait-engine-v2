# Bait Engine v2

Cognitive core for a rhetoric analysis and response generation engine.

## Project structure

- `src/bait_engine/` — main package
  - `analysis/` — signal detection, archetypes, axes, contradictions, opportunity scoring
  - `planning/` — tactical planning, persona routing, objectives, exits, branches
  - `generation/` — candidate reply generation, mutation, ranking, critic, LLM writer
  - `adapters/` — platform adapters (reddit, x, discord, web), presets, emit/dispatch
  - `intake/` — hunt intake, target discovery, scoring
  - `storage/` — SQLite persistence, models, schema, repository, autopsy
  - `providers/` — LLM provider abstraction (OpenAI-compatible)
  - `core/types.py` — shared domain types (Pydantic models)
  - `cli/main.py` — CLI entry point
- `tests/` — test suite (145 tests)
- `docs/` — phase documentation and build order

## Tech stack

- Python 3.11+
- Pydantic v2 for all domain models
- SQLite for storage
- setuptools for packaging
- `uv` for dependency management

## Key commands

```bash
# Install
pip install -e .

# Run tests
python -m pytest tests/

# CLI entry point
python -m bait_engine.cli.main <command>
# or after install:
bait-engine <command>
```

## Architecture principles

1. Lock schemas before writing clever code
2. Separate analysis from generation
3. Make every decision inspectable
4. Prefer deterministic scoring around the model, not model-only magic
5. Keep work chunked into small, durable files so session loss does not rot the project

## Current phase

All phases (0–13) complete. The engine has: analysis pipeline, decision engine, generation shell, persistence, provider-backed generation, platform adapters, dispatch, rhetoric scoring, hunt intake, mutation loop, optimization rail, auto persona router, and autopilot hardening.
