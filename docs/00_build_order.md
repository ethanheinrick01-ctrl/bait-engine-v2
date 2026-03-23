# Build Order — Purity Protocol

This project dies if we let implementation outrun structure.

## Rule of build order

We will build the system in layers that preserve context and prevent rework.

### Layer 1 — Canonical schemas
Lock these first:
- rhetorical axes
- archetype blend schema
- contradiction taxonomy
- thread phase model
- tactical objectives
- tactic families
- branch outcome classes
- scoring record shapes

Deliverables:
- `docs/01_core_domain_model.md`
- `src/bait_engine/core/types.py`

### Layer 2 — Analysis engine
Input: comment + optional thread/user context.
Output: structured analysis object, not prose.

Submodules:
- signal extraction
- axis scoring
- archetype blending
- contradiction mining
- thread phase estimation
- opportunity scoring

Deliverables:
- `docs/02_analysis_pipeline.md`
- analyzer interfaces + test fixtures

### Layer 3 — Decision engine
Input: analysis object.
Output: objective + tactic family + generation constraints.

Submodules:
- tactic rules
- objective selector
- risk gates
- persona compatibility checks
- branch forecast

Deliverables:
- `docs/03_decision_and_branching.md`
- planner interfaces + fixtures

### Layer 4 — Generation shell
Only after the above is stable.
The model should write inside a cage built by the prior layers.

### Layer 5 — Persistence + CLI
Only once the cognitive core shape is stable.
Then add SQLite, event logs, CLI, and replay tools.

### Layer 6 — Adapters + UI
Only after storage/reporting are stable.
The core should hand surfaces a neutral envelope, not platform-specific sludge.

Deliverables:
- `docs/09_phase6_adapters_ui.md`
- adapter contracts + compiler
- dry-run adapter preview tooling

## Session-resilience doctrine

Each major concept gets its own file.
No giant monolith spec.
Every future session should be able to resume from a single document without rehydrating the entire project.

## Immediate next build targets

1. Finish canonical types
2. Write analysis contracts
3. Add test fixtures for 10 comment archetypes
4. Implement deterministic decision rules before any LLM generation
