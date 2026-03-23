# Phase 4 — Persistence and Replay

Phase 4 gives the engine memory that survives the session and supports autopsy.

## Doctrine

The system should remember enough to answer:
- what target was analyzed
- what plan was chosen
- what candidates were drafted
- which branch forecast existed
- what happened after posting

Persistence should be local, queryable, and boring.
That means SQLite.

## Modules

### `storage/schema.py`
Creates and migrates the SQLite schema.

### `storage/models.py`
Defines DB-facing row payloads and convenience serialization helpers.

### `storage/db.py`
Connection helpers, transaction wrapper, and generic query methods.

### `storage/repository.py`
High-level methods:
- save analysis
- save plan
- save draft
- list recent runs
- fetch run detail
- record outcome

### `storage/autopsy.py`
Given a stored run, produce a compact summary of:
- target profile
- selected objective/tactic
- top candidate
- branch expectations
- outcome if present

## Schema shape

### `runs`
One row per analyze/plan/draft cycle.
Stores:
- timestamp
- source text
- platform
- persona
- selected objective
- selected tactic
- exit state
- serialized analysis JSON
- serialized plan JSON

### `candidates`
One row per generated candidate.
Stores:
- run id
- rank index
- text
- tactic
- objective
- bite score
- audience score
- critic penalty
- rank score
- critic notes JSON

### `outcomes`
Optional follow-up data.
Stores:
- run id
- got_reply
- reply_delay_seconds
- reply_length
- tone_shift
- spectator_engagement
- result_label
- notes

## CLI additions

Phase 4 should extend the CLI with:
- `draft --save`
- `runs`
- `show-run <id>`
- `autopsy <id>`
- `record-outcome <id> ...`

## Quality gates

- storage must work with standard library only
- JSON fields must be recoverable into typed objects
- a stored run must be fully inspectable without external services
