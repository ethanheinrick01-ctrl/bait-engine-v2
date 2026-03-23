# Phase 9 Completion — Hunt Intake Rail Sealed

Phase 9 closes the honest gap left after execution and rhetoric scoring:

> the engine could draft, stage, approve, and dispatch bait,
> but it still could not **find fresh targets on its own**.

This phase adds a real intake rail instead of faking autonomy.

**Phase 9 is complete.**

## What Phase 9 delivered

### 1. Source-backed target discovery

A new `bait_engine.intake` package now exposes source drivers for:

- `jsonl_file`
- `reddit_listing`
- `reddit_search`
- `x_search_recent`

Each source returns normalized hunt targets with:

- `source_driver`
- `source_item_id`
- `platform`
- `thread_id`
- `reply_to_id`
- `author_handle`
- `subject`
- `body`
- context + metadata payloads suitable for later reply compilation

### 2. Inspectable hunt scoring

Fetched targets are not auto-fired blindly.

Each candidate is run through the existing analysis core and scored using a transparent blend of:

- reply probability
- engagement value
- essay probability
- audience value
- contradiction signal
- bait-hunger / certainty cues
- public engagement metadata (score, comments, likes, replies)
- boringness / overplay penalties

The output is stored as explicit score + analysis JSON, not hidden in model vapor.

### 3. Persistent intake target storage

A new `intake_targets` table now tracks discovered targets across runs:

- unique source identity (`source_driver`, `source_item_id`)
- status (`new`, `promoted`, `staged`, `approved`, etc.)
- stored scoring and full analysis
- normalized context
- linked promoted run id
- linked emit outbox id

This means the hunter loop is now durable and auditable instead of session-local.

### 4. Promotion rail from target → run → emit

Stored targets can now be promoted directly into the existing generation and execution pipeline.

The repo now supports:

- saving discovered targets
- listing them by status/platform
- promoting one target into a full run
- optionally staging an emit
- optionally approving it for worker dispatch

If a promotion produces no candidates, the system does **not** lie and pretend it staged something. It leaves the target promoted but unstaged.

### 5. New CLI hunt surface

The CLI now exposes:

- `hunt-preview`
- `hunt-list`
- `hunt-promote`
- `hunt-cycle`
- `hunt-run`

These commands let the engine:

- preview ranked targets without writing
- persist discovered targets
- promote selected targets into bait runs
- stage / approve emits
- optionally dispatch approved emits in the same cycle
- loop continuously under cron or any external scheduler

## Operator smoke paths

### Inspect targets without writing

```bash
PYTHONPATH=src python3 -m bait_engine.cli.main hunt-preview reddit_listing \
  --subreddit AmItheAsshole \
  --sort new \
  --limit 25
```

### Persist + stage top candidates

```bash
PYTHONPATH=src python3 -m bait_engine.cli.main hunt-cycle reddit_search \
  --query "site:reddit.com \"objectively false\"" \
  --promote-limit 3 \
  --stage-emit
```

### Fully autonomous pass

```bash
PYTHONPATH=src python3 -m bait_engine.cli.main hunt-cycle reddit_listing \
  --subreddit unpopularopinion \
  --sort new \
  --promote-limit 2 \
  --approve-emit \
  --dispatch-approved \
  --driver reddit_api
```

### Cron-friendly loop

```bash
*/10 * * * * cd /Users/ethanheinrick/.openclaw/workspace/bait-engine-v2 && PYTHONPATH=src /usr/bin/python3 -m bait_engine.cli.main hunt-cycle reddit_listing --subreddit unpopularopinion --sort new --limit 25 --promote-limit 2 --approve-emit --dispatch-approved --driver reddit_api >> ~/Library/Logs/bait-engine-hunt.log 2>&1
```

## Tests

Phase 9 adds coverage for:

- JSONL hunt preview ranking
- hunt-cycle promotion + staging
- hunt-cycle dispatch of approved emits
- idempotent target promotion
- Reddit listing payload parsing

At closure, the full suite passes:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -q
```

Result at closure: **77 tests passing**.

## What is intentionally not part of Phase 9

Still not pretending to exist:

- hidden autonomous target selection outside the stored intake rail
- stealth scraping beyond explicit source drivers
- magical long-lived scheduler inside the app itself
- fabricated “AI discovered someone somehow” behavior without source records

Cron/launchd/worker loops can now drive the system, but the intake path remains explicit and inspectable.

## Net result

Phase 9 turns Bait Engine from:

- *"I can fire approved bait"*

into:

- *"I can discover, score, store, promote, and optionally dispatch bait in a truthful autonomous loop."*
