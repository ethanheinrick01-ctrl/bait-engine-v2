# Operator Runbook — Phase 13 Artery 7

This runbook is the deterministic operating guide for dispatch autopilot.

## 1) Startup / Shutdown Procedure

### Startup
1. Verify status + preflight:
   - `python -m bait_engine.cli.main operator-status --db .data/bait-engine.db`
   - `python -m bait_engine.cli.main preflight --db .data/bait-engine.db`
2. If preflight fails, resolve items before enabling worker loop.
3. Start worker:
   - One-shot cycle: `python -m bait_engine.cli.main worker-cycle --db .data/bait-engine.db`
   - Continuous loop: `python -m bait_engine.cli.main worker-run --db .data/bait-engine.db --interval-seconds 30`

### Shutdown
1. Stop worker process / service.
2. Snapshot state checkpoint:
   - `python - <<'PY'
from bait_engine.storage import RunRepository
repo = RunRepository('.data/bait-engine.db')
print(repo.create_dispatch_control_checkpoint(reason='operator_shutdown'))
PY`
3. Confirm no in-flight unexpected retries:
   - `python -m bait_engine.cli.main dispatch-status --db .data/bait-engine.db`

## 2) Daily Checks

Run at least once daily:
- `operator-status`
- `preflight`
- `report-markdown --since-hours 24`

Review:
- governor allow/deny and reason
- open breakers
- retry queue (due/waiting/dead-letter)
- safety pause/safe-mode settings
- observability alert totals + critical severity
- active checkpoint id and last_good checkpoint

## 3) Incident Response Steps

1. **Stabilize blast radius**
   - enable global pause:
     - `repo.set_dispatch_control_state(safety={'global_pause': True, 'override_source': 'manual'})`
2. **Inspect state**
   - `operator-status`
   - `dispatch-status`
   - `report-markdown --since-hours 6`
3. **Checkpoint current incident state**
   - `repo.create_dispatch_control_checkpoint(reason='incident_<short_name>')`
4. **Containment actions**
   - keep breaker enabled for failing lane
   - redrive only when retry window is open and root cause is understood
5. **Resume cautiously**
   - disable global pause only after `preflight` passes.

## 4) Rollback Procedure (Artery 6 APIs)

### Roll back to explicit checkpoint
```python
from bait_engine.storage import RunRepository
repo = RunRepository('.data/bait-engine.db')
result = repo.rollback_to_checkpoint(<checkpoint_id>)
print(result)
```

### Roll back to last known good state
```python
from bait_engine.storage import RunRepository
repo = RunRepository('.data/bait-engine.db')
result = repo.rollback_to_last_good_state()
print(result)
```

Expected result schema:
- `applied: bool`
- `failed: bool`
- `reason: str | None`
- `checkpoint_id: int | None`
- `state: {...}` when applied

## 5) Safe-Mode / Pause Controls

Use control state updates:
```python
repo.set_dispatch_control_state(
    safety={
        'global_pause': True,
        'paused_platforms': ['reddit'],
        'safe_mode': True,
        'safe_mode_allowed_drivers': ['manual_copy', 'jsonl_append'],
        'override_source': 'manual',
    },
    source='operator',
)
```

Recommended sequence:
1. `global_pause=True` for immediate freeze.
2. Narrow to per-platform pause if needed.
3. Use safe-mode allowlist for controlled recovery.
4. Run `preflight` before re-enabling autopilot.

## 6) Launch Checklist (Go/No-Go)

A launch is **GO** only when `preflight.overall_pass == true` and all items pass:
- governor allows execution
- no global pause
- no open breakers
- dead-letter count within threshold
- retry backlog within threshold
- critical alerts within threshold
- last_good checkpoint exists
