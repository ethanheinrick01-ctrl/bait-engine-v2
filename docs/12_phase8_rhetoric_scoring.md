# Phase 8: Rhetoric Scoring & Persona Reputation

Phase 8 moves the engine from "honest delivery" to "high-performance rhetoric." We implement a feedback loop where the success of historical dispatches directly weights future candidate selection.

## Objectives
- **Outcome Tracking:** Record real-world results of dispatches (replies, engagement, tone shifts).
- **Persona Reputation:** Aggregated success metrics per (persona, platform, tactic).
- **Autonomous Selection (`auto_best`):** A selection strategy that favors high-performing tactics based on reputation data.
- **Cockpit Scoring UI:** Enable operators to quickly "Autopsy" a run and log the result.

## Key Components
1. **`OutcomeRecord` Integration:** Tie `OutcomeRecord` more tightly to the delivery history.
2. **Reputation Engine:** `RunRepository.get_persona_reputation(persona, platform)`.
3. **Selection Refinement:** Update `Adapter` logic to optionally query reputation during selection.
