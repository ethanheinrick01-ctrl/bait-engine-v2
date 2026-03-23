from __future__ import annotations

from bait_engine.core.types import AnalysisResult, DecisionPlan, PersonaRouterDecision
from bait_engine.planning.branches import forecast_branches
from bait_engine.planning.exits import choose_exit_state
from bait_engine.planning.objectives import select_objective
from bait_engine.planning.personas import PersonaProfile, get_persona
from bait_engine.planning.tactics import shortlist_tactics


def build_plan(
    result: AnalysisResult,
    persona: PersonaProfile | str | None = None,
    persona_router: PersonaRouterDecision | None = None,
) -> DecisionPlan:
    persona_profile = get_persona(persona) if isinstance(persona, str) or persona is None else persona
    objective = select_objective(result)
    tactic, alternates, tactic_reasons = shortlist_tactics(result, objective, persona_profile)
    branch_forecast = forecast_branches(result, tactic)
    exit_state = choose_exit_state(result, objective, tactic)

    risk_gates: list[str] = []
    if objective.value == "do_not_engage":
        risk_gates.append("planner inherited analyzer veto")
    if result.opportunity.overplay_risk >= 0.7:
        risk_gates.append("high overplay risk")
    if result.opportunity.human_plausibility_window <= 0.3:
        risk_gates.append("low human plausibility window")

    tone_constraints = list(persona_profile.tone_tags)
    if tactic_reasons:
        tone_constraints.extend(tactic_reasons)

    return DecisionPlan(
        selected_objective=objective,
        selected_tactic=tactic,
        alternates=alternates,
        risk_gates=risk_gates,
        length_band_words=persona_profile.length_band_words,
        tone_constraints=tone_constraints,
        branch_forecast=branch_forecast,
        exit_state=exit_state,
        persona_router=persona_router,
    )
