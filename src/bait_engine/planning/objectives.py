from __future__ import annotations

from bait_engine.core.types import AnalysisResult, TacticalObjective, ThreadPhase


def select_objective(result: AnalysisResult) -> TacticalObjective:
    if TacticalObjective.DO_NOT_ENGAGE in result.recommended_objectives:
        return TacticalObjective.DO_NOT_ENGAGE

    opp = result.opportunity
    phase = result.phase
    has_contradictions = bool(result.contradictions)

    if opp.overplay_risk >= 0.72:
        return TacticalObjective.EXIT_ON_TOP
    if phase == ThreadPhase.CLOSURE:
        return TacticalObjective.EXIT_ON_TOP
    if phase == ThreadPhase.COMBUSTION:
        return TacticalObjective.TILT if opp.audience_value >= 0.5 else TacticalObjective.EXIT_ON_TOP
    if has_contradictions and opp.essay_probability >= 0.45:
        return TacticalObjective.COLLAPSE
    if opp.audience_value >= 0.62:
        return TacticalObjective.AUDIENCE_WIN
    if opp.reply_probability >= 0.55 and opp.essay_probability >= 0.5:
        return TacticalObjective.INFLATE
    if opp.reply_probability >= 0.35:
        return TacticalObjective.HOOK
    return TacticalObjective.RESURRECT
