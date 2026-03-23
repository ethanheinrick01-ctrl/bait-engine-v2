from __future__ import annotations

from bait_engine.core.types import AnalysisResult, TacticalObjective, TacticFamily


def choose_exit_state(result: AnalysisResult, objective: TacticalObjective, tactic: TacticFamily | None) -> str:
    opp = result.opportunity

    if objective == TacticalObjective.DO_NOT_ENGAGE:
        return "abandon"
    if opp.overplay_risk >= 0.75:
        return "exit_now"
    if objective == TacticalObjective.EXIT_ON_TOP:
        return "exit_now"
    if objective == TacticalObjective.COLLAPSE and opp.audience_value >= 0.45:
        return "one_more_spike"
    if objective == TacticalObjective.INFLATE and opp.reply_probability >= 0.55:
        return "stall_lightly"
    if tactic == TacticFamily.CALM_REDUCTION and opp.reply_probability >= 0.4:
        return "one_more_spike"
    return "stall_lightly" if opp.reply_probability >= 0.35 else "abandon"
