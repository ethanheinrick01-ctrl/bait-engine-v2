from __future__ import annotations

from bait_engine.core.types import AnalysisResult, BranchClass, BranchForecast, TacticalObjective, TacticFamily, RhetoricalAxis


def _axis(result: AnalysisResult, key: RhetoricalAxis) -> float:
    for item in result.axes:
        if item.axis == key:
            return item.score
    return 0.0


def forecast_branches(result: AnalysisResult, selected_tactic: TacticFamily | None) -> list[BranchForecast]:
    if not selected_tactic:
        return [
            BranchForecast(
                branch=BranchClass.SILENCE,
                probability=1.0,
                follow_up_objective=TacticalObjective.DO_NOT_ENGAGE,
                follow_up_tactic=None,
                disengage=True,
            )
        ]

    aggression = _axis(result, RhetoricalAxis.AGGRESSION)
    curiosity = _axis(result, RhetoricalAxis.CURIOSITY)
    verbosity = _axis(result, RhetoricalAxis.VERBOSITY)
    certainty = _axis(result, RhetoricalAxis.CERTAINTY)

    branches: list[BranchForecast] = []

    if selected_tactic in {TacticFamily.ESSAY_COLLAPSE, TacticFamily.SCHOLAR_HEX, TacticFamily.BURDEN_REVERSAL}:
        branches.extend(
            [
                BranchForecast(branch=BranchClass.DENIAL, probability=0.34 + certainty * 0.12, follow_up_objective=TacticalObjective.MISFRAME, follow_up_tactic=TacticFamily.BURDEN_REVERSAL),
                BranchForecast(branch=BranchClass.ESSAY_DEFENSE, probability=0.24 + verbosity * 0.22, follow_up_objective=TacticalObjective.COLLAPSE, follow_up_tactic=TacticFamily.ESSAY_COLLAPSE),
                BranchForecast(branch=BranchClass.ANGER, probability=0.12 + aggression * 0.22, follow_up_objective=TacticalObjective.EXIT_ON_TOP, follow_up_tactic=TacticFamily.CALM_REDUCTION),
            ]
        )
    elif selected_tactic == TacticFamily.REVERSE_INTERROGATION:
        branches.extend(
            [
                BranchForecast(branch=BranchClass.CLARIFICATION, probability=0.30 + curiosity * 0.18, follow_up_objective=TacticalObjective.INFLATE, follow_up_tactic=TacticFamily.FAKE_CLARIFICATION),
                BranchForecast(branch=BranchClass.QUESTION_SPAM, probability=0.26 + curiosity * 0.2, follow_up_objective=TacticalObjective.BRANCH_SPLIT, follow_up_tactic=TacticFamily.REVERSE_INTERROGATION),
                BranchForecast(branch=BranchClass.DEFLECTION, probability=0.16 + certainty * 0.1, follow_up_objective=TacticalObjective.COLLAPSE, follow_up_tactic=TacticFamily.LABEL_AND_LEAVE),
            ]
        )
    elif selected_tactic == TacticFamily.CALM_REDUCTION:
        branches.extend(
            [
                BranchForecast(branch=BranchClass.ANGER, probability=0.30 + aggression * 0.3, follow_up_objective=TacticalObjective.EXIT_ON_TOP, follow_up_tactic=TacticFamily.LABEL_AND_LEAVE),
                BranchForecast(branch=BranchClass.SARCASM, probability=0.18 + certainty * 0.12, follow_up_objective=TacticalObjective.AUDIENCE_WIN, follow_up_tactic=TacticFamily.LABEL_AND_LEAVE),
                BranchForecast(branch=BranchClass.SILENCE, probability=0.16, follow_up_objective=TacticalObjective.EXIT_ON_TOP, follow_up_tactic=None, disengage=True),
            ]
        )
    else:
        branches.extend(
            [
                BranchForecast(branch=BranchClass.CLARIFICATION, probability=0.24 + curiosity * 0.14, follow_up_objective=TacticalObjective.INFLATE, follow_up_tactic=TacticFamily.BURDEN_REVERSAL),
                BranchForecast(branch=BranchClass.DENIAL, probability=0.22 + certainty * 0.12, follow_up_objective=TacticalObjective.COLLAPSE, follow_up_tactic=TacticFamily.LABEL_AND_LEAVE),
                BranchForecast(branch=BranchClass.SILENCE, probability=0.18, follow_up_objective=TacticalObjective.EXIT_ON_TOP, follow_up_tactic=None, disengage=True),
            ]
        )

    total = sum(max(item.probability, 0.0) for item in branches) or 1.0
    normalized = []
    for item in branches:
        normalized.append(item.model_copy(update={"probability": round(item.probability / total, 4)}))
    normalized.sort(key=lambda item: item.probability, reverse=True)
    return normalized[:3]
