from __future__ import annotations

from bait_engine.core.types import AnalysisResult, TacticalObjective, TacticFamily, RhetoricalAxis
from bait_engine.planning.personas import PersonaProfile


def _axis(result: AnalysisResult, key: RhetoricalAxis) -> float:
    for item in result.axes:
        if item.axis == key:
            return item.score
    return 0.0


def _persona_penalty(persona: PersonaProfile, tactic: TacticFamily, result: AnalysisResult) -> float:
    penalty = 0.0
    jargon = _axis(result, RhetoricalAxis.JARGON_FLUENCY)
    aggression = _axis(result, RhetoricalAxis.AGGRESSION)

    if tactic in persona.forbidden_tactics:
        return 1.0
    if tactic == TacticFamily.SCHOLAR_HEX and persona.jargon_ceiling < 0.45:
        penalty += 0.7
    if tactic == TacticFamily.ABSURDIST_DERAIL and persona.absurdity_tolerance < 0.45:
        penalty += 0.7
    if tactic == TacticFamily.CALM_REDUCTION and persona.calmness_preference < 0.45:
        penalty += 0.45
    if jargon > persona.jargon_ceiling and tactic == TacticFamily.SCHOLAR_HEX:
        penalty += 0.25
    if aggression > 0.75 and tactic == TacticFamily.FAKE_CLARIFICATION and persona.calmness_preference < 0.6:
        penalty += 0.25
    return min(1.0, penalty)


def shortlist_tactics(result: AnalysisResult, objective: TacticalObjective, persona: PersonaProfile) -> tuple[TacticFamily | None, list[TacticFamily], list[str]]:
    if objective == TacticalObjective.DO_NOT_ENGAGE:
        return None, [], ["selectivity gate vetoed engagement"]

    candidates: list[TacticFamily] = []
    reasons: list[str] = []
    contradictions = {item.type.value for item in result.contradictions}
    aggression = _axis(result, RhetoricalAxis.AGGRESSION)
    curiosity = _axis(result, RhetoricalAxis.CURIOSITY)
    verbosity = _axis(result, RhetoricalAxis.VERBOSITY)
    audience = _axis(result, RhetoricalAxis.AUDIENCE_CONSCIOUSNESS)

    if objective in {TacticalObjective.COLLAPSE, TacticalObjective.AUDIENCE_WIN}:
        candidates += [TacticFamily.ESSAY_COLLAPSE, TacticFamily.LABEL_AND_LEAVE]
    if objective == TacticalObjective.INFLATE:
        candidates += [TacticFamily.BURDEN_REVERSAL, TacticFamily.FAKE_CLARIFICATION]
    if objective == TacticalObjective.TILT:
        candidates += [TacticFamily.CALM_REDUCTION, TacticFamily.LABEL_AND_LEAVE]
    if objective == TacticalObjective.HOOK:
        candidates += [TacticFamily.FAKE_CLARIFICATION, TacticFamily.AGREE_AND_ACCELERATE]
    if objective == TacticalObjective.RESURRECT:
        candidates += [TacticFamily.ABSURDIST_DERAIL, TacticFamily.LABEL_AND_LEAVE]
    if objective == TacticalObjective.EXIT_ON_TOP:
        candidates += [TacticFamily.LABEL_AND_LEAVE, TacticFamily.CALM_REDUCTION]

    if "utility_vs_truth" in contradictions or "mechanism_vs_necessity" in contradictions:
        candidates += [TacticFamily.SCHOLAR_HEX, TacticFamily.BURDEN_REVERSAL]
        reasons.append("contradiction profile favors structural pressure")
    if aggression >= 0.55:
        candidates += [TacticFamily.CALM_REDUCTION]
        reasons.append("high aggression supports calm asymmetry")
    if curiosity >= 0.55:
        candidates += [TacticFamily.REVERSE_INTERROGATION]
        reasons.append("question-heavy posture supports reverse interrogation")
    if verbosity >= 0.62:
        candidates += [TacticFamily.ESSAY_COLLAPSE]
        reasons.append("verbose target supports compression tactics")
    if audience >= 0.55:
        candidates += [TacticFamily.LABEL_AND_LEAVE]
        reasons.append("spectator-visible framing rewards portable lines")

    deduped: list[TacticFamily] = []
    seen: set[TacticFamily] = set()
    for tactic in candidates:
        if tactic not in seen:
            seen.add(tactic)
            deduped.append(tactic)

    scored: list[tuple[float, TacticFamily]] = []
    for tactic in deduped:
        penalty = _persona_penalty(persona, tactic, result)
        if penalty >= 1.0:
            continue
        score = 1.0 - penalty
        if tactic == TacticFamily.CALM_REDUCTION and aggression >= 0.55:
            score += 0.18
        if tactic == TacticFamily.REVERSE_INTERROGATION and curiosity >= 0.55:
            score += 0.18
        if tactic == TacticFamily.ESSAY_COLLAPSE and verbosity >= 0.62:
            score += 0.18
        if tactic == TacticFamily.SCHOLAR_HEX and result.contradictions:
            score += 0.14
        if tactic == TacticFamily.LABEL_AND_LEAVE and objective in {TacticalObjective.AUDIENCE_WIN, TacticalObjective.EXIT_ON_TOP}:
            score += 0.14
        scored.append((score, tactic))

    scored.sort(key=lambda item: item[0], reverse=True)
    ordered = [tactic for _, tactic in scored]
    primary = ordered[0] if ordered else None
    alternates = ordered[1:4]

    return primary, alternates, reasons
