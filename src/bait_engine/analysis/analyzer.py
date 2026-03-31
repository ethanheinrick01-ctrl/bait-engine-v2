from __future__ import annotations

import logging
from dataclasses import dataclass

from bait_engine.analysis.archetypes import blend_archetypes, summarize_weight_profile
from bait_engine.analysis.axes import axis_map, score_axes
from bait_engine.analysis.semantics import infer_semantics
from bait_engine.analysis.contradictions import mine_contradictions
from bait_engine.analysis.opportunity import score_opportunity, should_engage
from bait_engine.analysis.phases import estimate_phase
from bait_engine.analysis.signals import SignalReport, extract_signals
from bait_engine.core.types import AnalysisResult, TacticalObjective, TacticFamily


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class AnalyzeInput:
    text: str
    platform: str = "unknown"
    turn_count: int | None = None
    avg_reply_length: int | None = None
    emotional_intensity: float | None = None
    outside_participants: int | None = None
    frame_adoption: float | None = None
    archetype_weight_profile: dict[str, object] | None = None


def _recommend_objectives(report: SignalReport, contradictions_count: int, engage: bool) -> list[TacticalObjective]:
    if not engage:
        return [TacticalObjective.DO_NOT_ENGAGE]
    objectives: list[TacticalObjective] = [TacticalObjective.HOOK]
    if report.token_count > 45:
        objectives.append(TacticalObjective.INFLATE)
    if contradictions_count:
        objectives.append(TacticalObjective.COLLAPSE)
    if report.insult_density > 0.02:
        objectives.append(TacticalObjective.EXIT_ON_TOP)
    if report.question_count >= 2:
        objectives.append(TacticalObjective.BRANCH_SPLIT)
    return objectives


def _recommend_tactics(report: SignalReport, contradictions_count: int, engage: bool) -> list[TacticFamily]:
    if not engage:
        return []
    tactics: list[TacticFamily] = []
    if contradictions_count:
        tactics.extend([TacticFamily.ESSAY_COLLAPSE, TacticFamily.BURDEN_REVERSAL])
    if report.question_count >= 2:
        tactics.append(TacticFamily.REVERSE_INTERROGATION)
    if report.insult_density > 0.02:
        tactics.append(TacticFamily.CALM_REDUCTION)
    if report.jargon_density > 0.015:
        tactics.append(TacticFamily.SCHOLAR_HEX)
    if report.token_count > 60:
        tactics.append(TacticFamily.LABEL_AND_LEAVE)
    if not tactics:
        tactics.extend([TacticFamily.FAKE_CLARIFICATION, TacticFamily.BURDEN_REVERSAL])
    seen: set[TacticFamily] = set()
    ordered: list[TacticFamily] = []
    for tactic in tactics:
        if tactic not in seen:
            seen.add(tactic)
            ordered.append(tactic)
    return ordered


def analyze_comment(payload: AnalyzeInput) -> AnalysisResult:
    logger.debug("analyze_comment: platform=%s tokens~=%d", payload.platform, len(payload.text.split()))
    signals = extract_signals(payload.text)
    semantic = infer_semantics(payload.text)
    axes = score_axes(signals, semantic=semantic)
    axis_scores = axis_map(axes)
    contradictions = mine_contradictions(signals)
    phase = estimate_phase(
        turn_count=payload.turn_count,
        avg_reply_length=payload.avg_reply_length,
        emotional_intensity=payload.emotional_intensity,
        outside_participants=payload.outside_participants,
        frame_adoption=payload.frame_adoption,
    )
    opportunity = score_opportunity(signals)
    engage = should_engage(opportunity) or (len(contradictions) > 0 and signals.token_count >= 12)
    recommended_objectives = _recommend_objectives(signals, len(contradictions), engage)
    recommended_tactics = _recommend_tactics(signals, len(contradictions), engage)

    archetype_blend = blend_archetypes(axis_scores, weight_profile=payload.archetype_weight_profile)

    vulnerabilities: list[str] = []
    notes: list[str] = []

    if contradictions:
        vulnerabilities.extend(c.recommended_label or c.type.value for c in contradictions)
    if signals.question_count >= 2:
        notes.append("question-heavy posture may support reverse interrogation")
    if signals.token_count < 6:
        notes.append("low-information comment; likely weak yield")
    if semantic.polarity_inversion_probability >= 0.35:
        notes.append(f"semantic_inversion_detected:{semantic.polarity_inversion_probability:.2f}")
    if semantic.quoted_text_ratio > 0:
        notes.append(f"quoted_frame_ratio:{semantic.quoted_text_ratio:.2f}")
    if semantic.reasons:
        notes.append(f"semantic_reasons:{', '.join(semantic.reasons[:4])}")

    if not engage:
        notes.append("selectivity gate recommends skipping this target")
        logger.info("analyze_comment: veto — engage=False engagement_value=%.2f contradictions=%d", opportunity.engagement_value, len(contradictions))

    profile_note = summarize_weight_profile(payload.archetype_weight_profile)
    if profile_note:
        notes.append(profile_note)

    logger.debug(
        "analyze_comment: done engage=%s objectives=%s contradictions=%d opportunity=%.2f",
        engage,
        [o.value for o in recommended_objectives],
        len(contradictions),
        opportunity.engagement_value,
    )
    return AnalysisResult(
        source_text=payload.text,
        platform=payload.platform,
        axes=axes,
        archetype_blend=archetype_blend,
        contradictions=contradictions,
        phase=phase,
        opportunity=opportunity,
        vulnerabilities=vulnerabilities,
        recommended_objectives=recommended_objectives,
        recommended_tactics=recommended_tactics,
        notes=notes,
    )
