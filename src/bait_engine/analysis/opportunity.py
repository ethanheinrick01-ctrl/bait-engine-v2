from __future__ import annotations

from bait_engine.analysis.common import clamp_unit as _clamp
from bait_engine.analysis.signals import SignalReport
from bait_engine.core.types import OpportunityScores


def score_opportunity(report: SignalReport) -> OpportunityScores:
    token_factor = min(report.token_count / 80, 1.0)
    directness = min(report.second_person_count / max(report.token_count, 1) * 4, 1.0)
    engagement_value = _clamp(
        token_factor * 0.42
        + report.question_density * 0.18
        + directness * 0.18
        + report.certainty_density * 3.2
        + report.jargon_density * 8
    )
    reply_probability = _clamp(
        report.question_density * 0.24
        + directness * 0.26
        + report.insult_density * 5
        + report.certainty_density * 1.8
        + report.jargon_density * 6
        + token_factor * 0.15
    )
    essay_probability = _clamp(
        token_factor * 0.45
        + report.question_count * 0.08
        + report.quote_hint_count * 0.12
        + report.certainty_density * 1.5
        + report.jargon_density * 5
    )
    audience_value = _clamp(
        directness * 0.34
        + min(report.line_count / 5, 1.0) * 0.2
        + report.quote_hint_count * 0.15
        + report.insult_density * 2
        + report.jargon_density * 4
    )
    human_plausibility_window = _clamp(0.85 - report.all_caps_count * 0.08 - report.exclamation_count * 0.04)
    risk_of_boringness = _clamp(0.75 - engagement_value + (0.15 if report.token_count < 6 else 0.0))
    overplay_risk = _clamp(report.insult_density * 4 + report.exclamation_count * 0.06 + report.all_caps_count * 0.08)

    return OpportunityScores(
        engagement_value=engagement_value,
        reply_probability=reply_probability,
        essay_probability=essay_probability,
        audience_value=audience_value,
        human_plausibility_window=human_plausibility_window,
        risk_of_boringness=risk_of_boringness,
        overplay_risk=overplay_risk,
    )


def should_engage(opportunity: OpportunityScores) -> bool:
    return (
        opportunity.engagement_value >= 0.3
        and opportunity.reply_probability >= 0.25
        and opportunity.risk_of_boringness <= 0.72
        and opportunity.overplay_risk <= 0.82
    )
