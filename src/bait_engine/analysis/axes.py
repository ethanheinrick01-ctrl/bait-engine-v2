from __future__ import annotations

from bait_engine.analysis.common import clamp_unit as _clamp
from bait_engine.analysis.semantics import SemanticReport
from bait_engine.analysis.signals import SignalReport
from bait_engine.core.types import AxisScore, RhetoricalAxis


def _axis(axis: RhetoricalAxis, score: float, confidence: float, reasons: list[str]) -> AxisScore:
    return AxisScore(axis=axis, score=_clamp(score), confidence=_clamp(confidence), reasons=reasons)


def _semantic_scale_for_axis(axis: RhetoricalAxis, semantic: SemanticReport | None) -> tuple[float, float, list[str]]:
    if semantic is None:
        return 1.0, 1.0, []

    inversion = _clamp(semantic.polarity_inversion_probability)
    quote_ratio = _clamp(semantic.quoted_text_ratio)
    literal_conf = _clamp(semantic.literal_confidence)

    score_scale = 1.0
    confidence_scale = max(0.2, 0.45 + 0.55 * literal_conf)
    reasons: list[str] = []

    if axis in {RhetoricalAxis.CERTAINTY, RhetoricalAxis.AGGRESSION}:
        score_scale *= max(0.35, 1.0 - (0.5 * inversion) - (0.25 * quote_ratio))
        if inversion >= 0.35:
            reasons.append(f"semantic inversion attenuation={round(inversion, 3)}")

    if axis == RhetoricalAxis.CONTRADICTION_SUSCEPTIBILITY:
        score_scale *= max(0.4, 1.0 - (0.2 * quote_ratio))
        if quote_ratio > 0.1:
            reasons.append(f"quoted-frame attenuation={round(quote_ratio, 3)}")

    if axis == RhetoricalAxis.BAIT_HUNGER and inversion >= 0.4:
        score_scale *= max(0.45, 1.0 - (0.35 * inversion))
        reasons.append(f"bait cue softened by semantic inversion={round(inversion, 3)}")

    if quote_ratio >= 0.2 and axis in {RhetoricalAxis.CERTAINTY, RhetoricalAxis.AGGRESSION, RhetoricalAxis.BAIT_HUNGER}:
        confidence_scale *= max(0.45, 1.0 - (0.45 * quote_ratio))
        reasons.append(f"quoted text ratio={round(quote_ratio, 3)}")

    return _clamp(score_scale), _clamp(confidence_scale), reasons


def score_axes(report: SignalReport, semantic: SemanticReport | None = None) -> list[AxisScore]:
    tc = max(report.token_count, 1)
    verbosity = _clamp((report.token_count - 8) / 52)
    aggression = _clamp((sum(report.insult_hits.values()) * 0.45) + (report.exclamation_count * 0.08) + (report.all_caps_count * 0.12))
    certainty = _clamp((sum(report.certainty_hits.values()) * 0.22) + (sum(report.absolutist_hits.values()) * 0.14) - (sum(report.hedge_hits.values()) * 0.12))
    curiosity = _clamp((report.question_count * 0.18) + (report.question_opener_count * 0.08) - (sum(report.insult_hits.values()) * 0.12))
    self_awareness = _clamp((sum(report.concession_hits.values()) * 0.35) + (sum(report.hedge_hits.values()) * 0.22) - (sum(report.certainty_hits.values()) * 0.08))
    audience_consciousness = _clamp((report.quote_hint_count * 0.18) + (report.second_person_count / tc) * 1.8 + (report.line_count > 2) * 0.08)
    jargon_fluency = _clamp((sum(report.jargon_hits.values()) * 0.24) + ((report.avg_token_length - 4.4) * 0.08))
    contradiction_susceptibility = _clamp((sum(report.certainty_hits.values()) * 0.16) + (sum(report.absolutist_hits.values()) * 0.18) + ((0.3 - min(report.evidence_density, 0.3)) * 1.2))
    moralizing_tendency = _clamp((sum(report.moralizing_hits.values()) * 0.28) + (sum(report.absolutist_hits.values()) * 0.08))
    bait_hunger = _clamp((report.second_person_count / tc) * 2.2 + (report.question_count * 0.09) + (sum(report.insult_hits.values()) * 0.12))
    reply_stamina = _clamp((report.token_count / 85) + (report.line_count * 0.06) + (report.question_count * 0.05))
    ego_fragility = _clamp((aggression * 0.42) + (certainty * 0.24) + (moralizing_tendency * 0.18) - (self_awareness * 0.22))

    base_confidence = _clamp(0.45 + min(report.token_count, 80) / 120)

    rows = [
        (RhetoricalAxis.EGO_FRAGILITY, ego_fragility, ["derived from aggression, certainty, and low concession behavior"]),
        (RhetoricalAxis.VERBOSITY, verbosity, [f"token_count={report.token_count}"]),
        (RhetoricalAxis.CERTAINTY, certainty, ["certainty and absolutist markers minus hedging"]),
        (RhetoricalAxis.AGGRESSION, aggression, ["insults, exclamations, and all-caps intensity"]),
        (RhetoricalAxis.CURIOSITY, curiosity, ["question density adjusted by hostility"]),
        (RhetoricalAxis.SELF_AWARENESS, self_awareness, ["concessions and hedges versus hard certainty"]),
        (RhetoricalAxis.AUDIENCE_CONSCIOUSNESS, audience_consciousness, ["restatement, second-person targeting, and structure"]),
        (RhetoricalAxis.JARGON_FLUENCY, jargon_fluency, ["specialized vocabulary density and token complexity"]),
        (RhetoricalAxis.CONTRADICTION_SUSCEPTIBILITY, contradiction_susceptibility, ["confidence unsupported by evidence"]),
        (RhetoricalAxis.MORALIZING_TENDENCY, moralizing_tendency, ["moral language and absolutism"]),
        (RhetoricalAxis.BAIT_HUNGER, bait_hunger, ["direct engagement markers"]),
        (RhetoricalAxis.REPLY_STAMINA, reply_stamina, ["length, structure, and question count"]),
    ]

    scored: list[AxisScore] = []
    for axis, score, reasons in rows:
        score_scale, confidence_scale, semantic_reasons = _semantic_scale_for_axis(axis, semantic)
        adjusted_score = _clamp(score * score_scale)
        adjusted_confidence = _clamp(base_confidence * confidence_scale)
        scored.append(_axis(axis, adjusted_score, adjusted_confidence, [*reasons, *semantic_reasons]))

    return scored


def axis_map(scores: list[AxisScore]) -> dict[RhetoricalAxis, float]:
    return {score.axis: score.score for score in scores}
