from __future__ import annotations

from typing import Any

from bait_engine.core.types import AnalysisResult, PersonaRouterDecision, RhetoricalAxis
from bait_engine.planning.personas import DEFAULT_PERSONAS, PersonaProfile


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _axis(result: AnalysisResult, axis: RhetoricalAxis, default: float = 0.5) -> float:
    for score in result.axes:
        if score.axis == axis:
            return _clamp(score.score)
    return _clamp(default)


def _length_mid_score(persona: PersonaProfile) -> float:
    low, high = persona.length_band_words
    midpoint = (float(low) + float(high)) / 2.0
    return _clamp((midpoint - 4.0) / 18.0)


def _prior_entry(prior: dict[str, Any] | None) -> tuple[float, float]:
    if not isinstance(prior, dict):
        return 0.5, 0.0
    raw_score = prior.get("score")
    raw_confidence = prior.get("confidence")
    return _clamp(raw_score if raw_score is not None else 0.5), _clamp(raw_confidence if raw_confidence is not None else 0.0)


def _calibration_entry(calibration: dict[str, Any] | None, persona_name: str) -> tuple[float, float]:
    if not isinstance(calibration, dict):
        return 0.5, 0.0
    weights = calibration.get("weights") if isinstance(calibration.get("weights"), dict) else {}
    persona_state = weights.get(persona_name) if isinstance(weights.get(persona_name), dict) else {}
    score = _clamp(persona_state.get("score") if persona_state.get("score") is not None else 0.5)
    confidence = _clamp(
        (persona_state.get("confidence") if persona_state.get("confidence") is not None else calibration.get("segment_confidence") or 0.0)
    )
    return score, confidence


def _duel_personas(
    *,
    first_name: str,
    second_name: str,
    personas: dict[str, PersonaProfile],
    analysis: AnalysisResult,
    diagnostics: dict[str, dict[str, float]],
    close_margin: float,
    fallback_persona: str,
) -> tuple[str, list[str]]:
    first = personas[first_name]
    second = personas[second_name]

    target_aggression = _axis(analysis, RhetoricalAxis.AGGRESSION)
    target_boring = _clamp(analysis.opportunity.risk_of_boringness)

    calm_need = _clamp(1.0 - target_aggression)
    absurd_need = _clamp(target_boring)

    def duel_score(profile: PersonaProfile, diag: dict[str, float]) -> float:
        calm_fit = 1.0 - abs(profile.calmness_preference - calm_need)
        absurd_fit = 1.0 - abs(profile.absurdity_tolerance - absurd_need)
        calibration_blend = float(diag.get("calibration_blend") or 0.0)
        calibration_conf = float(diag.get("calibrated_confidence") or 0.0)
        return (calm_fit * 0.45) + (absurd_fit * 0.35) + (calibration_blend * calibration_conf * 0.2)

    first_score = duel_score(first, diagnostics.get(first_name, {}))
    second_score = duel_score(second, diagnostics.get(second_name, {}))
    delta = first_score - second_score

    notes = [f"duel={first_name}:{first_score:.4f}|{second_name}:{second_score:.4f}"]
    duel_threshold = max(0.008, close_margin * 0.3)
    if abs(delta) >= duel_threshold:
        winner = first_name if delta > 0 else second_name
        notes.append(f"duel_winner={winner}")
        return winner, notes

    fallback = fallback_persona if fallback_persona in personas else first_name
    notes.append("duel_inconclusive=fallback")
    return fallback, notes


def select_persona(
    analysis: AnalysisResult,
    *,
    platform: str,
    priors: dict[str, dict[str, Any]] | None = None,
    calibration: dict[str, Any] | None = None,
    personas: dict[str, PersonaProfile] | None = None,
    fallback_persona: str = "dry_midwit_savant",
    close_margin: float = 0.035,
) -> PersonaRouterDecision:
    persona_profiles = personas or DEFAULT_PERSONAS

    target_aggression = _axis(analysis, RhetoricalAxis.AGGRESSION)
    target_jargon = _axis(analysis, RhetoricalAxis.JARGON_FLUENCY)
    target_verbosity = _axis(analysis, RhetoricalAxis.VERBOSITY)
    target_certainty = _axis(analysis, RhetoricalAxis.CERTAINTY)
    target_curiosity = _axis(analysis, RhetoricalAxis.CURIOSITY)
    target_bait_hunger = _axis(analysis, RhetoricalAxis.BAIT_HUNGER)

    absurdity_need = _clamp((analysis.opportunity.risk_of_boringness * 0.85) + (target_bait_hunger * 0.35))
    calmness_need = _clamp(1.0 - (target_aggression * 0.75))

    persona_scores: dict[str, float] = {}
    diagnostics: dict[str, dict[str, float]] = {}

    calibration_segment_confidence = _clamp((calibration or {}).get("segment_confidence") if isinstance(calibration, dict) else 0.0)

    for name, persona in sorted(persona_profiles.items(), key=lambda item: item[0]):
        jargon_fit = 1.0 - abs(persona.jargon_ceiling - target_jargon)
        absurd_fit = 1.0 - abs(persona.absurdity_tolerance - absurdity_need)
        calm_fit = 1.0 - abs(persona.calmness_preference - calmness_need)
        length_fit = 1.0 - abs(_length_mid_score(persona) - target_verbosity)

        certainty_bias = _clamp((target_certainty * 0.5) + ((1.0 - target_curiosity) * 0.5))
        pressure_bias = {
            "surgical_pinch": 0.72,
            "taunt_escalator": 0.85,
            "ice_pick": 0.35,
            "velvet_snare": 0.46,
            "chaos_ramp": 0.92,
        }.get(persona.pressure_profile, 0.55)
        pressure_fit = 1.0 - abs(pressure_bias - certainty_bias)

        base_score = (
            (jargon_fit * 0.23)
            + (absurd_fit * 0.24)
            + (calm_fit * 0.22)
            + (length_fit * 0.15)
            + (pressure_fit * 0.16)
        )

        prior_score, prior_confidence = _prior_entry((priors or {}).get(name))
        prior_blend = prior_confidence * 0.35
        score_after_prior = _clamp((base_score * (1.0 - prior_blend)) + (prior_score * prior_blend))

        calibrated_score, calibrated_confidence = _calibration_entry(calibration, name)
        calibration_blend = _clamp(calibration_segment_confidence * calibrated_confidence * 0.55)
        score = _clamp((score_after_prior * (1.0 - calibration_blend)) + (calibrated_score * calibration_blend))

        persona_scores[name] = round(score, 4)
        diagnostics[name] = {
            "base_score": round(base_score, 4),
            "prior_score": round(prior_score, 4),
            "prior_confidence": round(prior_confidence, 4),
            "prior_blend": round(prior_blend, 4),
            "calibrated_score": round(calibrated_score, 4),
            "calibrated_confidence": round(calibrated_confidence, 4),
            "calibration_blend": round(calibration_blend, 4),
        }

    ranked = sorted(persona_scores.items(), key=lambda item: (-item[1], item[0]))
    selected_persona = ranked[0][0]
    top_score = float(ranked[0][1])
    second_score = float(ranked[1][1]) if len(ranked) > 1 else top_score
    margin = max(0.0, top_score - second_score)

    why_selected = [
        f"platform={platform or 'unknown'}",
        f"score={top_score:.4f}",
        f"margin={margin:.4f}",
    ]

    used_duel = False
    if margin < close_margin and len(ranked) > 1:
        duel_winner, duel_notes = _duel_personas(
            first_name=ranked[0][0],
            second_name=ranked[1][0],
            personas=persona_profiles,
            analysis=analysis,
            diagnostics=diagnostics,
            close_margin=close_margin,
            fallback_persona=fallback_persona,
        )
        selected_persona = duel_winner
        why_selected.append(f"close_scores<{close_margin:.3f}")
        why_selected.extend(duel_notes)
        used_duel = True

    selected_diag = diagnostics.get(selected_persona)
    if selected_diag:
        why_selected.append(f"base={selected_diag['base_score']:.4f}")
        if selected_diag["prior_confidence"] > 0:
            why_selected.append(f"prior={selected_diag['prior_score']:.4f}@{selected_diag['prior_confidence']:.2f}")
        if selected_diag["calibration_blend"] > 0:
            why_selected.append(
                f"calibration={selected_diag['calibrated_score']:.4f}@{selected_diag['calibrated_confidence']:.2f}"
            )

    confidence = _clamp(0.42 + (margin * 2.1))
    if used_duel:
        confidence = _clamp(confidence * 0.95)
    if selected_persona == fallback_persona and margin < close_margin:
        confidence = _clamp(confidence * 0.9)

    calibration_version = str((calibration or {}).get("version") or "") or None
    calibration_timestamp = str((calibration or {}).get("timestamp") or "") or None
    segment_key = str((calibration or {}).get("segment") or "") or None

    return PersonaRouterDecision(
        selected_persona=selected_persona,
        persona_scores=persona_scores,
        confidence=round(confidence, 4),
        why_selected=why_selected,
        calibration_version=calibration_version,
        calibration_timestamp=calibration_timestamp,
        segment_confidence=round(calibration_segment_confidence, 4),
        segment_key=segment_key,
    )
