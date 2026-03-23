from __future__ import annotations

from typing import Any

from bait_engine.analysis.common import clamp_unit as _clamp
from bait_engine.core.types import Archetype, RhetoricalAxis


BASE_ARCHETYPE_AXIS_WEIGHTS: dict[Archetype, dict[RhetoricalAxis, float]] = {
    Archetype.SPECTRAL: {
        RhetoricalAxis.JARGON_FLUENCY: 0.35,
        RhetoricalAxis.CERTAINTY: 0.20,
        RhetoricalAxis.VERBOSITY: 0.18,
        RhetoricalAxis.AUDIENCE_CONSCIOUSNESS: 0.10,
    },
    Archetype.CONFIDENT_IDIOT: {
        RhetoricalAxis.CERTAINTY: 0.30,
        RhetoricalAxis.CONTRADICTION_SUSCEPTIBILITY: 0.24,
        RhetoricalAxis.MORALIZING_TENDENCY: 0.18,
        RhetoricalAxis.SELF_AWARENESS: -0.10,
    },
    Archetype.AGGRESSIVE_POSTER: {
        RhetoricalAxis.AGGRESSION: 0.45,
        RhetoricalAxis.EGO_FRAGILITY: 0.22,
        RhetoricalAxis.BAIT_HUNGER: 0.16,
    },
    Archetype.CONSPIRACY_COUNTERPARTY: {
        RhetoricalAxis.CERTAINTY: 0.16,
        RhetoricalAxis.CONTRADICTION_SUSCEPTIBILITY: 0.18,
        RhetoricalAxis.JARGON_FLUENCY: 0.12,
        RhetoricalAxis.AUDIENCE_CONSCIOUSNESS: 0.16,
    },
    Archetype.SEALION: {
        RhetoricalAxis.CURIOSITY: 0.34,
        RhetoricalAxis.VERBOSITY: 0.18,
        RhetoricalAxis.BAIT_HUNGER: 0.18,
        RhetoricalAxis.AUDIENCE_CONSCIOUSNESS: 0.10,
    },
    Archetype.NAIVE_LITERALIST: {
        RhetoricalAxis.CURIOSITY: 0.20,
        RhetoricalAxis.CONTRADICTION_SUSCEPTIBILITY: 0.20,
        RhetoricalAxis.CERTAINTY: 0.12,
        RhetoricalAxis.JARGON_FLUENCY: -0.12,
    },
    Archetype.OVEREXTENDER: {
        RhetoricalAxis.VERBOSITY: 0.34,
        RhetoricalAxis.REPLY_STAMINA: 0.24,
        RhetoricalAxis.CERTAINTY: 0.12,
    },
}


def _coerce_archetype(name: str) -> Archetype | None:
    try:
        return Archetype(name)
    except ValueError:
        return None


def _coerce_axis(name: str) -> RhetoricalAxis | None:
    try:
        return RhetoricalAxis(name)
    except ValueError:
        return None


def _learned_axis_weights(weight_profile: dict[str, Any] | None) -> tuple[dict[Archetype, dict[RhetoricalAxis, float]], float, bool]:
    if not weight_profile:
        return BASE_ARCHETYPE_AXIS_WEIGHTS, 0.0, False

    confidence = _clamp(float(weight_profile.get("confidence") or 0.0))
    if confidence <= 0:
        return BASE_ARCHETYPE_AXIS_WEIGHTS, 0.0, False

    learned_blob = weight_profile.get("weights")
    if not isinstance(learned_blob, dict):
        return BASE_ARCHETYPE_AXIS_WEIGHTS, 0.0, False

    merged: dict[Archetype, dict[RhetoricalAxis, float]] = {}
    learned_used = False

    for archetype, base_axes in BASE_ARCHETYPE_AXIS_WEIGHTS.items():
        learned_axes_raw = learned_blob.get(archetype.value)
        learned_axes = learned_axes_raw if isinstance(learned_axes_raw, dict) else {}
        bucket: dict[RhetoricalAxis, float] = {}
        for axis, base_weight in base_axes.items():
            learned_candidate = learned_axes.get(axis.value)
            if isinstance(learned_candidate, (int, float)):
                learned_used = True
                bucket[axis] = float(base_weight) * (1.0 - confidence) + float(learned_candidate) * confidence
            else:
                bucket[axis] = float(base_weight)
        merged[archetype] = bucket

    if not learned_used:
        return BASE_ARCHETYPE_AXIS_WEIGHTS, 0.0, False
    return merged, confidence, True


def blend_archetypes(
    axis_scores: dict[RhetoricalAxis, float],
    *,
    weight_profile: dict[str, Any] | None = None,
) -> dict[Archetype, float]:
    a = axis_scores
    axis_weights, _, _ = _learned_axis_weights(weight_profile)

    raw: dict[Archetype, float] = {}
    for archetype, weights in axis_weights.items():
        score = 0.0
        for axis, weight in weights.items():
            score += float(weight) * a.get(axis, 0.0)
        raw[archetype] = score

    clipped = {k: _clamp(v) for k, v in raw.items()}
    total = sum(clipped.values())
    if total <= 0:
        return {Archetype.NAIVE_LITERALIST: 1.0}
    return {k: round(v / total, 4) for k, v in sorted(clipped.items(), key=lambda item: item[1], reverse=True) if v > 0}


def summarize_weight_profile(weight_profile: dict[str, Any] | None) -> str | None:
    if not weight_profile:
        return None

    sample_size = int(weight_profile.get("sample_size") or 0)
    min_samples = int(weight_profile.get("min_samples") or 0)
    confidence = round(_clamp(float(weight_profile.get("confidence") or 0.0)), 3)
    persona = str(weight_profile.get("persona") or "any")
    platform = str(weight_profile.get("platform") or "any")
    objective = str(weight_profile.get("objective") or "any")

    if weight_profile.get("enabled"):
        return (
            "archetype weights: learned"
            f" persona={persona} platform={platform} objective={objective}"
            f" samples={sample_size} confidence={confidence}"
        )

    fallback_reason = str(weight_profile.get("fallback_reason") or "static_defaults")
    return (
        "archetype weights: static"
        f" persona={persona} platform={platform} objective={objective}"
        f" samples={sample_size}/{min_samples} fallback={fallback_reason}"
    )
