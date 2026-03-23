from __future__ import annotations

import math
import re
from typing import Any

from bait_engine.analysis import AnalyzeInput, analyze_comment
from bait_engine.intake.contracts import HuntTarget, compose_source_text


_WORD_RE = re.compile(r"[A-Za-z']+")
_DEFENSIVE_PHRASES = (
    "that's not what i said",
    "you are twisting",
    "you're twisting",
    "strawman",
    "out of context",
    "you clearly didn't read",
    "you dont understand",
    "you don't understand",
    "nice try",
    "cope",
)
_EMOTIONAL_TERMS = {
    "angry",
    "mad",
    "furious",
    "insane",
    "ridiculous",
    "stupid",
    "dumb",
    "pathetic",
    "embarrassing",
    "wild",
    "crazy",
    "delusional",
}
_TOPIC_SHIFT_MARKERS = (
    "anyway",
    "besides",
    "meanwhile",
    "by the way",
    "off topic",
    "irrelevant",
    "also",
)


def _axis_map(analysis: dict[str, Any]) -> dict[str, float]:
    axes = analysis.get("axes") or []
    return {str(item.get("axis")): float(item.get("score") or 0.0) for item in axes if isinstance(item, dict)}


def _bounded(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _tokenize_words(text: str) -> list[str]:
    return [token.lower() for token in _WORD_RE.findall(text or "")]


def _bite_detection(text: str, metadata: dict[str, Any], analysis: dict[str, Any]) -> dict[str, Any]:
    lowered = (text or "").lower()
    words = _tokenize_words(text)
    token_count = len(words)
    unique_ratio = (len(set(words)) / token_count) if token_count else 0.0

    emotional_hits = sum(1 for token in words if token in _EMOTIONAL_TERMS)
    emotional_ratio = (emotional_hits / max(token_count, 1))
    punctuation_spike = _bounded(((text.count("!") * 1.4) + text.count("?")) / 10.0)
    emotional_spike = _bounded(emotional_ratio * 8.0 + punctuation_spike * 0.45)

    defensive_hits = sum(1 for phrase in _DEFENSIVE_PHRASES if phrase in lowered)
    defensive_signal = _bounded((defensive_hits / 2.0) + (0.18 if "you " in lowered else 0.0))

    topic_shift_hits = sum(1 for marker in _TOPIC_SHIFT_MARKERS if marker in lowered)
    contradiction_count = len(analysis.get("contradictions") or [])
    topic_shift_signal = _bounded((topic_shift_hits / 2.0) + (0.15 if contradiction_count else 0.0) + (0.15 if unique_ratio >= 0.78 else 0.0))

    baseline_len = metadata.get("avg_reply_length") or metadata.get("parent_length") or metadata.get("baseline_length")
    if baseline_len is not None and float(baseline_len) > 0:
        length_spike = _bounded((float(token_count) / float(baseline_len)) - 1.0)
    else:
        length_spike = _bounded((token_count - 18) / 30.0)

    bite_score = _bounded(
        0.32 * length_spike
        + 0.24 * emotional_spike
        + 0.26 * defensive_signal
        + 0.18 * topic_shift_signal
    )
    qualified = bite_score >= 0.46 or (defensive_signal >= 0.5 and emotional_spike >= 0.32)

    reasons: list[str] = []
    if length_spike >= 0.4:
        reasons.append("reply length spike")
    if emotional_spike >= 0.33:
        reasons.append("emotional language spike")
    if defensive_signal >= 0.4:
        reasons.append("defensive phrasing")
    if topic_shift_signal >= 0.35:
        reasons.append("topic-shift pressure")
    if not reasons:
        reasons.append("no strong bite indicators yet")

    return {
        "score": round(bite_score, 4),
        "qualified": qualified,
        "reasons": reasons,
        "signals": {
            "length_spike": round(length_spike, 4),
            "emotional_spike": round(emotional_spike, 4),
            "defensive_signal": round(defensive_signal, 4),
            "topic_shift_signal": round(topic_shift_signal, 4),
        },
    }


def _log_signal(value: float | int | None, ceiling: float) -> float:
    numeric = max(0.0, float(value or 0.0))
    if ceiling <= 1:
        return _bounded(numeric)
    return _bounded(math.log1p(numeric) / math.log1p(float(ceiling)))


def score_target(target: HuntTarget) -> dict[str, Any]:
    source_text = compose_source_text(target.subject, target.body)
    analysis = analyze_comment(AnalyzeInput(text=source_text, platform=target.platform)).model_dump(mode="json")
    axes = _axis_map(analysis)
    opportunity = analysis.get("opportunity") or {}
    metadata = target.metadata or {}

    audience_signal = max(
        _log_signal(metadata.get("score"), 250),
        _log_signal(metadata.get("like_count"), 250),
        _log_signal(metadata.get("upvote_count"), 250),
    )
    conversation_signal = max(
        _log_signal(metadata.get("num_comments"), 120),
        _log_signal(metadata.get("reply_count"), 120),
        _log_signal(metadata.get("quote_count"), 80),
    )
    contradiction_signal = _bounded(len(analysis.get("contradictions") or []) / 3.0)
    certainty_signal = axes.get("certainty", 0.0)
    bait_hunger_signal = axes.get("bait_hunger", 0.0)

    reply_probability = float(opportunity.get("reply_probability") or 0.0)
    engagement_value = float(opportunity.get("engagement_value") or 0.0)
    essay_probability = float(opportunity.get("essay_probability") or 0.0)
    audience_value = float(opportunity.get("audience_value") or 0.0)
    boring_penalty = float(opportunity.get("risk_of_boringness") or 0.0)
    overplay_penalty = float(opportunity.get("overplay_risk") or 0.0)

    bite_detection = _bite_detection(source_text, metadata, analysis)

    score = _bounded(
        0.24 * reply_probability
        + 0.20 * engagement_value
        + 0.14 * essay_probability
        + 0.14 * audience_value
        + 0.10 * contradiction_signal
        + 0.08 * bait_hunger_signal
        + 0.05 * certainty_signal
        + 0.03 * audience_signal
        + 0.06 * conversation_signal
        + 0.08 * float(bite_detection["score"])
        - 0.08 * boring_penalty
        - 0.04 * overplay_penalty
    )
    if not bool(bite_detection["qualified"]):
        score = min(score, 0.61)

    reasons: list[str] = []
    if conversation_signal >= 0.35:
        reasons.append("active conversation surface")
    if reply_probability >= 0.35:
        reasons.append("reply probability looks healthy")
    if essay_probability >= 0.35:
        reasons.append("target may overextend")
    if contradiction_signal >= 0.34:
        reasons.append("contradiction surface present")
    if audience_signal >= 0.3:
        reasons.append("audience signal is nontrivial")
    if bite_detection["qualified"]:
        reasons.append("bite detection qualified")
    else:
        reasons.append("bite detection not yet qualified")
    if not reasons:
        reasons.append("moderate bait surface; keep under review")

    return {
        "score": round(score, 4),
        "reasons": reasons,
        "bite_detection": bite_detection,
        "signals": {
            "reply_probability": round(reply_probability, 4),
            "engagement_value": round(engagement_value, 4),
            "essay_probability": round(essay_probability, 4),
            "audience_value": round(audience_value, 4),
            "audience_signal": round(audience_signal, 4),
            "conversation_signal": round(conversation_signal, 4),
            "contradiction_signal": round(contradiction_signal, 4),
            "certainty_signal": round(certainty_signal, 4),
            "bait_hunger_signal": round(bait_hunger_signal, 4),
            "boring_penalty": round(boring_penalty, 4),
            "overplay_penalty": round(overplay_penalty, 4),
        },
        "analysis": analysis,
    }


def rank_targets(targets: list[HuntTarget]) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for target in targets:
        scored = score_target(target)
        ranked.append({
            "target": target,
            "score": {k: v for k, v in scored.items() if k != "analysis"},
            "analysis": scored["analysis"],
        })
    ranked.sort(key=lambda item: float((item.get("score") or {}).get("score") or 0.0), reverse=True)
    return ranked
