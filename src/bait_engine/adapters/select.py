from __future__ import annotations

from math import log
from typing import Any, Literal

SelectionStrategy = Literal["rank", "top_score", "highest_bite", "highest_audience", "lowest_penalty", "auto_best", "blend_top3", "mega_bait"]
DispatchDriver = Literal["manual_copy", "jsonl_append", "webhook_post", "reddit_api", "x_api", "auto"]


def _match_candidate(candidate: dict[str, Any], tactic: str | None = None, objective: str | None = None) -> bool:
    if tactic is not None and candidate.get("tactic") != tactic:
        return False
    if objective is not None and candidate.get("objective") != objective:
        return False
    return True


def _auto_best_score(candidate: dict[str, Any], reputation_data: dict[str, Any]) -> tuple[float, float, float, float, float]:
    perf = reputation_data.get("tactic_performance") or {}
    tactic_stats = perf.get(candidate.get("tactic"), {}) if isinstance(perf, dict) else {}

    rate = float(tactic_stats.get("rate") or 0.0)
    count = float(tactic_stats.get("count") or 0.0)
    replies = float(tactic_stats.get("replies") or 0.0)
    avg_engagement = float(tactic_stats.get("avg_engagement") or 0.0)
    avg_reply_delay = float(tactic_stats.get("avg_reply_delay") or 0.0)
    delivery_confidence = float(tactic_stats.get("delivery_confidence") or 0.0)

    global_delivery_confidence = float(reputation_data.get("delivery_confidence") or 0.0)
    global_avg_engagement = float(reputation_data.get("avg_engagement") or 0.0)

    rank_score = float(candidate.get("rank_score") or 0.0)
    bite = float(candidate.get("estimated_bite_score") or 0.0)
    audience = float(candidate.get("estimated_audience_score") or 0.0)
    penalty = float(candidate.get("critic_penalty") or 0.0)

    # Diminishing-returns confidence weight: tiny samples no longer dominate.
    confidence = min(log(count + 1.0, 10), 1.0)
    empirical_rate = (replies / count) if count > 0 else rate
    smoothed_rate = ((replies + 1.0) / (count + 4.0)) if count > 0 else (rate * 0.5)
    calibrated_rate = smoothed_rate if count > 0 else empirical_rate
    volume_bonus = min(count / 25.0, 1.0) * 0.08

    normalized_engagement = min(avg_engagement / 10.0, 1.0) if avg_engagement > 0 else min(global_avg_engagement / 10.0, 1.0)
    # Prefer faster replies if available. 300s = neutral baseline.
    speed_bonus = 0.0
    if avg_reply_delay > 0:
        speed_bonus = max(min((300.0 - avg_reply_delay) / 600.0, 0.12), -0.06)

    delivery_signal = delivery_confidence if delivery_confidence > 0 else global_delivery_confidence
    historical_edge = (
        calibrated_rate * (0.42 + confidence * 0.5)
        + volume_bonus
        + normalized_engagement * 0.12
        + delivery_signal * 0.08
        + speed_bonus
    )

    # Composite decision score; tie-breakers preserve deterministic stability.
    composite = (
        historical_edge * 0.52
        + rank_score * 0.22
        + bite * 0.14
        + audience * 0.12
        - penalty * 0.12
    )
    return (composite, historical_edge, rank_score, bite + audience, -penalty)


def describe_auto_best_candidate(candidate: dict[str, Any], reputation_data: dict[str, Any] | None) -> dict[str, Any] | None:
    if not reputation_data:
        return None
    perf = reputation_data.get("tactic_performance") or {}
    tactic = candidate.get("tactic")
    if not isinstance(perf, dict) or tactic not in perf:
        return None
    tactic_stats = perf.get(tactic) or {}
    return {
        "tactic": tactic,
        "count": int(tactic_stats.get("count") or 0),
        "rate": float(tactic_stats.get("rate") or 0.0),
        "avg_engagement": tactic_stats.get("avg_engagement"),
        "avg_reply_delay": tactic_stats.get("avg_reply_delay"),
        "delivery_confidence": float(tactic_stats.get("delivery_confidence") or 0.0),
        "global_reply_rate": float(reputation_data.get("reply_rate") or 0.0),
        "global_delivery_confidence": float(reputation_data.get("delivery_confidence") or 0.0),
    }


def select_dispatch_driver(platform: str | None, requested: DispatchDriver | str | None = "auto") -> str:
    explicit = str(requested or "auto")
    if explicit != "auto":
        return explicit
    normalized_platform = str(platform or "").lower()
    if normalized_platform == "reddit":
        return "reddit_api"
    if normalized_platform == "x":
        return "x_api"
    return "manual_copy"


def select_candidate(
    candidates: list[dict[str, Any]],
    candidate_rank_index: int = 1,
    strategy: SelectionStrategy = "rank",
    tactic: str | None = None,
    objective: str | None = None,
    reputation_data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    filtered = [candidate for candidate in candidates if _match_candidate(candidate, tactic=tactic, objective=objective)]
    if not filtered:
        raise KeyError("no candidates matched the requested filters")

    if strategy == "auto_best":
        # Fall back to top_score if no reputation.
        if not reputation_data or not reputation_data.get("tactic_performance"):
            strategy = "top_score"
        else:
            return max(filtered, key=lambda item: _auto_best_score(item, reputation_data))

    if strategy == "rank":
        candidate = next((item for item in filtered if item.get("rank_index") == candidate_rank_index), None)
        if candidate is None and 1 <= candidate_rank_index <= len(filtered):
            candidate = filtered[candidate_rank_index - 1]
        if candidate is None:
            raise KeyError(f"candidate rank_index={candidate_rank_index} not found")
        return candidate

    if strategy in {"top_score", "blend_top3", "mega_bait"}:
        return max(filtered, key=lambda item: (float(item.get("rank_score") or 0.0), -int(item.get("rank_index") or 999999)))
    if strategy == "highest_bite":
        return max(filtered, key=lambda item: (float(item.get("estimated_bite_score") or 0.0), float(item.get("rank_score") or 0.0)))
    if strategy == "highest_audience":
        return max(filtered, key=lambda item: (float(item.get("estimated_audience_score") or 0.0), float(item.get("rank_score") or 0.0)))
    if strategy == "lowest_penalty":
        return min(filtered, key=lambda item: (float(item.get("critic_penalty") or 0.0), -float(item.get("rank_score") or 0.0)))

    raise KeyError(f"unknown selection strategy '{strategy}'")
