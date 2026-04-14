from __future__ import annotations

from bait_engine.generation.contracts import CandidateReply


def rank_candidates(candidates: list[CandidateReply]) -> list[CandidateReply]:
    ranked: list[CandidateReply] = []
    for candidate in candidates:
        score = (
            0.28 * candidate.grounding_score
            + 0.24 * candidate.estimated_bite_score
            + 0.18 * candidate.estimated_audience_score
            + 0.18 * (1.0 - candidate.critic_penalty)
            + 0.12 * (1.0 if len(candidate.text.split()) <= 16 else 0.6)
        )
        ranked.append(candidate.model_copy(update={"rank_score": round(min(1.0, score), 4)}))
    ranked.sort(key=lambda item: item.rank_score, reverse=True)
    return ranked
