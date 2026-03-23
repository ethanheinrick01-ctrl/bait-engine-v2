from __future__ import annotations

from bait_engine.generation.contracts import CandidateReply, DraftRequest, DraftResult
from bait_engine.generation.critic import critique_candidate, starts_with_agreement_language
from bait_engine.generation.ranker import rank_candidates
from bait_engine.generation.writer import generate_candidates


DISAGREE_FALLBACKS = [
    "nah, that conclusion doesn't actually follow",
    "not really, you're skipping the part that matters",
    "that's a neat framing, but it's still wrong on impact",
]


def _enforce_disagreement(candidates: list[CandidateReply], request: DraftRequest) -> list[CandidateReply]:
    if request.plan.selected_objective.value == "do_not_engage":
        return candidates

    filtered = [candidate for candidate in candidates if not starts_with_agreement_language(candidate.text)]
    if filtered:
        return filtered

    fallback: list[CandidateReply] = []
    for idx in range(request.candidate_count):
        text = DISAGREE_FALLBACKS[idx % len(DISAGREE_FALLBACKS)]
        fallback.append(
            CandidateReply(
                text=text,
                tactic=request.plan.selected_tactic,
                objective=request.plan.selected_objective.value,
                persona=request.persona.name,
                estimated_bite_score=0.58,
                estimated_audience_score=0.54,
            )
        )
    return fallback


def draft_candidates(request: DraftRequest) -> DraftResult:
    raw = generate_candidates(request)
    critiqued = [critique_candidate(candidate, request.persona) for candidate in raw]
    guarded = _enforce_disagreement(critiqued, request)
    ranked = rank_candidates(guarded)
    return DraftResult(request=request, candidates=ranked)
