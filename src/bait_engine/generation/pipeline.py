from __future__ import annotations

import logging

from bait_engine.generation.contracts import CandidateReply, DraftRequest, DraftResult
from bait_engine.generation.critic import critique_candidate, objective_shape_ok, starts_with_agreement_language
from bait_engine.generation.ranker import rank_candidates
from bait_engine.generation.writer import generate_candidates


logger = logging.getLogger(__name__)


DISAGREE_FALLBACKS = [
    "that leap still doesn't prove your claim",
    "you're skipping the step that actually matters",
    "useful isn't the same as true, that's the gap",
]
QUESTION_DISAGREE_FALLBACKS = [
    "where is the missing step between premise and conclusion?",
    "what proof is supposed to carry that leap?",
    "how does that line establish the actual claim?",
]


def _enforce_disagreement(candidates: list[CandidateReply], request: DraftRequest) -> list[CandidateReply]:
    if request.plan.selected_objective.value == "do_not_engage":
        return candidates

    filtered = [candidate for candidate in candidates if not starts_with_agreement_language(candidate.text)]
    if filtered:
        return filtered

    objective = request.plan.selected_objective.value
    requires_question = objective in {"hook", "resurrect", "stall", "branch_split"}
    fallback_pool = QUESTION_DISAGREE_FALLBACKS if requires_question else DISAGREE_FALLBACKS
    fallback: list[CandidateReply] = []
    for idx in range(request.candidate_count):
        text = fallback_pool[idx % len(fallback_pool)]
        fallback.append(
            CandidateReply(
                text=text,
                tactic=request.plan.selected_tactic,
                objective=request.plan.selected_objective.value,
                persona=request.persona.name,
                grounding_score=0.22,
                generation_source="disagreement_fallback",
                estimated_bite_score=0.58,
                estimated_audience_score=0.54,
            )
        )
    return fallback


def _filter_valid_candidates(candidates: list[CandidateReply], request: DraftRequest) -> list[CandidateReply]:
    objective = request.plan.selected_objective.value
    if objective == "do_not_engage":
        return []

    valid: list[CandidateReply] = []
    for candidate in candidates:
        if starts_with_agreement_language(candidate.text):
            continue
        if candidate.grounding_score < 0.16:
            continue
        if not objective_shape_ok(candidate.text, objective):
            continue
        valid.append(candidate)
    return valid


def _backfill_candidate_floor(
    validated: list[CandidateReply],
    critiqued: list[CandidateReply],
    request: DraftRequest,
) -> list[CandidateReply]:
    objective = request.plan.selected_objective.value
    if objective == "do_not_engage":
        return validated

    floor = min(max(1, request.candidate_count), 3)
    if len(validated) >= floor:
        return validated

    selected = list(validated)
    seen = {candidate.text.strip().lower() for candidate in selected if candidate.text.strip()}
    for candidate in critiqued:
        if len(selected) >= floor:
            break
        normalized = candidate.text.strip().lower()
        if not normalized or normalized in seen:
            continue
        if starts_with_agreement_language(candidate.text):
            continue
        if not objective_shape_ok(candidate.text, objective):
            continue
        if candidate.grounding_score < 0.08:
            continue
        seen.add(normalized)
        selected.append(
            candidate.model_copy(
                update={
                    "generation_source": (
                        f"{candidate.generation_source}_floor_backfill"
                        if candidate.generation_source
                        else "floor_backfill"
                    )
                }
            )
        )
    return selected


def draft_candidates(request: DraftRequest) -> DraftResult:
    logger.debug(
        "draft_candidates: persona=%s objective=%s tactic=%s count=%d",
        request.persona.name,
        request.plan.selected_objective.value,
        request.plan.selected_tactic.value if request.plan.selected_tactic else None,
        request.candidate_count,
    )
    raw = generate_candidates(request)
    critiqued = [
        critique_candidate(
            candidate,
            request.persona,
            source_text=request.source_text,
            objective=request.plan.selected_objective.value,
        )
        for candidate in raw
    ]
    validated = _filter_valid_candidates(critiqued, request)
    with_floor = _backfill_candidate_floor(validated, critiqued, request)
    guarded = _enforce_disagreement(with_floor, request)
    ranked = rank_candidates(guarded)
    logger.debug("draft_candidates: produced %d candidates", len(ranked))
    return DraftResult(request=request, candidates=ranked)
