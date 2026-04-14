from __future__ import annotations

import logging

from bait_engine.generation.contracts import CandidateReply, DraftRequest, DraftResult
from bait_engine.generation.critic import critique_candidate, objective_shape_ok, starts_with_agreement_language
from bait_engine.generation.llm_writer import generate_candidates_via_provider
from bait_engine.generation.ranker import rank_candidates
from bait_engine.generation.writer import generate_candidates
from bait_engine.providers.base import TextGenerationProvider
from bait_engine.providers.openai_compatible import OpenAICompatibleProvider


logger = logging.getLogger(__name__)


DISAGREE_FALLBACKS = [
    "nah, that conclusion doesn't actually follow",
    "not really, you're skipping the part that matters",
    "that's a neat framing, but it's still wrong on impact",
]


def _provider_context(request: DraftRequest, provider: TextGenerationProvider) -> dict[str, str | int | None]:
    return {
        "provider": provider.__class__.__name__,
        "persona": request.persona.name,
        "objective": request.plan.selected_objective.value,
        "tactic": request.plan.selected_tactic.value if request.plan.selected_tactic else None,
        "candidate_count": request.candidate_count,
    }


def _log_provider_failure(request: DraftRequest, provider: TextGenerationProvider, exc: Exception, *, unexpected: bool) -> None:
    context = _provider_context(request, provider)
    message = (
        "Provider-backed drafting failed for provider=%s persona=%s objective=%s tactic=%s candidate_count=%s; "
        "falling back to heuristic generator"
    )
    args = (
        context["provider"],
        context["persona"],
        context["objective"],
        context["tactic"],
        context["candidate_count"],
    )
    if unexpected:
        logger.exception(message, *args)
        return
    logger.warning("%s: %s", message % args, exc)


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


def draft_candidates_with_provider(
    request: DraftRequest,
    provider: TextGenerationProvider | None = None,
) -> DraftResult:
    provider = provider or OpenAICompatibleProvider()

    raw_candidates = []
    if provider.is_available():
        for attempt in range(2):
            try:
                raw_candidates = generate_candidates_via_provider(request, provider)
                if attempt == 0 or raw_candidates:
                    break
            except RuntimeError as exc:
                if attempt == 0:
                    continue
                _log_provider_failure(request, provider, exc, unexpected=False)
                raw_candidates = []
            except Exception as exc:
                if attempt == 0:
                    continue
                _log_provider_failure(request, provider, exc, unexpected=True)
                raw_candidates = []

    if not raw_candidates:
        raw_candidates = [
            candidate.model_copy(update={"generation_source": "heuristic_fallback"})
            for candidate in generate_candidates(request)
        ]

    critiqued = [
        critique_candidate(
            candidate,
            request.persona,
            source_text=request.source_text,
            objective=request.plan.selected_objective.value,
        )
        for candidate in raw_candidates
    ]
    validated = _filter_valid_candidates(critiqued, request)
    guarded = _enforce_disagreement(validated, request)
    ranked = rank_candidates(guarded)
    return DraftResult(request=request, candidates=ranked)
