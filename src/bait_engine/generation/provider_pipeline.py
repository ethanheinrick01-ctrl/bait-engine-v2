from __future__ import annotations

import logging

from bait_engine.generation.contracts import CandidateReply, DraftRequest, DraftResult
from bait_engine.generation.critic import critique_candidate, starts_with_agreement_language
from bait_engine.generation.llm_writer import generate_candidates_via_provider, _is_refusal
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
                estimated_bite_score=0.58,
                estimated_audience_score=0.54,
            )
        )
    return fallback


_MAX_PROVIDER_ATTEMPTS = 2


def _strip_refusals(candidates: list[CandidateReply]) -> list[CandidateReply]:
    """Remove any candidate whose text is an LLM refusal."""
    return [c for c in candidates if not _is_refusal(c.text)]


def draft_candidates_with_provider(
    request: DraftRequest,
    provider: TextGenerationProvider | None = None,
) -> DraftResult:
    provider = provider or OpenAICompatibleProvider()

    raw_candidates: list[CandidateReply] = []
    if provider.is_available():
        for attempt in range(_MAX_PROVIDER_ATTEMPTS):
            try:
                raw_candidates = generate_candidates_via_provider(request, provider)
            except RuntimeError as exc:
                _log_provider_failure(request, provider, exc, unexpected=False)
                raw_candidates = []
            except Exception as exc:
                _log_provider_failure(request, provider, exc, unexpected=True)
                raw_candidates = []

            # Strip any refusals that slipped through the parser
            raw_candidates = _strip_refusals(raw_candidates)
            if raw_candidates:
                break
            logger.warning(
                "Provider attempt %d/%d returned only refusals; %s",
                attempt + 1,
                _MAX_PROVIDER_ATTEMPTS,
                "retrying" if attempt + 1 < _MAX_PROVIDER_ATTEMPTS else "falling back to heuristic",
            )

    if not raw_candidates:
        raw_candidates = generate_candidates(request)

    critiqued = [critique_candidate(candidate, request.persona) for candidate in raw_candidates]
    guarded = _enforce_disagreement(critiqued, request)
    ranked = rank_candidates(guarded)

    # Final safety net: drop any refusals that survived critic + ranking
    ranked = _strip_refusals(ranked)
    if not ranked:
        logger.warning("All ranked candidates were refusals; regenerating via heuristic")
        heuristic = generate_candidates(request)
        critiqued = [critique_candidate(c, request.persona) for c in heuristic]
        guarded = _enforce_disagreement(critiqued, request)
        ranked = rank_candidates(guarded)

    return DraftResult(request=request, candidates=ranked)
