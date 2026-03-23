from __future__ import annotations

import re
from typing import Any

from bait_engine.adapters.contracts import OutboundReplyEnvelope
from bait_engine.adapters.inbound import InboundThreadContext, summarize_thread_context
from bait_engine.adapters.normalize import normalize_target
from bait_engine.adapters.presets import resolve_selection_preset
from bait_engine.adapters.recommend import recommend_selection_preset
from bait_engine.adapters.registry import DEFAULT_ADAPTERS
from bait_engine.adapters.select import SelectionStrategy, describe_auto_best_candidate, select_candidate
from bait_engine.adapters.validate import validate_target


def _validate_platform(platform: str) -> None:
    if platform not in DEFAULT_ADAPTERS:
        raise KeyError(f"adapter '{platform}' not found")


_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


def _normalize_sentence(text: str) -> str:
    sentence = " ".join((text or "").strip().split())
    if not sentence:
        return ""
    if sentence[-1] not in ".!?":
        sentence = f"{sentence}."
    return sentence


def _sentence_fingerprint(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def _compose_top_candidates(candidates: list[dict[str, Any]], limit: int = 3) -> tuple[str | None, list[int]]:
    ranked = sorted(candidates, key=lambda item: int(item.get("rank_index") or 9999))
    selected: list[tuple[str, int]] = []
    seen: set[str] = set()

    for item in ranked:
        rank_index = int(item.get("rank_index") or 0)
        raw = str(item.get("text") or "").strip()
        if not raw:
            continue
        sentences = [
            _normalize_sentence(part)
            for part in _SENTENCE_SPLIT_RE.split(raw)
            if part and part.strip()
        ]
        if not sentences:
            continue

        preferred = sentences[0]
        for sentence in sentences:
            if 45 <= len(sentence) <= 240:
                preferred = sentence
                break

        key = _sentence_fingerprint(preferred)
        if not key or key in seen:
            continue
        seen.add(key)
        selected.append((preferred, rank_index))
        if len(selected) >= max(limit, 1):
            break

    if not selected:
        return None, []

    ordered_sentences = [text for text, _ in selected]
    rank_indexes = [rank for _, rank in selected]

    combined = " ".join(ordered_sentences).strip()
    return combined, rank_indexes


def build_reply_envelope(
    run: dict[str, Any],
    candidate_rank_index: int = 1,
    selection_strategy: SelectionStrategy = "rank",
    selection_preset: str | None = None,
    tactic: str | None = None,
    objective: str | None = None,
    thread_id: str | None = None,
    reply_to_id: str | None = None,
    author_handle: str | None = None,
    metadata: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    reputation_data: dict[str, Any] | None = None,
    combine_top_candidates: bool = False,
    allow_incomplete_target: bool = False,
) -> dict[str, Any]:
    platform = run.get("platform") or "unknown"
    _validate_platform(platform)
    thread_context = InboundThreadContext.model_validate(context) if context is not None else None
    resolved_strategy = selection_strategy
    resolved_tactic = tactic
    resolved_objective = objective
    resolved_preset_name = None
    resolved_dispatch_driver = None
    selection_filter_fallback = False
    if selection_preset is not None:
        preset = (
            recommend_selection_preset(platform, thread_context, reputation_data=reputation_data)
            if selection_preset == "auto"
            else resolve_selection_preset(platform, selection_preset)
        )
        resolved_preset_name = str(preset["name"])
        resolved_dispatch_driver = preset.get("dispatch_driver")
        resolved_strategy = preset["strategy"]
        resolved_tactic = tactic if tactic is not None else preset["tactic"]
        resolved_objective = objective if objective is not None else preset["objective"]

    candidates = run.get("candidates") or []
    try:
        candidate = select_candidate(
            candidates,
            candidate_rank_index=candidate_rank_index,
            strategy=resolved_strategy,
            tactic=resolved_tactic,
            objective=resolved_objective,
            reputation_data=reputation_data,
        )
    except KeyError:
        if resolved_preset_name is None and resolved_tactic is None and resolved_objective is None:
            raise
        candidate = select_candidate(
            candidates,
            candidate_rank_index=candidate_rank_index,
            strategy=resolved_strategy,
            reputation_data=reputation_data,
        )
        resolved_tactic = None
        resolved_objective = None
        selection_filter_fallback = True

    target = normalize_target(
        platform=platform,
        thread_id=thread_id,
        reply_to_id=reply_to_id,
        author_handle=author_handle,
    )
    target_complete = target.reply_to_id is not None or target.thread_id is not None
    if not (allow_incomplete_target and not target_complete):
        validate_target(target)

    candidate_pool = candidates
    if resolved_tactic is not None:
        candidate_pool = [item for item in candidate_pool if item.get("tactic") == resolved_tactic]
    if resolved_objective is not None:
        candidate_pool = [item for item in candidate_pool if item.get("objective") == resolved_objective]

    should_combine = bool(combine_top_candidates or resolved_strategy == "blend_top3")
    composed_body, composed_ranks = _compose_top_candidates(candidate_pool, limit=3) if should_combine else (None, [])
    resolved_body = composed_body or candidate["text"]

    envelope = OutboundReplyEnvelope(
        run_id=run["id"],
        candidate_rank_index=int(candidate.get("rank_index") or candidate_rank_index),
        platform=platform,
        persona=run.get("persona") or "unknown",
        objective=run.get("selected_objective"),
        tactic=candidate.get("tactic") or run.get("selected_tactic"),
        exit_state=run.get("exit_state"),
        body=resolved_body,
        target=target,
        metadata={
            "selection_strategy": resolved_strategy,
            "selection_preset": resolved_preset_name,
            "preferred_dispatch_driver": resolved_dispatch_driver,
            "selection_filter_fallback": selection_filter_fallback,
            "candidate_tactic": candidate.get("tactic"),
            "candidate_objective": candidate.get("objective"),
            "candidate_rank_score": candidate.get("rank_score"),
            "combined_top_candidates": should_combine,
            "combined_candidate_rank_indexes": composed_ranks,
            "target_complete": target_complete,
            "auto_best_rationale": describe_auto_best_candidate(candidate, reputation_data)
            if resolved_strategy == "auto_best"
            else None,
            "thread_context": summarize_thread_context(thread_context) if thread_context is not None else None,
            **(metadata or {}),
        },
    )
    return envelope.model_dump(mode="json")
