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
_BLEND_PREFIXES = {
    "small correction",
    "premise check",
    "quick calibration",
    "translation",
    "let's tighten this",
    "version that survives contact",
    "if we're scoring rigor",
    "mechanically",
    "clean room pass",
    "diagnosis",
    "reality check",
    "in one line",
    "plain terms",
    "premise first",
    "quick question",
}
_BLEND_STING_MARKERS = (
    "that's the gap",
    "that's the miss",
    "that's the whole gap",
    "that's the whole trick",
    "prove it",
    "category slop",
    "label swapping",
    "cartoon logic",
    "confidence cosplay",
    "rhetorical cosplay",
    "cosplay",
    "still underdetermined",
)
_BLEND_RELATION_MARKERS = (
    " doesn't ",
    " isn't ",
    " not ",
    " equivalent ",
    " equals ",
    " count as ",
    " becomes ",
    " make ",
    " with ",
    " enough for ",
    " get you ",
)
_LOW_SIGNAL_CLAUSE_RE = re.compile(r"^that'?s (?:a |an )?[a-z]+ framing$", re.IGNORECASE)


def _normalize_sentence(text: str) -> str:
    sentence = " ".join((text or "").strip().split())
    if not sentence:
        return ""
    if sentence[-1] not in ".!?":
        sentence = f"{sentence}."
    return sentence


def _sentence_fingerprint(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def _candidate_text(candidate: dict[str, Any]) -> str:
    text = candidate.get("text")
    if text is None:
        return ""
    if isinstance(text, str):
        return text
    return str(text)


def _strip_blend_prefix(text: str) -> str:
    cleaned = " ".join((text or "").strip().split())
    lowered = cleaned.lower()
    for prefix in sorted(_BLEND_PREFIXES, key=len, reverse=True):
        token = f"{prefix}, "
        if lowered.startswith(token):
            return cleaned[len(token):].strip()
    return cleaned


def _candidate_clauses(text: str) -> list[str]:
    cleaned = _strip_blend_prefix(text)
    clauses: list[str] = []
    for sentence in _SENTENCE_SPLIT_RE.split(cleaned):
        sentence = sentence.strip()
        if not sentence:
            continue
        for clause in re.split(r",\s+", sentence):
            normalized = " ".join(clause.strip().split())
            if len(normalized.split()) < 3:
                continue
            clauses.append(normalized.rstrip(".?!"))
    return clauses


def _looks_like_sting(clause: str) -> bool:
    lowered = f" {clause.lower()} "
    return any(marker in lowered for marker in _BLEND_STING_MARKERS)


def _looks_like_relation(clause: str) -> bool:
    lowered = f" {clause.lower()} "
    return any(marker in lowered for marker in _BLEND_RELATION_MARKERS)


def _lowercase_lead(text: str) -> str:
    if not text:
        return text
    if text[0].isalpha():
        return text[0].lower() + text[1:]
    return text


def _is_low_signal_clause(clause: str) -> bool:
    cleaned = " ".join((clause or "").strip().split())
    if not cleaned:
        return True
    lowered = cleaned.lower().rstrip(".?!")
    if lowered in {"that's neat framing", "that's a neat framing", "thats a neat framing"}:
        return True
    if _LOW_SIGNAL_CLAUSE_RE.match(lowered):
        return True
    return False


def _best_clause(clauses: list[str], *, predicate: callable[[str], bool] | None = None) -> str | None:
    filtered = [clause for clause in clauses if predicate(clause)] if predicate is not None else list(clauses)
    if not filtered:
        return None
    high_signal = [clause for clause in filtered if not _is_low_signal_clause(clause)]
    if high_signal:
        filtered = high_signal
    return max(filtered, key=lambda clause: (len(clause.split()), len(clause)))


def _compose_top_candidates(
    candidates: list[dict[str, Any]],
    limit: int = 3,
    *,
    strategy: str | None = None,
) -> tuple[str | None, list[int]]:
    ranked = sorted(candidates, key=lambda item: int(item.get("rank_index") or 9999))
    selected: list[tuple[str, int, str | None]] = []
    seen: set[str] = set()

    for item in ranked:
        rank_index = int(item.get("rank_index") or 0)
        raw = str(item.get("text") or "").strip()
        if not raw:
            continue
        cleaned = _strip_blend_prefix(raw)
        key = _sentence_fingerprint(cleaned)
        if not key or key in seen:
            continue
        seen.add(key)
        selected.append((cleaned, rank_index, item.get("weave_role")))
        if len(selected) >= max(limit, 1):
            break

    if not selected:
        return None, []

    lead = support = sting = None
    rank_indexes: list[int] = []

    if strategy == "mega_bait":
        role_map: dict[str, tuple[str, int]] = {}
        for text, rank, role in selected:
            if role and role not in role_map:
                role_map[role] = (text, rank)
        lead_entry = role_map.get("lead")
        support_entry = role_map.get("support")
        sting_entry = role_map.get("sting")
        if lead_entry or support_entry or sting_entry:
            chosen_entries = [entry for entry in (lead_entry, support_entry, sting_entry) if entry is not None]
            rank_indexes = [rank for _, rank in chosen_entries]
            lead_clauses = _candidate_clauses((lead_entry or chosen_entries[0])[0])
            support_clauses = _candidate_clauses(support_entry[0]) if support_entry is not None else []
            sting_clauses = _candidate_clauses(sting_entry[0]) if sting_entry is not None else []
            lead = _best_clause(lead_clauses, predicate=_looks_like_relation) or _best_clause(lead_clauses) or (lead_entry or chosen_entries[0])[0].rstrip(".?!")
            if support_entry is not None:
                support = _best_clause(support_clauses, predicate=_looks_like_relation) or _best_clause(support_clauses) or support_entry[0].rstrip(".?!")
            if sting_entry is not None:
                sting = _best_clause(sting_clauses, predicate=_looks_like_sting) or _best_clause(sting_clauses) or sting_entry[0].rstrip(".?!")

    if lead is None:
        rank_indexes = [rank for _, rank, _ in selected]
        candidate_clauses = [_candidate_clauses(text) for text, _, _ in selected]
        lead = _best_clause(candidate_clauses[0], predicate=_looks_like_relation) or _best_clause(candidate_clauses[0]) or selected[0][0].rstrip(".?!")
        for clauses in candidate_clauses:
            if sting is None:
                sting = _best_clause(clauses, predicate=_looks_like_sting)
            if support is None:
                candidate_support = _best_clause(clauses, predicate=_looks_like_relation) or _best_clause(clauses)
                if candidate_support and _sentence_fingerprint(candidate_support) != _sentence_fingerprint(lead):
                    support = candidate_support

    if support and _sentence_fingerprint(support) == _sentence_fingerprint(lead):
        support = None
    if sting and _sentence_fingerprint(sting) in {_sentence_fingerprint(lead), _sentence_fingerprint(support or "")}:
        sting = None

    body = lead.rstrip(".?!")
    if support:
        body = f"{body}, and {_lowercase_lead(support.rstrip('.?!'))}"
    if sting:
        final = f"{body}. {sting.rstrip('.?!').capitalize()}."
    else:
        final = f"{body}."
    return final.strip(), rank_indexes


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

    selected_candidate_text = _candidate_text(candidate)
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

    should_combine = bool(combine_top_candidates or resolved_strategy in {"blend_top3", "mega_bait"})
    compose_limit = len(candidate_pool) if resolved_strategy == "mega_bait" else 3
    composed_body, composed_ranks = _compose_top_candidates(candidate_pool, limit=compose_limit, strategy=resolved_strategy) if should_combine else (None, [])
    resolved_body = composed_body or selected_candidate_text
    emitted_body_differs_from_selected_candidate = resolved_body != selected_candidate_text
    selected_candidate_rank_index = int(candidate.get("rank_index") or candidate_rank_index)

    envelope = OutboundReplyEnvelope(
        run_id=run["id"],
        candidate_rank_index=selected_candidate_rank_index,
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
            "selected_candidate_rank_index": selected_candidate_rank_index,
            "selected_candidate_text": selected_candidate_text,
            "emitted_body_differs_from_selected_candidate": emitted_body_differs_from_selected_candidate,
            "combined_top_candidates": should_combine,
            "composition_strategy": resolved_strategy if should_combine else None,
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
