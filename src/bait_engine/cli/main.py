from __future__ import annotations

import argparse
import html
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
import webbrowser

from bait_engine.adapters import (
    build_emit_request,
    build_preview_panel,
    build_reply_envelope,
    get_adapter,
    list_adapters,
    normalize_target,
    recommend_selection_preset,
    render_preview_panel_html,
    validate_target,
)
from bait_engine.analysis import AnalyzeInput, analyze_comment
from bait_engine.core.types import AnalysisResult, TacticalObjective, TacticFamily
from bait_engine.generation import DraftRequest, build_prompt_payload, draft_candidates, draft_candidates_with_provider
from bait_engine.planning import DEFAULT_PERSONAS, build_plan, get_persona, select_persona
from bait_engine.providers import OpenAICompatibleProvider, TextGenerationProvider
from bait_engine.intake import fetch_targets, rank_targets, supported_hunt_sources
from bait_engine.storage import EmitOutboxRecord, IntakeTargetRecord, OutcomeRecord, PanelReviewRecord, RunRepository, build_outcome_scoreboard, build_report, render_report_csv, render_report_markdown, summarize_run, summarize_runs


def _build_provider(
    model: str | None = None,
    base_url: str | None = None,
    timeout_seconds: int = 30,
) -> TextGenerationProvider:
    return OpenAICompatibleProvider(
        model=model,
        base_url=base_url,
        timeout_seconds=timeout_seconds,
    )


def _generate_draft(
    request: DraftRequest,
    heuristic_only: bool = False,
    provider: TextGenerationProvider | None = None,
):
    return draft_candidates(request) if heuristic_only else draft_candidates_with_provider(request, provider=provider)


def _bounded(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _first_recommended_value(values: Any) -> str | None:
    if isinstance(values, list) and values:
        candidate = str(values[0] or "").strip()
        return candidate or None
    return None


def _persona_priors_for_platform(
    repo: RunRepository | None,
    *,
    platform: str,
    prior_days: int = 90,
    min_samples: int = 3,
) -> dict[str, dict[str, Any]]:
    if repo is None:
        return {}
    priors: dict[str, dict[str, Any]] = {}
    for persona_name in sorted(DEFAULT_PERSONAS.keys()):
        reputation = repo.get_persona_reputation(persona_name, platform=platform, days=prior_days)
        samples = int(reputation.get("total_runs") or 0)
        if samples < min_samples:
            continue
        priors[persona_name] = {
            "score": float(reputation.get("reply_rate") or 0.0),
            "confidence": _bounded(samples / max(float(min_samples * 3), 1.0)),
            "sample_count": samples,
        }
    return priors


def _resolve_persona_router_decision(
    analysis: AnalysisResult,
    *,
    requested_persona_name: str,
    platform: str,
    objective_hint: str | None,
    repo: RunRepository | None,
    prior_days: int = 90,
) -> tuple[str, Any | None]:
    normalized = str(requested_persona_name or "").strip().lower()
    if normalized != "auto":
        return requested_persona_name, None

    decision = select_persona(
        analysis,
        platform=platform,
        priors=_persona_priors_for_platform(repo, platform=platform, prior_days=prior_days),
        calibration=repo.persona_router_calibration(platform=platform, objective=objective_hint, days=prior_days)
        if repo is not None
        else None,
    )
    return decision.selected_persona, decision


def _analysis_from_ranked_item(item: dict[str, Any], *, platform_fallback: str = "reddit") -> AnalysisResult:
    analysis_payload = item.get("analysis") if isinstance(item.get("analysis"), dict) else {}
    if isinstance(analysis_payload, dict) and analysis_payload:
        return AnalysisResult.model_validate(analysis_payload)
    target = item.get("target")
    body = str(getattr(target, "body", "") or "")
    subject = str(getattr(target, "subject", "") or "")
    source_text = f"{subject}\n{body}".strip() or body or subject or ""
    platform = str(getattr(target, "platform", "") or platform_fallback)
    return analyze_comment(AnalyzeInput(text=source_text, platform=platform))


def _analysis_from_target_record(target: dict[str, Any]) -> AnalysisResult:
    analysis_payload = target.get("analysis") if isinstance(target.get("analysis"), dict) else {}
    if analysis_payload:
        return AnalysisResult.model_validate(analysis_payload)
    subject = str(target.get("subject") or "")
    body = str(target.get("body") or "")
    source_text = f"{subject}\n{body}".strip() or body or subject
    platform = str(target.get("platform") or "reddit")
    return analyze_comment(AnalyzeInput(text=source_text, platform=platform))


def _apply_lane_prior_to_score(score_block: dict[str, Any], *, lane_prior: dict[str, Any] | None) -> dict[str, Any]:
    base_score = float(score_block.get("score") or 0.0)
    if not lane_prior:
        score_block["effective_score"] = round(base_score, 4)
        return score_block

    prior_score = _bounded(float(lane_prior.get("prior_score") or 0.5))
    confidence = _bounded(float(lane_prior.get("confidence") or 0.0))
    boost = (prior_score - 0.5) * 0.16 * confidence
    effective_score = _bounded(base_score + boost)

    lane_prior_block = dict(lane_prior)
    lane_prior_block["boost"] = round(boost, 4)
    lane_prior_block["effective_score"] = round(effective_score, 4)
    lane_prior_block["base_score"] = round(base_score, 4)

    score_block["lane_prior"] = lane_prior_block
    score_block["effective_score"] = round(effective_score, 4)
    return score_block


def _resolve_hunt_generation_lane(
    item_or_target: dict[str, Any],
    *,
    heuristic_only: bool | None = None,
) -> dict[str, Any]:
    if heuristic_only is True:
        return {
            "lane": "fast",
            "heuristic_only": True,
            "reason": "explicit heuristic-only override",
            "score": None,
        }
    if heuristic_only is False:
        return {
            "lane": "deep",
            "heuristic_only": False,
            "reason": "explicit provider-lane override",
            "score": None,
        }

    score_block = item_or_target.get("score") if isinstance(item_or_target.get("score"), dict) else {}
    score = float(score_block.get("score") or 0.0)
    effective_score = float(score_block.get("effective_score") or score)
    signals = score_block.get("signals") if isinstance(score_block.get("signals"), dict) else {}
    bite_detection = score_block.get("bite_detection") if isinstance(score_block.get("bite_detection"), dict) else {}
    reply_probability = float(signals.get("reply_probability") or 0.0)
    essay_probability = float(signals.get("essay_probability") or 0.0)
    contradiction_signal = float(signals.get("contradiction_signal") or 0.0)
    audience_value = float(signals.get("audience_value") or 0.0)
    bite_qualified = bool(bite_detection.get("qualified")) if bite_detection else True
    bite_score = float(bite_detection.get("score") or 0.0) if bite_detection else None

    deserves_deep_lane = (
        bite_qualified
        and (
            effective_score >= 0.72
            or (effective_score >= 0.64 and reply_probability >= 0.40 and contradiction_signal >= 0.30)
            or (effective_score >= 0.66 and essay_probability >= 0.42)
            or (effective_score >= 0.68 and audience_value >= 0.45)
        )
    )
    if deserves_deep_lane:
        return {
            "lane": "deep",
            "heuristic_only": False,
            "reason": "high-value intake score + bite detection qualified for provider generation",
            "score": round(score, 4),
            "effective_score": round(effective_score, 4),
            "bite_score": round(bite_score, 4) if bite_score is not None else None,
        }
    fast_reason = "default fast lane: heuristic generation until score earns deep lane"
    if not bite_qualified:
        fast_reason = "default fast lane: bite detection not yet qualified"
    return {
        "lane": "fast",
        "heuristic_only": True,
        "reason": fast_reason,
        "score": round(score, 4),
        "effective_score": round(effective_score, 4),
        "bite_score": round(bite_score, 4) if bite_score is not None else None,
    }


def cmd_analyze(text: str, platform: str = "cli") -> dict:
    result = analyze_comment(AnalyzeInput(text=text, platform=platform))
    return result.model_dump(mode="json")


def cmd_plan(text: str, persona_name: str, platform: str = "cli") -> dict:
    analysis = analyze_comment(AnalyzeInput(text=text, platform=platform))
    plan = build_plan(analysis, persona=persona_name)
    return {
        "analysis": analysis.model_dump(mode="json"),
        "plan": plan.model_dump(mode="json"),
    }


def _apply_force_engage_override(analysis) -> None:
    if TacticalObjective.DO_NOT_ENGAGE not in (analysis.recommended_objectives or []):
        return
    analysis.recommended_objectives = [
        TacticalObjective.HOOK,
        TacticalObjective.INFLATE,
        TacticalObjective.COLLAPSE,
    ]
    if not analysis.recommended_tactics:
        analysis.recommended_tactics = [
            TacticFamily.AGREE_AND_ACCELERATE,
            TacticFamily.FAKE_CLARIFICATION,
        ]
    analysis.notes = [
        note
        for note in (analysis.notes or [])
        if "selectivity gate recommends skipping this target" not in note
    ]
    analysis.notes.append("engagement override enabled: bypassed selectivity gate")


def cmd_draft(
    text: str,
    persona_name: str,
    candidate_count: int,
    save: bool = False,
    db_path: str | None = None,
    platform: str = "cli",
    heuristic_only: bool = False,
    model: str | None = None,
    base_url: str | None = None,
    timeout_seconds: int = 30,
    force_engage: bool = False,
    mutation_source: str = "auto",
) -> dict:
    provider = None if heuristic_only else _build_provider(model=model, base_url=base_url, timeout_seconds=timeout_seconds)
    if save:
        repo = RunRepository(db_path)
        stored = repo.create_run_from_text(
            text=text,
            persona_name=persona_name,
            platform=platform,
            candidate_count=candidate_count,
            provider=provider,
            heuristic_only=heuristic_only,
            force_engage=force_engage,
            mutation_source=mutation_source,
        )
        analysis = stored["analysis"]
        plan = stored["plan"]
        request = repo.rebuild_request(stored["id"], candidate_count=candidate_count, mutation_source=mutation_source)
        router_meta = (plan.get("persona_router") if isinstance(plan, dict) else None) or {}
        return {
            "saved": True,
            "run_id": stored["id"],
            "analysis": analysis,
            "plan": plan,
            "selected_persona": stored.get("persona"),
            "persona_scores": router_meta.get("persona_scores") or {},
            "confidence": router_meta.get("confidence"),
            "why_selected": router_meta.get("why_selected") or [],
            "calibration_version": router_meta.get("calibration_version"),
            "calibration_timestamp": router_meta.get("calibration_timestamp"),
            "segment_confidence": router_meta.get("segment_confidence"),
            "segment_key": router_meta.get("segment_key"),
            "prompt_payload": build_prompt_payload(request),
            "draft": {"candidates": stored["candidates"]},
        }

    repo = RunRepository(db_path) if db_path is not None else None
    analysis = analyze_comment(AnalyzeInput(text=text, platform=platform))
    if force_engage:
        _apply_force_engage_override(analysis)
    resolved_persona_name, persona_router = _resolve_persona_router_decision(
        analysis,
        requested_persona_name=persona_name,
        platform=platform,
        objective_hint=_first_recommended_value(analysis.recommended_objectives),
        repo=repo,
    )
    plan = build_plan(analysis, persona=resolved_persona_name, persona_router=persona_router)
    persona = get_persona(resolved_persona_name)
    mutation_seeds = []
    mutation_context = None
    winner_anchors: list[str] = []
    avoid_patterns: list[str] = []
    if mutation_source == "auto" and repo is not None:
        mutation_seeds = repo._select_mutation_seeds(
            persona=persona.name,
            platform=platform,
            tactic=plan.selected_tactic.value if plan.selected_tactic else None,
            objective=plan.selected_objective.value if plan.selected_objective else None,
            limit=min(candidate_count, 3),
        )
        mutation_context, winner_anchors, avoid_patterns = repo._derive_mutation_context(mutation_seeds)
    request = DraftRequest(
        source_text=text,
        plan=plan,
        persona=persona,
        candidate_count=candidate_count,
        mutation_seeds=mutation_seeds,
        mutation_context=mutation_context,
        winner_anchors=winner_anchors,
        avoid_patterns=avoid_patterns,
    )
    draft = _generate_draft(request, heuristic_only=heuristic_only, provider=provider)
    plan_payload = plan.model_dump(mode="json")
    router_meta = plan_payload.get("persona_router") or {}
    return {
        "saved": False,
        "analysis": analysis.model_dump(mode="json"),
        "plan": plan_payload,
        "selected_persona": persona.name,
        "persona_scores": router_meta.get("persona_scores") or {},
        "confidence": router_meta.get("confidence"),
        "why_selected": router_meta.get("why_selected") or [],
        "calibration_version": router_meta.get("calibration_version"),
        "calibration_timestamp": router_meta.get("calibration_timestamp"),
        "segment_confidence": router_meta.get("segment_confidence"),
        "segment_key": router_meta.get("segment_key"),
        "prompt_payload": build_prompt_payload(request),
        "draft": draft.model_dump(mode="json"),
    }


def _serialize_ranked_hunt_item(item: dict[str, Any]) -> dict[str, Any]:
    target = item["target"]
    routing = _resolve_hunt_generation_lane(item)
    persona_router = item.get("persona_router") if isinstance(item.get("persona_router"), dict) else {}
    return {
        "source_driver": target.source_driver,
        "source_item_id": target.source_item_id,
        "platform": target.platform,
        "thread_id": target.thread_id,
        "reply_to_id": target.reply_to_id,
        "author_handle": target.author_handle,
        "subject": target.subject,
        "body": target.body,
        "permalink": target.permalink,
        "context": target.context,
        "metadata": target.metadata,
        "score": item.get("score") or {},
        "analysis": item.get("analysis") or {},
        "selected_persona": item.get("selected_persona"),
        "persona_scores": persona_router.get("persona_scores") or {},
        "confidence": persona_router.get("confidence"),
        "why_selected": persona_router.get("why_selected") or [],
        "calibration_version": persona_router.get("calibration_version"),
        "calibration_timestamp": persona_router.get("calibration_timestamp"),
        "segment_confidence": persona_router.get("segment_confidence"),
        "segment_key": persona_router.get("segment_key"),
        "generation_lane": routing["lane"],
        "generation_reason": routing["reason"],
    }



def _enrich_ranked_items_with_lane_priors(
    ranked_items: list[dict[str, Any]],
    *,
    repo: RunRepository | None,
    persona_name: str | None,
    prior_days: int = 30,
) -> list[dict[str, Any]]:
    if not persona_name:
        return ranked_items

    for item in ranked_items:
        target = item.get("target")
        if target is None:
            continue
        platform = str(getattr(target, "platform", "") or "")
        analysis_model = _analysis_from_ranked_item(item, platform_fallback=platform or "reddit")
        resolved_persona, router_meta = _resolve_persona_router_decision(
            analysis_model,
            requested_persona_name=persona_name,
            platform=platform,
            objective_hint=_first_recommended_value(analysis_model.recommended_objectives),
            repo=repo,
        )
        item["selected_persona"] = resolved_persona
        if router_meta is not None:
            item["persona_router"] = router_meta.model_dump(mode="json")

        if repo is None:
            continue
        analysis = analysis_model.model_dump(mode="json")
        tactic = _first_recommended_value(analysis.get("recommended_tactics"))
        objective = _first_recommended_value(analysis.get("recommended_objectives"))
        lane_prior = repo.get_lane_prior(
            persona=resolved_persona,
            platform=platform,
            tactic=tactic,
            objective=objective,
            days=prior_days,
        )

        score_block = dict(item.get("score") or {})
        item["score"] = _apply_lane_prior_to_score(score_block, lane_prior=lane_prior)

    ranked_items.sort(key=lambda ranked: float((ranked.get("score") or {}).get("effective_score") or (ranked.get("score") or {}).get("score") or 0.0), reverse=True)
    return ranked_items


def _fetch_ranked_hunt_items(
    source: str,
    *,
    subreddit: str | None = None,
    sort: str = "new",
    query: str | None = None,
    limit: int = 25,
    file_path: str | None = None,
    access_token: str | None = None,
    bearer_token: str | None = None,
    user_agent: str | None = None,
    timeout_seconds: float = 15.0,
    db_path: str | None = None,
    persona_name: str | None = None,
    prior_days: int = 30,
) -> list[dict[str, Any]]:
    targets = fetch_targets(
        source,
        subreddit=subreddit,
        sort=sort,
        query=query,
        limit=limit,
        file_path=file_path,
        access_token=access_token,
        bearer_token=bearer_token,
        user_agent=user_agent,
        timeout_seconds=timeout_seconds,
    )
    ranked = rank_targets(targets)
    repo = RunRepository(db_path) if db_path is not None else None
    return _enrich_ranked_items_with_lane_priors(
        ranked,
        repo=repo,
        persona_name=persona_name,
        prior_days=prior_days,
    )



def cmd_hunt_preview(
    source: str,
    *,
    subreddit: str | None = None,
    sort: str = "new",
    query: str | None = None,
    limit: int = 25,
    file_path: str | None = None,
    access_token: str | None = None,
    bearer_token: str | None = None,
    user_agent: str | None = None,
    timeout_seconds: float = 15.0,
    db_path: str | None = None,
    persona_name: str = "dry_midwit_savant",
    prior_days: int = 30,
) -> dict:
    ranked = _fetch_ranked_hunt_items(
        source,
        subreddit=subreddit,
        sort=sort,
        query=query,
        limit=limit,
        file_path=file_path,
        access_token=access_token,
        bearer_token=bearer_token,
        user_agent=user_agent,
        timeout_seconds=timeout_seconds,
        db_path=db_path,
        persona_name=persona_name,
        prior_days=prior_days,
    )
    return {
        "ok": True,
        "source": source,
        "fetched": len(ranked),
        "targets": [_serialize_ranked_hunt_item(item) for item in ranked],
    }



def cmd_hunt_list(
    *,
    db_path: str | None = None,
    limit: int = 100,
    status: str | None = None,
    platform: str | None = None,
) -> dict:
    repo = RunRepository(db_path)
    return {
        "ok": True,
        "targets": repo.list_intake_targets(limit=limit, status=status, platform=platform),
        "status": status,
        "platform": platform,
    }



def cmd_hunt_promote(
    target_id: int,
    *,
    db_path: str | None = None,
    persona_name: str = "dry_midwit_savant",
    candidate_count: int = 5,
    heuristic_only: bool | None = None,
    model: str | None = None,
    base_url: str | None = None,
    timeout_seconds: int = 30,
    stage_emit: bool = True,
    approve_emit: bool = False,
    selection_preset: str | None = "auto",
    selection_strategy: str = "rank",
    tactic: str | None = None,
    objective: str | None = None,
    force: bool = False,
    dispatch_approved: bool = False,
    dispatch_limit: int = 25,
    driver: str = "auto",
    out_dir: str | None = None,
    notes: str | None = None,
) -> dict:
    repo = RunRepository(db_path)
    target = repo.get_intake_target(target_id)
    routing = _resolve_hunt_generation_lane(target, heuristic_only=heuristic_only)
    provider = None if routing["heuristic_only"] else _build_provider(model=model, base_url=base_url, timeout_seconds=timeout_seconds)
    effective_approve = approve_emit or dispatch_approved
    effective_stage = stage_emit or effective_approve
    analysis_model = _analysis_from_target_record(target)
    resolved_persona_name, persona_router = _resolve_persona_router_decision(
        analysis_model,
        requested_persona_name=persona_name,
        platform=str(target.get("platform") or "reddit"),
        objective_hint=_first_recommended_value(analysis_model.recommended_objectives),
        repo=repo,
    )
    promoted = repo.promote_intake_target(
        target_id,
        persona_name=resolved_persona_name,
        candidate_count=candidate_count,
        provider=provider,
        heuristic_only=bool(routing["heuristic_only"]),
        stage_emit=effective_stage,
        approve_emit=effective_approve,
        selection_preset=selection_preset,
        selection_strategy=selection_strategy,
        tactic=tactic,
        objective=objective,
        force=force,
    )
    dispatch = None
    if dispatch_approved:
        dispatch = cmd_dispatch_approved(limit=dispatch_limit, db_path=db_path, driver=driver, out_dir=out_dir, notes=notes)
    router_payload = persona_router.model_dump(mode="json") if persona_router is not None else {}
    return {
        "ok": True,
        "promoted": promoted,
        "dispatch": dispatch,
        "generation_lane": routing,
        "selected_persona": resolved_persona_name,
        "persona_scores": router_payload.get("persona_scores") or {},
        "confidence": router_payload.get("confidence"),
        "why_selected": router_payload.get("why_selected") or [],
        "calibration_version": router_payload.get("calibration_version"),
        "calibration_timestamp": router_payload.get("calibration_timestamp"),
        "segment_confidence": router_payload.get("segment_confidence"),
        "segment_key": router_payload.get("segment_key"),
    }



def cmd_hunt_cycle(
    source: str,
    *,
    db_path: str | None = None,
    subreddit: str | None = None,
    sort: str = "new",
    query: str | None = None,
    limit: int = 25,
    file_path: str | None = None,
    access_token: str | None = None,
    bearer_token: str | None = None,
    user_agent: str | None = None,
    timeout_seconds: float = 15.0,
    save_limit: int | None = None,
    promote_limit: int = 3,
    persona_name: str = "dry_midwit_savant",
    candidate_count: int = 5,
    heuristic_only: bool | None = None,
    model: str | None = None,
    base_url: str | None = None,
    generation_timeout_seconds: int = 30,
    stage_emit: bool = True,
    approve_emit: bool = False,
    selection_preset: str | None = "auto",
    selection_strategy: str = "rank",
    tactic: str | None = None,
    objective: str | None = None,
    dispatch_approved: bool = False,
    dispatch_limit: int = 25,
    driver: str = "auto",
    out_dir: str | None = None,
    notes: str | None = None,
    prior_days: int = 30,
) -> dict:
    repo = RunRepository(db_path)
    ranked = _fetch_ranked_hunt_items(
        source,
        subreddit=subreddit,
        sort=sort,
        query=query,
        limit=limit,
        file_path=file_path,
        access_token=access_token,
        bearer_token=bearer_token,
        user_agent=user_agent,
        timeout_seconds=timeout_seconds,
        db_path=db_path,
        persona_name=persona_name,
        prior_days=prior_days,
    )
    persist_cap = len(ranked) if save_limit is None else max(0, min(len(ranked), int(save_limit)))
    persisted: list[dict[str, Any]] = []
    for item in ranked[:persist_cap]:
        target = item["target"]
        persisted.append(
            repo.upsert_intake_target(
                IntakeTargetRecord(
                    id=None,
                    source_driver=target.source_driver,
                    source_item_id=target.source_item_id,
                    platform=target.platform,
                    thread_id=target.thread_id,
                    reply_to_id=target.reply_to_id,
                    author_handle=target.author_handle,
                    subject=target.subject,
                    body=target.body,
                    permalink=target.permalink,
                    status="new",
                    score_json=json.dumps(item.get("score") or {}, ensure_ascii=False),
                    analysis_json=json.dumps(item.get("analysis") or {}, ensure_ascii=False),
                    context_json=json.dumps(target.context or {}, ensure_ascii=False),
                    metadata_json=json.dumps(target.metadata or {}, ensure_ascii=False),
                )
            )
        )

    provider: TextGenerationProvider | None = None
    effective_approve = approve_emit or dispatch_approved
    effective_stage = stage_emit or effective_approve
    promoted: list[dict[str, Any]] = []
    for item in persisted[: max(0, int(promote_limit))]:
        routing = _resolve_hunt_generation_lane(item, heuristic_only=heuristic_only)
        if not routing["heuristic_only"] and provider is None:
            provider = _build_provider(model=model, base_url=base_url, timeout_seconds=generation_timeout_seconds)
        analysis_model = _analysis_from_target_record(item)
        resolved_persona_name, persona_router = _resolve_persona_router_decision(
            analysis_model,
            requested_persona_name=persona_name,
            platform=str(item.get("platform") or "reddit"),
            objective_hint=_first_recommended_value(analysis_model.recommended_objectives),
            repo=repo,
        )
        promoted_result = repo.promote_intake_target(
            int(item["id"]),
            persona_name=resolved_persona_name,
            candidate_count=candidate_count,
            provider=None if routing["heuristic_only"] else provider,
            heuristic_only=bool(routing["heuristic_only"]),
            stage_emit=effective_stage,
            approve_emit=effective_approve,
            selection_preset=selection_preset,
            selection_strategy=selection_strategy,
            tactic=tactic,
            objective=objective,
        )
        promoted_result["generation_lane"] = routing
        promoted_result["selected_persona"] = resolved_persona_name
        if persona_router is not None:
            router_payload = persona_router.model_dump(mode="json")
            promoted_result["persona_scores"] = router_payload.get("persona_scores") or {}
            promoted_result["confidence"] = router_payload.get("confidence")
            promoted_result["why_selected"] = router_payload.get("why_selected") or []
            promoted_result["calibration_version"] = router_payload.get("calibration_version")
            promoted_result["calibration_timestamp"] = router_payload.get("calibration_timestamp")
            promoted_result["segment_confidence"] = router_payload.get("segment_confidence")
            promoted_result["segment_key"] = router_payload.get("segment_key")
        else:
            promoted_result["persona_scores"] = {}
            promoted_result["confidence"] = None
            promoted_result["why_selected"] = []
            promoted_result["calibration_version"] = None
            promoted_result["calibration_timestamp"] = None
            promoted_result["segment_confidence"] = None
            promoted_result["segment_key"] = None
        promoted.append(promoted_result)

    dispatch = None
    if dispatch_approved:
        dispatch = cmd_dispatch_approved(limit=dispatch_limit, db_path=db_path, driver=driver, out_dir=out_dir, notes=notes)

    return {
        "ok": True,
        "source": source,
        "fetched": len(ranked),
        "saved": len(persisted),
        "promoted": len(promoted),
        "targets": persisted,
        "top_ranked": [_serialize_ranked_hunt_item(item) for item in ranked[: min(5, len(ranked))]],
        "promotion_results": promoted,
        "dispatch": dispatch,
        "automation": {
            "stage_emit": effective_stage,
            "approve_emit": effective_approve,
            "dispatch_approved": dispatch_approved,
            "heuristic_only": heuristic_only,
        },
    }



def cmd_mutate_winners(
    *,
    db_path: str | None = None,
    winner_limit: int = 5,
    variants_per_winner: int = 5,
    persona: str | None = None,
    platform: str | None = None,
    tactic: str | None = None,
    objective: str | None = None,
    days: int = 30,
    require_reply: bool = True,
    strategy: str = "controlled_v1",
) -> dict:
    repo = RunRepository(db_path)
    return repo.mutate_top_winners(
        winner_limit=winner_limit,
        variants_per_winner=variants_per_winner,
        persona=persona,
        platform=platform,
        tactic=tactic,
        objective=objective,
        days=days,
        require_reply=require_reply,
        strategy=strategy,
    )


def cmd_mutate_run(
    run_id: int,
    *,
    db_path: str | None = None,
    variants_per_winner: int = 5,
    strategy: str = "controlled_v1",
) -> dict:
    repo = RunRepository(db_path)
    return repo.mutate_run(run_id, variants_per_winner=variants_per_winner, strategy=strategy)


def cmd_mutation_report(
    *,
    db_path: str | None = None,
    limit: int = 250,
    persona: str | None = None,
    platform: str | None = None,
    tactic: str | None = None,
    objective: str | None = None,
    status: str | None = None,
) -> dict:
    repo = RunRepository(db_path)
    return repo.mutation_report(
        limit=limit,
        persona=persona,
        platform=platform,
        tactic=tactic,
        objective=objective,
        status=status,
    )


def cmd_hunt_run(
    source: str,
    *,
    db_path: str | None = None,
    subreddit: str | None = None,
    sort: str = "new",
    query: str | None = None,
    limit: int = 25,
    file_path: str | None = None,
    access_token: str | None = None,
    bearer_token: str | None = None,
    user_agent: str | None = None,
    timeout_seconds: float = 15.0,
    save_limit: int | None = None,
    promote_limit: int = 3,
    persona_name: str = "dry_midwit_savant",
    candidate_count: int = 5,
    heuristic_only: bool | None = None,
    model: str | None = None,
    base_url: str | None = None,
    generation_timeout_seconds: int = 30,
    stage_emit: bool = True,
    approve_emit: bool = False,
    selection_preset: str | None = "auto",
    selection_strategy: str = "rank",
    tactic: str | None = None,
    objective: str | None = None,
    dispatch_approved: bool = False,
    dispatch_limit: int = 25,
    driver: str = "auto",
    out_dir: str | None = None,
    notes: str | None = None,
    prior_days: int = 30,
    interval_seconds: float = 60.0,
    max_cycles: int = 0,
) -> dict:
    cycles: list[dict[str, Any]] = []
    cycle_count = 0
    while True:
        cycles.append(
            cmd_hunt_cycle(
                source,
                db_path=db_path,
                subreddit=subreddit,
                sort=sort,
                query=query,
                limit=limit,
                file_path=file_path,
                access_token=access_token,
                bearer_token=bearer_token,
                user_agent=user_agent,
                timeout_seconds=timeout_seconds,
                save_limit=save_limit,
                promote_limit=promote_limit,
                persona_name=persona_name,
                candidate_count=candidate_count,
                heuristic_only=heuristic_only,
                model=model,
                base_url=base_url,
                generation_timeout_seconds=generation_timeout_seconds,
                stage_emit=stage_emit,
                approve_emit=approve_emit,
                selection_preset=selection_preset,
                selection_strategy=selection_strategy,
                tactic=tactic,
                objective=objective,
                dispatch_approved=dispatch_approved,
                dispatch_limit=dispatch_limit,
                driver=driver,
                out_dir=out_dir,
                notes=notes,
                prior_days=prior_days,
            )
        )
        cycle_count += 1
        if max_cycles > 0 and cycle_count >= max_cycles:
            break
        if interval_seconds > 0:
            time.sleep(interval_seconds)
    return {
        "ok": True,
        "cycles": cycles,
        "cycle_count": cycle_count,
        "interval_seconds": interval_seconds,
        "source": source,
    }


def cmd_runs(limit: int, db_path: str | None = None) -> dict:
    repo = RunRepository(db_path)
    return {"runs": repo.list_runs(limit=limit)}


def cmd_personas() -> dict:
    return {
        "personas": [
            {
                "name": persona.name,
                "length_band_words": list(persona.length_band_words),
                "tone_tags": persona.tone_tags,
                "jargon_ceiling": persona.jargon_ceiling,
                "absurdity_tolerance": persona.absurdity_tolerance,
                "calmness_preference": persona.calmness_preference,
                "punctuation_style": persona.punctuation_style,
                "pressure_profile": persona.pressure_profile,
                "escalation_cues": persona.escalation_cues,
                "forbidden_tactics": [tactic.value for tactic in persona.forbidden_tactics],
            }
            for persona in DEFAULT_PERSONAS.values()
        ]
    }


def cmd_show_run(run_id: int, db_path: str | None = None) -> dict:
    repo = RunRepository(db_path)
    return repo.get_run(run_id)


def _append_response_to_source_text(source_text: str, response_text: str) -> str:
    base = str(source_text or "").strip()
    reply = str(response_text or "").strip()
    if not reply:
        return base
    if not base:
        return f"Incoming response:\n{reply}"
    return f"{base}\n\nIncoming response:\n{reply}"


def cmd_replay(
    run_id: int,
    candidate_count: int | None = None,
    db_path: str | None = None,
    heuristic_only: bool = False,
    model: str | None = None,
    base_url: str | None = None,
    timeout_seconds: int = 30,
    mutation_source: str = "auto",
    response_text: str | None = None,
) -> dict:
    repo = RunRepository(db_path)
    stored = repo.get_run(run_id)
    request = repo.rebuild_request(run_id, candidate_count=candidate_count, mutation_source=mutation_source)
    response_text_value = str(response_text or "").strip() or None
    if response_text_value is not None:
        request = request.model_copy(
            update={"source_text": _append_response_to_source_text(str(stored.get("source_text") or ""), response_text_value)}
        )
    provider = None if heuristic_only else _build_provider(model=model, base_url=base_url, timeout_seconds=timeout_seconds)
    draft = _generate_draft(request, heuristic_only=heuristic_only, provider=provider)
    return {
        "run_id": run_id,
        "response_text": response_text_value,
        "source_text": request.source_text,
        "persona": stored["persona"],
        "plan": request.plan.model_dump(mode="json"),
        "prompt_payload": build_prompt_payload(request),
        "draft": draft.model_dump(mode="json"),
    }


def cmd_autopsy(run_id: int, db_path: str | None = None) -> dict:
    repo = RunRepository(db_path)
    return summarize_run(repo.get_run(run_id))


def cmd_autopsy_many(
    limit: int = 20,
    db_path: str | None = None,
    persona: str | None = None,
    platform: str | None = None,
    verdict: str | None = None,
) -> dict:
    repo = RunRepository(db_path)
    runs = repo.list_full_runs(limit=limit)
    summaries = summarize_runs(runs)
    if persona is not None:
        summaries = [summary for summary in summaries if summary.get("persona") == persona]
    if platform is not None:
        summaries = [summary for summary in summaries if summary.get("platform") == platform]
    if verdict is not None:
        summaries = [summary for summary in summaries if summary.get("verdict") == verdict]
    return {"filters": {"persona": persona, "platform": platform, "verdict": verdict}, "runs": summaries}


def _filter_runs_since_hours(runs: list[dict[str, Any]], since_hours: float | None) -> list[dict[str, Any]]:
    if since_hours is None:
        return runs
    window_seconds = max(0.0, float(since_hours)) * 3600.0
    if window_seconds <= 0.0:
        return runs
    now = datetime.now(timezone.utc)
    kept: list[dict[str, Any]] = []
    for run in runs:
        created_raw = run.get("created_at")
        if not created_raw:
            continue
        try:
            created = datetime.strptime(str(created_raw), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if (now - created).total_seconds() <= window_seconds:
            kept.append(run)
    return kept


def cmd_scoreboard(
    limit: int = 100,
    db_path: str | None = None,
    persona: str | None = None,
    platform: str | None = None,
    verdict: str | None = None,
    since_hours: float | None = None,
) -> dict:
    repo = RunRepository(db_path)
    runs = _filter_runs_since_hours(repo.list_full_runs(limit=limit), since_hours)
    return build_outcome_scoreboard(runs, persona=persona, platform=platform, verdict=verdict)


def cmd_report(
    limit: int = 100,
    section_limit: int = 5,
    db_path: str | None = None,
    persona: str | None = None,
    platform: str | None = None,
    verdict: str | None = None,
    since_hours: float | None = None,
) -> dict:
    repo = RunRepository(db_path)
    runs = _filter_runs_since_hours(repo.list_full_runs(limit=limit), since_hours)
    return build_report(runs, limit_per_section=section_limit, persona=persona, platform=platform, verdict=verdict)


def cmd_report_markdown(
    limit: int = 100,
    section_limit: int = 5,
    db_path: str | None = None,
    persona: str | None = None,
    platform: str | None = None,
    verdict: str | None = None,
    since_hours: float | None = None,
    out_path: str | None = None,
) -> dict:
    report = cmd_report(
        limit=limit,
        section_limit=section_limit,
        db_path=db_path,
        persona=persona,
        platform=platform,
        verdict=verdict,
        since_hours=since_hours,
    )
    markdown = render_report_markdown(report)
    result = {"markdown": markdown}
    if out_path is not None:
        path = Path(out_path)
        path.write_text(markdown, encoding="utf-8")
        result["path"] = str(path)
    return result


def cmd_report_csv(
    limit: int = 100,
    section_limit: int = 5,
    db_path: str | None = None,
    persona: str | None = None,
    platform: str | None = None,
    verdict: str | None = None,
    since_hours: float | None = None,
    out_path: str | None = None,
) -> dict:
    report = cmd_report(
        limit=limit,
        section_limit=section_limit,
        db_path=db_path,
        persona=persona,
        platform=platform,
        verdict=verdict,
        since_hours=since_hours,
    )
    csv_text = render_report_csv(report)
    result = {"csv": csv_text}
    if out_path is not None:
        path = Path(out_path)
        path.write_text(csv_text, encoding="utf-8")
        result["path"] = str(path)
    return result


def cmd_operator_status(db_path: str | None = None) -> dict:
    repo = RunRepository(db_path)
    return repo.operator_status_summary()


def cmd_preflight(
    db_path: str | None = None,
    dead_letter_fail_threshold: int = 10,
    waiting_retry_fail_threshold: int = 50,
    critical_alert_fail_threshold: int = 1,
) -> dict:
    repo = RunRepository(db_path)
    return repo.preflight_autopilot_checklist(
        dead_letter_fail_threshold=dead_letter_fail_threshold,
        waiting_retry_fail_threshold=waiting_retry_fail_threshold,
        critical_alert_fail_threshold=critical_alert_fail_threshold,
    )


def cmd_adapters() -> dict:
    return {"adapters": list_adapters()}


def cmd_adapter(platform: str) -> dict:
    return get_adapter(platform)


def cmd_context_preview(platform: str, thread_id: str, subject: str | None = None, messages_json: str | None = None) -> dict:
    from bait_engine.adapters import InboundThreadContext

    payload = {
        "platform": platform,
        "thread_id": thread_id,
        "subject": subject,
        "messages": json.loads(messages_json) if messages_json else [],
    }
    context = InboundThreadContext.model_validate(payload)
    return {"context": context.model_dump(mode="json")}


def cmd_target_preview(
    platform: str,
    thread_id: str | None = None,
    reply_to_id: str | None = None,
    author_handle: str | None = None,
) -> dict:
    target = normalize_target(platform, thread_id=thread_id, reply_to_id=reply_to_id, author_handle=author_handle)
    validate_target(target)
    return {"target": target.model_dump(mode="json")}


def cmd_adapter_preview(
    run_id: int,
    candidate_rank_index: int = 1,
    selection_strategy: str = "rank",
    selection_preset: str | None = None,
    tactic: str | None = None,
    objective: str | None = None,
    db_path: str | None = None,
    thread_id: str | None = None,
    reply_to_id: str | None = None,
    author_handle: str | None = None,
    context_json: str | None = None,
) -> dict:
    repo = RunRepository(db_path)
    run = repo.get_run(run_id)
    reputation_data = repo.get_persona_reputation(str(run.get("persona") or ""), str(run.get("platform") or ""))
    return build_reply_envelope(
        run,
        candidate_rank_index=candidate_rank_index,
        selection_strategy=selection_strategy,
        selection_preset=selection_preset,
        tactic=tactic,
        objective=objective,
        thread_id=thread_id,
        reply_to_id=reply_to_id,
        author_handle=author_handle,
        context=json.loads(context_json) if context_json else None,
        reputation_data=reputation_data,
    )


def cmd_recommend_preset(platform: str, context_json: str | None = None) -> dict:
    from bait_engine.adapters import InboundThreadContext

    context = InboundThreadContext.model_validate(json.loads(context_json)) if context_json else None
    return {"recommendation": recommend_selection_preset(platform, context)}


def cmd_emit_preview(
    run_id: int,
    candidate_rank_index: int = 1,
    selection_strategy: str = "rank",
    selection_preset: str | None = None,
    tactic: str | None = None,
    objective: str | None = None,
    db_path: str | None = None,
    thread_id: str | None = None,
    reply_to_id: str | None = None,
    author_handle: str | None = None,
    context_json: str | None = None,
) -> dict:
    envelope = cmd_adapter_preview(
        run_id,
        candidate_rank_index=candidate_rank_index,
        selection_strategy=selection_strategy,
        selection_preset=selection_preset,
        tactic=tactic,
        objective=objective,
        db_path=db_path,
        thread_id=thread_id,
        reply_to_id=reply_to_id,
        author_handle=author_handle,
        context_json=context_json,
    )
    return {"emit_request": build_emit_request(envelope), "envelope": envelope}


def _resolve_panel_run_id(run_id: int | str | None, db_path: str | None = None) -> int:
    if run_id is not None:
        return int(run_id)
    repo = RunRepository(db_path)
    latest_runs = repo.list_runs(limit=1)
    if not latest_runs:
        raise KeyError("no runs available")
    return int(latest_runs[0]["id"])


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _default_daemon_db_path(db_path: str | None = None) -> str:
    return str(Path(db_path).expanduser().resolve()) if db_path else str((_repo_root() / ".data" / "bait-engine.db").resolve())


def _launch_agents_dir(path: str | None = None) -> Path:
    return Path(path).expanduser().resolve() if path else (Path.home() / "Library" / "LaunchAgents")


def _daemon_log_dir(path: str | None = None) -> Path:
    return Path(path).expanduser().resolve() if path else (Path.home() / "Library" / "Logs" / "bait-engine-v2")


def _default_dispatch_dir(path: str | None = None) -> str:
    return str(Path(path).expanduser().resolve()) if path else str(RunRepository.default_dispatch_dir())


def _supported_dispatch_drivers() -> tuple[str, ...]:
    drivers = list(RunRepository.supported_dispatch_drivers())
    if "auto" not in drivers:
        drivers.insert(0, "auto")
    return tuple(drivers)


def Boolean(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if str(v).lower() in ("true", "1", "yes", "on"):
        return True
    return False


def _build_daemon_payload(
    db_path: str | None = None,
    host: str = "127.0.0.1",
    port: int = 8765,
    label: str | None = None,
    launch_agents_dir: str | None = None,
    log_dir: str | None = None,
    mode: str = "panel",
    dispatch_limit: int = 25,
    driver: str = "auto",
    out_dir: str | None = None,
    interval_seconds: float = 30.0,
    max_cycles: int = 0,
    include_failed_redrive: bool = False,
    redrive_limit: int = 10,
    min_failed_age_seconds: float = 0.0,
    max_actions_per_hour: int = 0,
    max_actions_per_day: int = 0,
    min_seconds_between_actions: float = 0.0,
    quiet_hours_start: int | None = None,
    quiet_hours_end: int | None = None,
) -> dict[str, Any]:
    repo_root = _repo_root()
    launch_dir = _launch_agents_dir(launch_agents_dir)
    logs_dir = _daemon_log_dir(log_dir)
    effective_db_path = _default_daemon_db_path(db_path)
    daemon_mode = str(mode or "panel")
    if daemon_mode not in {"panel", "worker"}:
        raise ValueError(f"unknown daemon mode: {daemon_mode}")
    supported_drivers = _supported_dispatch_drivers()
    failed_redrive_enabled = bool(include_failed_redrive)
    effective_max_cycles = max(0, int(max_cycles))
    effective_redrive_limit = max(1, int(redrive_limit))
    effective_min_failed_age_seconds = max(0.0, float(min_failed_age_seconds))
    effective_label = label or ("ai.bait-engine.worker" if daemon_mode == "worker" else "ai.bait-engine.panel")
    plist_path = launch_dir / f"{effective_label}.plist"
    stdout_path = logs_dir / f"{daemon_mode}.stdout.log"
    stderr_path = logs_dir / f"{daemon_mode}.stderr.log"
    program_args = [
        sys.executable,
        "-m",
        "bait_engine.cli.main",
        "--db",
        effective_db_path,
    ]
    if daemon_mode == "worker":
        program_args.extend(
            [
                "worker-run",
                "--dispatch-limit",
                str(dispatch_limit),
                "--driver",
                driver,
                "--interval-seconds",
                str(interval_seconds),
                "--max-cycles",
                str(effective_max_cycles),
                "--redrive-limit",
                str(effective_redrive_limit),
                "--min-failed-age-seconds",
                str(effective_min_failed_age_seconds),
                "--max-actions-per-hour",
                str(max(0, int(max_actions_per_hour))),
                "--max-actions-per-day",
                str(max(0, int(max_actions_per_day))),
                "--min-seconds-between-actions",
                str(max(0.0, float(min_seconds_between_actions))),
            ]
        )
        if quiet_hours_start is not None:
            program_args.extend(["--quiet-hours-start", str(int(quiet_hours_start) % 24)])
        if quiet_hours_end is not None:
            program_args.extend(["--quiet-hours-end", str(int(quiet_hours_end) % 24)])
        if failed_redrive_enabled:
            program_args.append("--include-failed-redrive")
        if out_dir is not None:
            program_args.extend(["--out-dir", _default_dispatch_dir(out_dir)])
    else:
        program_args.extend(
            [
                "panel-serve",
                "--host",
                host,
                "--port",
                str(port),
            ]
        )
    env = {"PYTHONPATH": str((repo_root / "src").resolve())}
    daemon_warnings: list[str] = []
    if daemon_mode == "worker":
        if effective_max_cycles > 0:
            daemon_warnings.append("max_cycles is bounded, but LaunchAgent KeepAlive will relaunch the worker after exit")
        if float(interval_seconds) <= 0:
            daemon_warnings.append("interval_seconds <= 0 will create a tight worker loop")
        elif float(interval_seconds) < 5:
            daemon_warnings.append("interval_seconds < 5s may be too aggressive for a login daemon")
        if driver not in supported_drivers:
            daemon_warnings.append(f"driver '{driver}' is unknown; worker-run will fail until a supported driver is configured")
        if int(dispatch_limit) > 100:
            daemon_warnings.append("dispatch_limit > 100 may create oversized worker bursts")
        if failed_redrive_enabled and effective_min_failed_age_seconds <= 0:
            daemon_warnings.append("failed redrive is enabled with 0s cooldown, so the same failed head may retry every cycle")
    plist = f"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">
<plist version=\"1.0\">
<dict>
  <key>Label</key>
  <string>{html.escape(effective_label)}</string>
  <key>ProgramArguments</key>
  <array>
    {''.join(f'<string>{html.escape(arg)}</string>' for arg in program_args)}
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PYTHONPATH</key>
    <string>{html.escape(env['PYTHONPATH'])}</string>
  </dict>
  <key>WorkingDirectory</key>
  <string>{html.escape(str(repo_root))}</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>{html.escape(str(stdout_path))}</string>
  <key>StandardErrorPath</key>
  <string>{html.escape(str(stderr_path))}</string>
</dict>
</plist>
"""
    loaded = False
    launchctl_error = None
    launchctl_bin = shutil.which("launchctl")
    if launchctl_bin and plist_path.exists():
        domain_target = f"gui/{os.getuid()}/{effective_label}"
        result = subprocess.run([launchctl_bin, "print", domain_target], capture_output=True, text=True)
        loaded = result.returncode == 0
        if result.returncode != 0 and result.stderr:
            launchctl_error = result.stderr.strip()
    return {
        "label": effective_label,
        "mode": daemon_mode,
        "host": host,
        "port": port,
        "server_url": f"http://{host}:{port}" if daemon_mode == "panel" else None,
        "dispatch_limit": dispatch_limit,
        "driver": driver,
        "out_dir": _default_dispatch_dir(out_dir) if out_dir is not None else _default_dispatch_dir(),
        "interval_seconds": interval_seconds,
        "max_cycles": effective_max_cycles,
        "include_failed_redrive": failed_redrive_enabled,
        "redrive_limit": effective_redrive_limit,
        "min_failed_age_seconds": effective_min_failed_age_seconds,
        "max_actions_per_hour": max(0, int(max_actions_per_hour)),
        "max_actions_per_day": max(0, int(max_actions_per_day)),
        "min_seconds_between_actions": max(0.0, float(min_seconds_between_actions)),
        "quiet_hours_start": None if quiet_hours_start is None else int(quiet_hours_start) % 24,
        "quiet_hours_end": None if quiet_hours_end is None else int(quiet_hours_end) % 24,
        "repo_root": str(repo_root),
        "db_path": effective_db_path,
        "launch_agents_dir": str(launch_dir),
        "plist_path": str(plist_path),
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "program_args": program_args,
        "environment": env,
        "daemon_warnings": daemon_warnings,
        "supported_drivers": list(supported_drivers),
        "plist": plist,
        "installed": plist_path.exists(),
        "loaded": loaded,
        "launchctl_error": launchctl_error,
    }


def cmd_daemon_manage(
    action: str,
    db_path: str | None = None,
    host: str = "127.0.0.1",
    port: int = 8765,
    label: str | None = None,
    launch_agents_dir: str | None = None,
    log_dir: str | None = None,
    load: bool = False,
    unload: bool = False,
    mode: str = "panel",
    dispatch_limit: int = 25,
    driver: str = "auto",
    out_dir: str | None = None,
    interval_seconds: float = 30.0,
    max_cycles: int = 0,
    include_failed_redrive: bool = False,
    redrive_limit: int = 10,
    min_failed_age_seconds: float = 0.0,
    max_actions_per_hour: int = 0,
    max_actions_per_day: int = 0,
    min_seconds_between_actions: float = 0.0,
    quiet_hours_start: int | None = None,
    quiet_hours_end: int | None = None,
) -> dict[str, Any]:
    payload = _build_daemon_payload(
        db_path=db_path,
        host=host,
        port=port,
        label=label,
        launch_agents_dir=launch_agents_dir,
        log_dir=log_dir,
        mode=mode,
        dispatch_limit=dispatch_limit,
        driver=driver,
        out_dir=out_dir,
        interval_seconds=interval_seconds,
        max_cycles=max_cycles,
        include_failed_redrive=include_failed_redrive,
        redrive_limit=redrive_limit,
        min_failed_age_seconds=min_failed_age_seconds,
        max_actions_per_hour=max_actions_per_hour,
        max_actions_per_day=max_actions_per_day,
        min_seconds_between_actions=min_seconds_between_actions,
        quiet_hours_start=quiet_hours_start,
        quiet_hours_end=quiet_hours_end,
    )
    plist_path = Path(payload["plist_path"])
    logs_dir = Path(payload["stdout_path"]).parent
    launch_dir = Path(payload["launch_agents_dir"])
    launchctl_bin = shutil.which("launchctl")

    if action == "status":
        return payload
    if action == "install":
        launch_dir.mkdir(parents=True, exist_ok=True)
        logs_dir.mkdir(parents=True, exist_ok=True)
        plist_path.write_text(payload["plist"], encoding="utf-8")
        if load and launchctl_bin:
            subprocess.run([launchctl_bin, "unload", str(plist_path)], capture_output=True, text=True)
            subprocess.run([launchctl_bin, "load", str(plist_path)], capture_output=True, text=True, check=False)
        return _build_daemon_payload(
            db_path=db_path,
            host=host,
            port=port,
            label=label,
            launch_agents_dir=launch_agents_dir,
            log_dir=log_dir,
            mode=mode,
            dispatch_limit=dispatch_limit,
            driver=driver,
            out_dir=out_dir,
            interval_seconds=interval_seconds,
            max_cycles=max_cycles,
            include_failed_redrive=include_failed_redrive,
            redrive_limit=redrive_limit,
            min_failed_age_seconds=min_failed_age_seconds,
            max_actions_per_hour=max_actions_per_hour,
            max_actions_per_day=max_actions_per_day,
            min_seconds_between_actions=min_seconds_between_actions,
            quiet_hours_start=quiet_hours_start,
            quiet_hours_end=quiet_hours_end,
        )
    if action == "uninstall":
        if unload and launchctl_bin and plist_path.exists():
            subprocess.run([launchctl_bin, "unload", str(plist_path)], capture_output=True, text=True, check=False)
        if plist_path.exists():
            plist_path.unlink()
        return _build_daemon_payload(
            db_path=db_path,
            host=host,
            port=port,
            label=label,
            launch_agents_dir=launch_agents_dir,
            log_dir=log_dir,
            mode=mode,
            dispatch_limit=dispatch_limit,
            driver=driver,
            out_dir=out_dir,
            interval_seconds=interval_seconds,
            max_cycles=max_cycles,
            include_failed_redrive=include_failed_redrive,
            redrive_limit=redrive_limit,
            min_failed_age_seconds=min_failed_age_seconds,
            max_actions_per_hour=max_actions_per_hour,
            max_actions_per_day=max_actions_per_day,
            min_seconds_between_actions=min_seconds_between_actions,
            quiet_hours_start=quiet_hours_start,
            quiet_hours_end=quiet_hours_end,
        )
    raise ValueError(f"unknown daemon action: {action}")


def _build_dashboard_payload(
    db_path: str | None = None,
    active_run_id: int | None = None,
    limit: int = 20,
    outbox_status: str | None = None,
) -> dict[str, Any]:
    repo = RunRepository(db_path)
    runs = repo.list_full_runs(limit=limit)
    latest_run_id = int(runs[0]["id"]) if runs else None
    scoreboard = build_outcome_scoreboard(runs)
    router_metrics = scoreboard.get("router_metrics") if isinstance(scoreboard, dict) else {}
    outbox = repo.list_emit_outbox(limit=limit, status=outbox_status)
    outbox_all = repo.list_emit_outbox(limit=500)
    dispatches = repo.list_emit_dispatches(limit=20)
    daemon = _build_daemon_payload(db_path=db_path)
    outbox_counts: dict[str, int] = {}
    for item in outbox_all:
        key = str(item.get("status") or "unknown")
        outbox_counts[key] = outbox_counts.get(key, 0) + 1
    outbox_counts["all"] = len(outbox_all)

    retry_queue = repo.get_retry_queue_summary(limit=100)
    mutation_variants = repo.list_mutation_variants(limit=30)

    execution_metrics = repo.get_execution_metrics()
    driver_requirements = {d: repo.driver_requirements(d) for d in repo.supported_dispatch_drivers()}

    # Reputation Engine Integration
    reputation = repo.get_persona_reputation("dry_midwit_savant", "reddit")

    reputation_json = json.dumps(reputation)

    return {
        "runs": runs,
        "active_run_id": active_run_id if active_run_id is not None else latest_run_id,
        "latest_run_id": latest_run_id,
        "total_runs": len(runs),
        "default_persona": "dry_midwit_savant",
        "default_platform": "reddit",
        "emit_outbox": outbox,
        "emit_dispatches": dispatches,
        "pending_emit_count": sum(1 for item in outbox_all if item.get("status") == "staged"),
        "dispatch_count": sum(1 for item in outbox_all if item.get("status") == "dispatched"),
        "outbox_status": outbox_status,
        "outbox_counts": outbox_counts,
        "dispatch_dir": _default_dispatch_dir(),
        "daemon": daemon,
        "execution_metrics": execution_metrics,
        "driver_requirements": driver_requirements,
        "reputation": reputation,
        "reputation_json": reputation_json,
        "retry_queue": retry_queue,
        "mutation_variants": mutation_variants,
        "scoreboard": scoreboard,
        "router_metrics": router_metrics or {},
        "override_audit": (router_metrics or {}).get("override_audit") or {},
    }


def _render_dashboard_html(payload: dict[str, Any], base_url: str) -> str:
    runs = payload.get("runs") or []
    active_run_id = payload.get("active_run_id")
    latest_run_id = payload.get("latest_run_id")
    default_persona_name = str(payload.get("default_persona") or "dry_midwit_savant")
    default_persona = html.escape(default_persona_name)
    default_platform = html.escape(payload.get("default_platform") or "reddit")
    persona_options_html = "".join(
        f'<option value="{html.escape(name)}"{" selected" if name == default_persona_name else ""}>{html.escape(name)}</option>'
        for name in sorted(DEFAULT_PERSONAS.keys())
    )
    pending_emit_count = int(payload.get("pending_emit_count") or 0)
    dispatch_count = int(payload.get("dispatch_count") or 0)
    outbox = payload.get("emit_outbox") or []
    dispatches = payload.get("emit_dispatches") or []
    outbox_status = payload.get("outbox_status")
    outbox_counts = payload.get("outbox_counts") or {}
    dispatch_dir = html.escape(payload.get("dispatch_dir") or "")
    daemon = payload.get("daemon") or {}
    metrics = payload.get("execution_metrics") or {}
    reputation = payload.get("reputation") or {}
    driver_requirements_json = json.dumps(payload.get("driver_requirements") or {})
    router_metrics = payload.get("router_metrics") or {}
    confidence_distribution = router_metrics.get("confidence_distribution") or {}

    total_dispatches = metrics.get("total_dispatches", 0)
    success_rate = metrics.get("success_rate_percent", 0)
    rep_rate = round(reputation.get("reply_rate", 0) * 100, 1) if reputation else 0
    persona_label = html.escape(reputation.get("persona", "dry_midwit_savant"))
    auto_pick_accuracy = round(float(router_metrics.get("auto_pick_accuracy") or 0.0) * 100, 1)
    persona_drift = round(float(router_metrics.get("persona_drift") or 0.0) * 100, 1)

    status_bar_html = f"""
    <div class="status-bar">
        <div><span class="small">MISSION HEALTH (7D)</span><br><b style="font-size:1.3rem; color:{'#8effa3' if success_rate > 90 else '#ff8e8e'}">{success_rate}%</b></div>
        <div><span class="small">REPUTATION: {persona_label}</span><br><b style="font-size:1.3rem; color:#ffd700;">{rep_rate}% reply rate</b></div>
        <div><span class="small">THROUGHPUT (7D)</span><br><b style="font-size:1.3rem;">{metrics.get('delivered_count', 0)}</b> delivered</div>
        <div style="margin-left:auto; text-align:right;"><span class="small">PENDING EMITS</span><br><b style="font-size:1.3rem; color:#8ec5ff;">{pending_emit_count}</b></div>
    </div>
    """
    supported_drivers_list = daemon.get("supported_drivers") or _supported_dispatch_drivers()
    supported_drivers = [str(item) for item in supported_drivers_list]
    current_driver = str(daemon.get("driver") or "auto")
    if current_driver not in supported_drivers:
        supported_drivers.append(current_driver)
    driver_options_html = "".join(
        f'<option value="{html.escape(name)}"{" selected" if name == current_driver else ""}>{html.escape(name)}</option>'
        for name in supported_drivers
    )
    daemon_state = "loaded" if daemon.get("loaded") else ("installed" if daemon.get("installed") else "not installed")

    def _js_call_arg(value: Any) -> str:
        return html.escape(json.dumps(value), quote=True)

    def _dashboard_blend_preview(run_row: dict[str, Any]) -> dict[str, Any] | None:
        try:
            envelope = build_reply_envelope(run_row, selection_strategy="blend_top3", allow_incomplete_target=True)
            body = str(envelope.get("body") or "").strip()
            if not body:
                return None
            metadata = envelope.get("metadata") or {}
            rank_indexes = [int(item) for item in metadata.get("combined_candidate_rank_indexes") or []]
            return {"body": body, "rank_indexes": rank_indexes}
        except (KeyError, ValueError):
            # Dashboard preview fallback for unsupported platform adapters (e.g., custom ingest labels)
            candidates = sorted(
                list(run_row.get("candidates") or []),
                key=lambda item: int(item.get("rank_index") or 9999),
            )
            if not candidates:
                return None
            body_parts: list[str] = []
            ranks: list[int] = []
            seen: set[str] = set()
            for candidate in candidates:
                text = str(candidate.get("text") or "").strip()
                rank = int(candidate.get("rank_index") or 0)
                if not text:
                    continue
                key = " ".join(text.lower().split())
                if key in seen:
                    continue
                seen.add(key)
                body_parts.append(text)
                if rank:
                    ranks.append(rank)
                if len(body_parts) >= 3:
                    break
            if not body_parts:
                return None
            return {"body": " ".join(body_parts).strip(), "rank_indexes": ranks}

    items = []
    for run in runs:
        run_id = run.get("id")
        run_id_value = int(run_id or 0)
        source = html.escape((run.get("source_text") or "")[:140])
        persona = html.escape(run.get("persona") or "unknown")
        platform = html.escape(run.get("platform") or "unknown")
        badge = " ⭐ latest" if run_id == latest_run_id else ""
        active = " active" if run_id == active_run_id else ""

        candidates = run.get("candidates") or []
        top_candidates = candidates[:3]
        run_persona_js = _js_call_arg(run.get("persona") or "dry_midwit_savant")
        run_platform_js = _js_call_arg(run.get("platform") or "reddit")
        run_id_js = _js_call_arg(run_id_value)
        blend_preview = _dashboard_blend_preview(run) if top_candidates else None
        if blend_preview is not None:
            rank_label = " / ".join(f"#{rank}" for rank in blend_preview["rank_indexes"]) if blend_preview["rank_indexes"] else "top 3"
            blend_body = str(blend_preview["body"])
            response_block = (
                '<div class="response"><span class="small">Combined response (blend_top3)</span>'
                f'<div class="small" style="margin-top:4px;">Connected from {html.escape(rank_label)}</div>'
                f'<pre style="white-space:pre-wrap; background:#0f1115; border:1px solid #2b313d; border-radius:8px; padding:10px; margin:6px 0 0 0;">{html.escape(blend_body[:1200])}</pre>'
                f'<div class="actions" style="margin-top:8px;">'
                f'<button type="button" class="small" onclick="saveCandidateResponse({_js_call_arg(blend_body)}, {run_persona_js}, {run_platform_js})">Save combined</button>'
                f'<button type="button" class="small" onclick="generateFromCombined({run_id_js}, {_js_call_arg(blend_body)}, {run_persona_js}, {run_platform_js})">Generate response</button>'
                f'<button type="button" class="small" onclick="generateUnderRun({run_id_js}, null, {run_persona_js}, {run_platform_js})">Add + generate under run</button>'
                '</div>'
                f'<div id="combined-generator-output-{run_id_value}" class="small" style="display:none; margin-top:8px; background:#131926; border:1px solid #2b313d; border-radius:8px; padding:10px;">'
                '<div style="font-weight:600; margin-bottom:6px;">Generated response</div>'
                '<div data-combined-output-text style="white-space:pre-wrap;"></div>'
                '<div style="margin-top:8px;"><a data-combined-output-link href="#">Open generated run</a></div>'
                '</div>'
                '</div>'
            )
        else:
            response_block = '<div class="response"><span class="small">Combined response (blend_top3)</span><div class="small">No candidate response saved.</div></div>'

        followup_block = (
            '<div class="response" style="margin-top:10px;">'
            '<span class="small">Add response + generate under original run</span>'
            f'<textarea data-replay-response="{run_id_value}" rows="3" placeholder="Paste incoming response, then generate a connected follow-up." style="width:100%; margin-top:6px; background:#0f1115; color:#e9edf3; border:1px solid #2b313d; border-radius:8px; padding:10px;"></textarea>'
            f'<div class="actions" style="margin-top:8px;"><button type="button" onclick="generateUnderRun({run_id_js}, null, {run_persona_js}, {run_platform_js})">Generate another one</button></div>'
            f'<div class="small" data-replay-status="{run_id_value}" style="margin-top:6px;"></div>'
            f'<div data-replay-results="{run_id_value}" style="margin-top:8px; display:none;"></div>'
            '</div>'
        )

        # Outcome section (Autopsy)
        outcome = run.get("outcome") or {}
        engagement = outcome.get("spectator_engagement") or 0

        items.append(
            f'<li class="run{active}"><div style="display:flex; justify-content:space-between; align-items:start;">'
            f'<div><a data-run-id="{run_id}" href="/?run_id={run_id}">Run #{run_id}</a> '
            f'<span class="meta">{platform} · {persona}{badge}</span></div>'
            f'<button type="button" class="small" onclick="document.getElementById(\'autopsy-{run_id}\').style.display=\'block\'">Autopsy</button></div>'
            f'<div class="source">{source}</div>'
            f'{response_block}'
            f'{followup_block}'
            f'<div id="autopsy-{run_id}" style="display:none; background:#11141b; border:1px solid #2b313d; border-radius:8px; padding:12px; margin-top:10px;">'
            f'<h4 style="margin:0 0 8px 0;">Run Autopsy #{run_id}</h4>'
            f'<div class="actions small">'
            f'<label><input type="checkbox" id="got-reply-{run_id}" {"checked" if outcome.get("got_reply") else ""}> Got reply</label>'
            f'<label>Engagement <input type="number" id="engagement-{run_id}" value="{engagement}" style="width:50px; background:#0f1115; color:#e9edf3; border:1px solid #2b313d;"></label>'
            f'<label>Tone <select id="tone-{run_id}" style="background:#0f1115; color:#e9edf3; border:1px solid #2b313d;"><option value="neutral">neutral</option><option value="hostile">hostile</option><option value="friendly">friendly</option></select></label>'
            f'</div>'
            f'<textarea id="notes-{run_id}" placeholder="Outcome notes..." rows="2" style="width:100%; margin-top:8px; background:#0f1115; color:#e9edf3; border:1px solid #2b313d; border-radius:8px; padding:8px;">{html.escape(outcome.get("notes") or "")}</textarea>'
            f'<div class="actions" style="margin-top:8px;"><button type="button" onclick="scoreRun({run_id})">Save Outcome</button><button type="button" onclick="document.getElementById(\'autopsy-{run_id}\').style.display=\'none\'">Cancel</button></div>'
            f'</div></li>'
        )
    runs_html = "\n".join(items) if items else "<li>No runs yet. Draft something first.</li>"
    outbox_items = []
    for item in outbox[:8]:
        emit_id = item.get("id")
        raw_status = item.get("status") or "unknown"
        status = html.escape(raw_status)
        transport = html.escape(item.get("transport") or "unknown")
        run_id = item.get("run_id")
        notes_value = str(item.get("notes") or "")
        notes = html.escape(notes_value)
        actions = []
        if item.get("status") != "approved":
            actions.append(f'<button type="button" data-outbox-action="approved" data-emit-id="{emit_id}">Approve</button>')
        if item.get("status") != "staged":
            actions.append(f'<button type="button" data-outbox-action="staged" data-emit-id="{emit_id}">Restage</button>')
        if item.get("status") != "archived":
            actions.append(f'<button type="button" data-outbox-action="archived" data-emit-id="{emit_id}">Archive</button>')
        if item.get("status") == "approved":
            actions.append(f'<button type="button" data-dispatch-emit data-emit-id="{emit_id}">Dispatch</button>')
        actions.append(f'<button type="button" data-outbox-save-notes data-emit-id="{emit_id}" data-current-status="{html.escape(raw_status)}">Save notes</button>')
        outbox_items.append(
            f'<li class="run"><div><b>#{emit_id}</b> · <span data-emit-status="{emit_id}">{status}</span> · '
            f'{transport} · <a href="/?run_id={run_id}">run {run_id}</a></div>'
            f'<label class="small" style="display:block; margin-top:10px;">Notes</label>'
            f'<textarea data-outbox-notes="{emit_id}" rows="3" style="width:100%; background:#0f1115; color:#e9edf3; border:1px solid #2b313d; border-radius:8px; padding:10px; margin-top:6px;">{notes}</textarea>'
            f'<div class="small" style="margin-top:6px;">{notes or "No notes."}</div>'
            f'<div class="actions" style="margin-top:8px;">{"".join(actions)}</div></li>'
        )
    outbox_html = "\n".join(outbox_items) if outbox_items else "<li>No outbox entries for this filter.</li>"
    dispatch_items = []
    for item in dispatches[:5]:
        dispatch_id = int(item.get("id") or 0)
        emit_id = int(item.get("emit_outbox_id") or 0)
        status = html.escape(item.get("status") or "unknown")
        driver = html.escape(item.get("driver") or "unknown")
        artifact_path = html.escape(str((item.get("response") or {}).get("artifact_path") or ""))
        dispatch_notes = html.escape(str(item.get("notes") or ""))
        actions = []
        if item.get("status") != "acknowledged":
            actions.append(f'<button type="button" data-dispatch-action="acknowledged" data-dispatch-id="{dispatch_id}">Acknowledge</button>')
        if item.get("status") != "delivered":
            actions.append(f'<button type="button" data-dispatch-action="delivered" data-dispatch-id="{dispatch_id}">Delivered</button>')
        if item.get("status") != "failed":
            actions.append(f'<button type="button" data-dispatch-action="failed" data-dispatch-id="{dispatch_id}">Failed</button>')
        if item.get("status") == "failed":
            actions.append(f'<button type="button" data-dispatch-redrive data-dispatch-id="{dispatch_id}">Redrive</button>')
        dispatch_items.append(
            f'<li class="small"><div><b>dispatch #{dispatch_id}</b> · emit #{emit_id} · '
            f'<span data-dispatch-status="{dispatch_id}">{status}</span> via {driver}</div>'
            f'<div><code>{artifact_path}</code></div>'
            f'<label style="display:block; margin-top:8px;">Notes</label>'
            f'<textarea data-dispatch-notes="{dispatch_id}" rows="2" style="width:100%; background:#0f1115; color:#e9edf3; border:1px solid #2b313d; border-radius:8px; padding:8px; margin-top:4px;">{dispatch_notes}</textarea>'
            f'<div class="actions" style="margin-top:8px;">{"".join(actions)}<button type="button" data-dispatch-save-notes data-dispatch-id="{dispatch_id}" data-current-status="{html.escape(str(item.get("status") or "dispatched"))}">Save dispatch notes</button></div></li>'
        )
    dispatch_html = "\n".join(dispatch_items) if dispatch_items else "<li>No dispatches yet.</li>"
    filter_labels = [("all", "All"), ("staged", "Staged"), ("approved", "Approved"), ("dispatched", "Dispatched"), ("acknowledged", "Acknowledged"), ("delivered", "Delivered"), ("failed", "Failed"), ("archived", "Archived")]
    retry_queue = payload.get("retry_queue") or {}
    due_items = retry_queue.get("due") or []
    waiting_items = retry_queue.get("waiting") or []
    dead_items = retry_queue.get("dead_letter") or []
    mutation_variants = payload.get("mutation_variants") or []
    mutation_items = []
    for item in mutation_variants[:15]:
        variant_id = int(item.get("id") or 0)
        run_id = int(item.get("run_id") or 0)
        transform = html.escape(str(item.get("transform") or ""))
        text = html.escape(str(item.get("variant_text") or "")[:280])
        lineage = item.get("lineage") or {}
        delta = lineage.get("delta_ratio")
        novelty = lineage.get("novelty_ratio")
        metric_bits = []
        if delta is not None:
            metric_bits.append(f"delta {float(delta):.2f}")
        if novelty is not None:
            metric_bits.append(f"novelty {float(novelty):.2f}")
        metric_line = f"<div class=\"small\" style=\"margin-top:4px;\">{' · '.join(metric_bits)}</div>" if metric_bits else ""
        mutation_items.append(
            f'<li class="run"><div><b>variant #{variant_id}</b> · run <a href="/?run_id={run_id}">{run_id}</a> · {transform}</div>'
            f'<div class="source" style="margin-top:6px;">{text}</div>{metric_line}</li>'
        )
    mutation_variants_html = "\n".join(mutation_items) if mutation_items else "<li>No mutation variants yet.</li>"

    retry_queue_html = f"""
    <div class="card" style="margin-bottom:20px;">
        <h3 style="margin-top:0;">Retry Queue</h3>
        <div style="display:grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px;">
            <div style="background:#1c212b; padding:12px; border-radius:8px; border-left: 4px solid #ffcc00;">
                <div class="small">DUE FOR RETRY</div>
                <div style="font-size:1.5rem; font-weight:bold; color:#ffcc00;">{retry_queue.get("due_count", 0)}</div>
            </div>
            <div style="background:#1c212b; padding:12px; border-radius:8px; border-left: 4px solid #8ec5ff;">
                <div class="small">WAITING</div>
                <div style="font-size:1.5rem; font-weight:bold; color:#8ec5ff;">{retry_queue.get("waiting_count", 0)}</div>
            </div>
            <div style="background:#1c212b; padding:12px; border-radius:8px; border-left: 4px solid #ff4d4d;">
                <div class="small">DEAD LETTER</div>
                <div style="font-size:1.5rem; font-weight:bold; color:#ff4d4d;">{retry_queue.get("dead_letter_count", 0)}</div>
            </div>
        </div>
        <div class="actions" style="margin-top:16px;">
            <button type="button" data-dispatch-approved data-include-retry-due="true" style="background: #ffcc00; color: #000; border-color: #ffcc00;">Process Due Retries</button>
            <button type="button" onclick="document.getElementById('retry-details').style.display='block'">View Details</button>
        </div>
        <div id="retry-details" style="display:none; margin-top:20px; border-top:1px solid #2b313d; padding-top:16px;">
            <h4>Due / Waiting Items</h4>
            <ul class="small">
                {"".join([f'<li><b>#{item["id"]}</b> ({item["status"]}) - next retry: {item["next_retry_at"]} (attempt {item["attempt_count"]})</li>' for item in due_items + waiting_items]) or "<li>No items in retry queue.</li>"}
            </ul>
            <h4 style="margin-top:16px; color:#ff4d4d;">Dead Letter Items</h4>
            <ul class="small">
                {"".join([f'<li><b>#{item["id"]}</b> - run {item["run_id"]} (attempts: {item["attempt_count"]})</li>' for item in dead_items]) or "<li>No dead letter items.</li>"}
            </ul>
            <button type="button" class="small" onclick="document.getElementById('retry-details').style.display='none'" style="margin-top:10px;">Close Details</button>
        </div>
    </div>
    """

    outbox_filter_links = []
    for key, label in filter_labels:
        count = int(outbox_counts.get(key) or 0)
        href = f"{base_url}/dashboard" if key == "all" else f"{base_url}/dashboard?outbox_status={key}"
        active_class = " button" if ((outbox_status is None and key == "all") or outbox_status == key) else ""
        outbox_filter_links.append(f'<a class="small{active_class}" href="{href}">{label} ({count})</a>')
    outbox_filter_html = " ".join(outbox_filter_links)
    latest_link = f"{base_url}/?run_id={latest_run_id}" if latest_run_id is not None else "#"
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>Bait Engine Dashboard</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; background: #0f1115; color: #e9edf3; margin: 0; padding: 24px; line-height: 1.4; }}
a {{ color: #8ec5ff; text-decoration: none; }}
a:hover {{ text-decoration: underline; }}
.card {{ background: #171a21; border: 1px solid #2b313d; border-radius: 12px; padding: 16px; margin-bottom: 16px; position: relative; overflow: hidden; }}
.card::after {{ content: ''; position: absolute; top: 0; left: 0; width: 4px; height: 100%; background: #2b313d; opacity: 0.5; }}
.card.health-high::after {{ background: #8effa3; }}
.card.health-low::after {{ background: #ff8e8e; }}
.status-bar {{ display: flex; gap: 24px; padding: 16px; background: #11141b; border: 1px solid #2b313d; border-radius: 12px; margin-bottom: 16px; border-left: 4px solid #8ec5ff; }}
.meta {{ color: #97a3b6; font-size: 0.9rem; margin-left: 8px; }}
.source {{ color: #cfd7e3; margin-top: 6px; font-size: 0.95rem; }}
.response {{ margin-top: 8px; padding: 10px; border: 1px solid #2b313d; border-radius: 8px; background: #151a24; color: #e9edf3; }}
.response .small {{ display: block; margin-bottom: 6px; }}
ul {{ list-style: none; padding: 0; margin: 0; }}
.run {{ padding: 12px; border: 1px solid #252b36; border-radius: 10px; margin-bottom: 10px; background: #1c212b; }}
.run.active {{ border-color: #8ec5ff; box-shadow: 0 0 0 1px rgba(142, 197, 255, 0.2) inset; }}
.actions {{ display: flex; gap: 12px; flex-wrap: wrap; margin-top: 12px; }}
button, .button {{ display: inline-block; background: #2b313d; color: #e9edf3; padding: 8px 14px; border-radius: 10px; font-weight: 600; border: 1px solid #3d4659; cursor: pointer; font-size: 0.9rem; transition: background 0.2s; }}
button:hover, .button:hover {{ background: #3d4659; }}
.button.primary, button[data-dispatch-approved], button[data-worker-cycle], #draftSubmitButton {{ background: #8ec5ff; color: #08111d; border-color: #8ec5ff; }}
.button.primary:hover, button[data-dispatch-approved]:hover, button[data-worker-cycle]:hover, #draftSubmitButton:hover {{ background: #b0d8ff; }}
.small {{ color: #97a3b6; font-size: 0.85rem; }}
code {{ font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace; background: #0f1115; padding: 2px 6px; border-radius: 4px; font-size: 0.9em; }}
</style>
</head>
<body>
  <div class="card">
    <h1 style="margin-top:0; font-size:1.5rem; letter-spacing:-0.02em;">Bait Engine Dashboard</h1>
    <p class="small" style="margin-bottom:16px;">Flight deck for cognitive rhetoric operations.</p>
    <div class="actions">
      <a class="button primary" id="openLatestRunLink" href="{latest_link}">Open latest run</a>
      <a href="{base_url}/panel.json">Panel JSON</a>
      <a href="{base_url}/api/runs">Runs API</a>
    </div>
  </div>

  <div class="status-bar">
    <div><span class="small">MISSION HEALTH (7D)</span><br><b style="font-size:1.3rem; color:{'#8effa3' if success_rate > 90 else '#ff8e8e'}">{success_rate}%</b></div>
    <div><span class="small">AUTO PICK ACCURACY</span><br><b style="font-size:1.3rem; color:#ffd700;">{auto_pick_accuracy}%</b></div>
    <div><span class="small">PERSONA DRIFT</span><br><b style="font-size:1.3rem;">{persona_drift}%</b></div>
    <div><span class="small">CONFIDENCE DIST</span><br><span class="small">&lt;.4 {confidence_distribution.get('lt_0_40', 0)} · .4-.6 {confidence_distribution.get('0_40_to_0_60', 0)} · .6-.8 {confidence_distribution.get('0_60_to_0_80', 0)} · ≥.8 {confidence_distribution.get('gte_0_80', 0)}</span></div>
    <div style="margin-left:auto; text-align:right;"><span class="small">PENDING EMITS</span><br><b style="font-size:1.3rem; color:#8ec5ff;">{pending_emit_count}</b></div>
  </div>
  <div class="card">
    <h2 style="margin-top:0;">Router audit</h2>
    <p class="small">forced_persona={int((payload.get('override_audit') or {}).get('forced_persona') or 0)} · auto_persona={int((payload.get('override_audit') or {}).get('auto_persona') or 0)} · unknown={int((payload.get('override_audit') or {}).get('unknown') or 0)}</p>
  </div>
  <div class="card">
    <h2 style="margin-top:0;">Daemon mode</h2>
    <p class="small">LaunchAgent state: <b id="daemonState">{html.escape(daemon_state)}</b> · label <code id="daemonLabelCode">{html.escape(str(daemon.get("label") or "ai.bait-engine.panel"))}</code></p>
    <p class="small">Worker policy: <span id="daemonPolicySummary">{html.escape('redrive on · limit ' + str(daemon.get('redrive_limit') or 10) + ' · cooldown ' + str(daemon.get('min_failed_age_seconds') or 0) + 's' if daemon.get('mode') == 'worker' and daemon.get('include_failed_redrive') else ('redrive off · limit ' + str(daemon.get('redrive_limit') or 10) + ' · cooldown ' + str(daemon.get('min_failed_age_seconds') or 0) + 's' if daemon.get('mode') == 'worker' else 'panel mode — no worker retry policy active'))}</span></p>
    <p class="small">Worker config: <span id="daemonConfigSummary">{html.escape('dispatch ' + str(daemon.get('dispatch_limit') or 25) + ' · driver ' + str(daemon.get('driver') or 'auto') + ' · every ' + str(daemon.get('interval_seconds') or 30.0) + 's · max cycles ' + str(daemon.get('max_cycles') or 0) + ' · out ' + str(daemon.get('out_dir') or '') if daemon.get('mode') == 'worker' else 'panel mode — worker launch config inactive')}</span></p>
    <p class="small">Warnings: <span id="daemonWarningsSummary">{html.escape(' | '.join(str(item) for item in (daemon.get('daemon_warnings') or [])) if daemon.get('daemon_warnings') else 'none')}</span></p>
    <p class="small">plist: <code>{html.escape(str(daemon.get("plist_path") or ""))}</code></p>
    <div class="actions">
      <label>Mode
        <select id="daemonModeSelect" style="margin-left:8px; background:#0f1115; color:#e9edf3; border:1px solid #2b313d; border-radius:8px; padding:8px;">
          <option value="panel"{' selected' if str(daemon.get('mode') or 'panel') == 'panel' else ''}>panel</option>
          <option value="worker"{' selected' if str(daemon.get('mode') or 'panel') == 'worker' else ''}>worker</option>
        </select>
      </label>
      <button id="daemonInstallButton" class="button" type="button" style="border:none; cursor:pointer;">Install login daemon</button>
      <button id="daemonRefreshButton" type="button">Refresh daemon state</button>
      <button id="daemonUninstallButton" type="button">Remove daemon</button>
      <a href="{base_url}/api/daemon">Daemon API</a>
    </div>
    <div id="daemonWorkerControls" class="actions small" style="margin-top:8px; align-items:center; flex-wrap:wrap; display:{'flex' if str(daemon.get('mode') or 'panel') == 'worker' else 'none'};">
      <label>Worker dispatch limit <input id="daemonWorkerDispatchLimit" type="number" min="1" value="{html.escape(str(daemon.get('dispatch_limit') or 25))}" style="margin-left:8px; width:84px; background:#0f1115; color:#e9edf3; border:1px solid #2b313d; border-radius:8px; padding:8px;" /></label>
      <label>Worker driver <select id="daemonWorkerDriver" style="margin-left:8px; width:148px; background:#0f1115; color:#e9edf3; border:1px solid #2b313d; border-radius:8px; padding:8px;" onchange="updateDriverTip(this.value)">{driver_options_html}</select></label>
      <label>Worker interval s <input id="daemonWorkerIntervalSeconds" type="number" min="0" step="0.1" value="{html.escape(str(daemon.get('interval_seconds') or 30.0))}" style="margin-left:8px; width:84px; background:#0f1115; color:#e9edf3; border:1px solid #2b313d; border-radius:8px; padding:8px;" /></label>
      <label>Worker out dir <input id="daemonWorkerOutDir" type="text" value="{html.escape(str(daemon.get('out_dir') or ''))}" style="margin-left:8px; width:240px; background:#0f1115; color:#e9edf3; border:1px solid #2b313d; border-radius:8px; padding:8px;" /></label>
      <label><input id="daemonWorkerIncludeFailedRedrive" type="checkbox"{' checked' if daemon.get('include_failed_redrive') else ''} /> worker redrive</label>
      <label>Worker redrive limit <input id="daemonWorkerRedriveLimit" type="number" min="1" value="{html.escape(str(daemon.get('redrive_limit') or 10))}" style="margin-left:8px; width:84px; background:#0f1115; color:#e9edf3; border:1px solid #2b313d; border-radius:8px; padding:8px;" /></label>
      <label>Worker cooldown s <input id="daemonWorkerMinFailedAgeSeconds" type="number" min="0" step="1" value="{html.escape(str(daemon.get('min_failed_age_seconds') or 0))}" style="margin-left:8px; width:84px; background:#0f1115; color:#e9edf3; border:1px solid #2b313d; border-radius:8px; padding:8px;" /></label>
      <label>Worker max cycles <input id="daemonWorkerMaxCycles" type="number" min="0" step="1" value="{html.escape(str(daemon.get('max_cycles') or 0))}" style="margin-left:8px; width:84px; background:#0f1115; color:#e9edf3; border:1px solid #2b313d; border-radius:8px; padding:8px;" /></label>
    </div>
    <div id="daemonDriverTip" class="small" style="background:#1c212b; border:1px solid #2b313d; border-radius:8px; padding:10px; margin-top:12px; border-left:4px solid #8ec5ff; display:none;"></div>
    <pre id="daemonPlistPre" class="small" style="white-space:pre-wrap; max-height:220px; overflow:auto; background:#0f1115; border:1px solid #2b313d; border-radius:10px; padding:12px; margin-top:12px;">{html.escape(str(daemon.get("plist") or ""))}</pre>
  </div>
  <div class="card">
    <h2 style="margin-top:0;">Quick draft</h2>
    <p class="small">Paste bait source text. The dashboard will draft, save, and kick you straight into the new run.</p>
    <div style="display:grid; gap:12px;">
      <textarea id="draftText" rows="6" style="width:100%; background:#0f1115; color:#e9edf3; border:1px solid #2b313d; border-radius:10px; padding:12px;">A model being useful doesn't make it true, and you're still confusing mechanism with necessity.</textarea>
      <div class="actions">
        <label>Persona <select id="draftPersona" style="margin-left:8px; background:#0f1115; color:#e9edf3; border:1px solid #2b313d; border-radius:8px; padding:8px;">{persona_options_html}</select></label>
        <label>Platform <input id="draftPlatform" value="{default_platform}" style="margin-left:8px; background:#0f1115; color:#e9edf3; border:1px solid #2b313d; border-radius:8px; padding:8px;" /></label>
        <label>Count <input id="draftCount" type="number" min="1" value="5" style="margin-left:8px; width:84px; background:#0f1115; color:#e9edf3; border:1px solid #2b313d; border-radius:8px; padding:8px;" /></label>
      </div>
      <div class="actions">
        <button id="draftSubmitButton" class="button" type="button" style="border:none; cursor:pointer;">Draft new run</button>
        <span id="draftStatus" class="small">Ready.</span>
        <span id="outboxStatus" class="small">Outbox idle.</span>
        <button type="button" data-worker-cycle>Run worker cycle</button>
      </div>
      <div class="actions small" style="margin-top:4px; align-items:center;">
        <label><input id="workerIncludeFailedRedrive" type="checkbox" /> include failed redrive</label>
        <label>Redrive limit <input id="workerRedriveLimit" type="number" min="1" value="10" style="margin-left:8px; width:84px; background:#0f1115; color:#e9edf3; border:1px solid #2b313d; border-radius:8px; padding:8px;" /></label>
        <label>Cooldown s <input id="workerMinFailedAgeSeconds" type="number" min="0" step="1" value="0" style="margin-left:8px; width:84px; background:#0f1115; color:#e9edf3; border:1px solid #2b313d; border-radius:8px; padding:8px;" /></label>
      </div>
    </div>
  </div>
  <div class="card">
    <h2 style="margin-top:0;">Evolution lab (Phase 10)</h2>
    <p class="small">Generate bounded variants from historical winners.</p>
    <div class="actions" style="align-items:center;">
      <label>Winner limit <input id="mutateWinnerLimit" type="number" min="1" value="5" style="margin-left:8px; width:84px; background:#0f1115; color:#e9edf3; border:1px solid #2b313d; border-radius:8px; padding:8px;" /></label>
      <label>Variants / winner <input id="mutateVariantsPerWinner" type="number" min="1" value="5" style="margin-left:8px; width:84px; background:#0f1115; color:#e9edf3; border:1px solid #2b313d; border-radius:8px; padding:8px;" /></label>
      <label>Days <input id="mutateDays" type="number" min="1" value="30" style="margin-left:8px; width:84px; background:#0f1115; color:#e9edf3; border:1px solid #2b313d; border-radius:8px; padding:8px;" /></label>
      <label><input id="mutateRequireReply" type="checkbox" checked /> require reply</label>
      <button id="mutateSubmitButton" type="button">Mutate winners</button>
      <span id="mutateStatus" class="small">Idle.</span>
    </div>
    <ul style="margin-top:12px;">{mutation_variants_html}</ul>
  </div>
  <div class="card">
    <h2 style="margin-top:0;">Recent runs</h2>
    <ul>{runs_html}</ul>
  </div>
  <div class="card">
    <h2 style="margin-top:0;">Local outbox</h2>
    <p class="small">Staged emits live here until they are manually dispatched.</p>
    <p class="small">Manual dispatch artifacts: <code>{dispatch_dir}</code></p>
    {retry_queue_html}
    <div class="actions" style="margin-bottom:12px;">{outbox_filter_html} <button type="button" data-dispatch-approved>Dispatch approved</button></div>
    <ul>{outbox_html}</ul>
  </div>
  <div class="card">
    <h2 style="margin-top:0;">Dispatch history</h2>
    <ul>{dispatch_html}</ul>
  </div>
<script>
const Boolean = (v) => !!v && v !== 'false' && v !== '0';
const STORAGE_KEYS = {{
  text: 'bait-engine:draft-text',
  persona: 'bait-engine:draft-persona',
  platform: 'bait-engine:draft-platform',
  count: 'bait-engine:draft-count',
  lastRunId: 'bait-engine:last-run-id',
  workerIncludeFailedRedrive: 'bait-engine:worker-include-failed-redrive',
  workerRedriveLimit: 'bait-engine:worker-redrive-limit',
  workerMinFailedAgeSeconds: 'bait-engine:worker-min-failed-age-seconds',
}};
const draftSubmitButton = document.getElementById('draftSubmitButton');
const draftStatus = document.getElementById('draftStatus');
const draftText = document.getElementById('draftText');
const draftPersona = document.getElementById('draftPersona');
const draftPlatform = document.getElementById('draftPlatform');
const draftCount = document.getElementById('draftCount');
const openLatestRunLink = document.getElementById('openLatestRunLink');
const outboxStatus = document.getElementById('outboxStatus');
const mutateWinnerLimit = document.getElementById('mutateWinnerLimit');
const mutateVariantsPerWinner = document.getElementById('mutateVariantsPerWinner');
const mutateDays = document.getElementById('mutateDays');
const mutateRequireReply = document.getElementById('mutateRequireReply');
const mutateSubmitButton = document.getElementById('mutateSubmitButton');
const mutateStatus = document.getElementById('mutateStatus');
const workerIncludeFailedRedrive = document.getElementById('workerIncludeFailedRedrive');
const workerRedriveLimit = document.getElementById('workerRedriveLimit');
const workerMinFailedAgeSeconds = document.getElementById('workerMinFailedAgeSeconds');
const daemonState = document.getElementById('daemonState');
const daemonLabelCode = document.getElementById('daemonLabelCode');
const daemonPolicySummary = document.getElementById('daemonPolicySummary');
const daemonConfigSummary = document.getElementById('daemonConfigSummary');
const daemonWarningsSummary = document.getElementById('daemonWarningsSummary');
const daemonModeSelect = document.getElementById('daemonModeSelect');
const daemonWorkerControls = document.getElementById('daemonWorkerControls');
const daemonWorkerDispatchLimit = document.getElementById('daemonWorkerDispatchLimit');
const daemonWorkerDriver = document.getElementById('daemonWorkerDriver');
const daemonWorkerIntervalSeconds = document.getElementById('daemonWorkerIntervalSeconds');
const daemonWorkerOutDir = document.getElementById('daemonWorkerOutDir');
const daemonWorkerIncludeFailedRedrive = document.getElementById('daemonWorkerIncludeFailedRedrive');
const daemonWorkerRedriveLimit = document.getElementById('daemonWorkerRedriveLimit');
const daemonWorkerMinFailedAgeSeconds = document.getElementById('daemonWorkerMinFailedAgeSeconds');
const daemonWorkerMaxCycles = document.getElementById('daemonWorkerMaxCycles');
const daemonPlistPre = document.getElementById('daemonPlistPre');
const daemonInstallButton = document.getElementById('daemonInstallButton');
const daemonRefreshButton = document.getElementById('daemonRefreshButton');
const daemonUninstallButton = document.getElementById('daemonUninstallButton');
const daemonDriverTip = document.getElementById('daemonDriverTip');
const driverRequirements = {driver_requirements_json};
function updateDriverTip(driver) {{
  if (!daemonDriverTip) return;
  const req = driverRequirements[driver];
  if (!req) {{
    daemonDriverTip.style.display = 'none';
    return;
  }}
  daemonDriverTip.style.display = 'block';
  daemonDriverTip.innerHTML = `<div><b>${{driver}}</b>: ${{req.description}}</div>
    <div class="small" style="margin-top:4px; font-family:monospace; opacity:0.8;">Requirements: ${{req.required.length ? req.required.map(r => '`'+r+'`').join(', ') : 'None'}}</div>
    <div class="small" style="margin-top:4px; font-family:monospace; opacity:0.8;">Example: ${{req.example}}</div>`;
}}
function setOutboxStatus(message) {{
  if (outboxStatus) {{
    outboxStatus.textContent = message;
  }}
}}
function setMutateStatus(message) {{
  if (mutateStatus) {{
    mutateStatus.textContent = message;
  }}
}}
function setDaemonState(message) {{
  if (daemonState) {{
    daemonState.textContent = message;
  }}
}}
function syncWorkerPolicyControls(source = null) {{
  const includeValue = source === 'daemon'
    ? Boolean(daemonWorkerIncludeFailedRedrive?.checked)
    : Boolean(workerIncludeFailedRedrive?.checked);
  const redriveValue = source === 'daemon'
    ? String(daemonWorkerRedriveLimit?.value || '10')
    : String(workerRedriveLimit?.value || '10');
  const cooldownValue = source === 'daemon'
    ? String(daemonWorkerMinFailedAgeSeconds?.value || '0')
    : String(workerMinFailedAgeSeconds?.value || '0');
  if (workerIncludeFailedRedrive) workerIncludeFailedRedrive.checked = includeValue;
  if (daemonWorkerIncludeFailedRedrive) daemonWorkerIncludeFailedRedrive.checked = includeValue;
  if (workerRedriveLimit) workerRedriveLimit.value = redriveValue;
  if (daemonWorkerRedriveLimit) daemonWorkerRedriveLimit.value = redriveValue;
  if (workerMinFailedAgeSeconds) workerMinFailedAgeSeconds.value = cooldownValue;
  if (daemonWorkerMinFailedAgeSeconds) daemonWorkerMinFailedAgeSeconds.value = cooldownValue;
}}
function applyDaemonMode() {{
  const workerMode = (daemonModeSelect?.value || 'panel') === 'worker';
  if (daemonWorkerControls) {{
    daemonWorkerControls.style.display = workerMode ? 'flex' : 'none';
  }}
  [
    daemonWorkerDispatchLimit,
    daemonWorkerDriver,
    daemonWorkerIntervalSeconds,
    daemonWorkerOutDir,
    daemonWorkerIncludeFailedRedrive,
    daemonWorkerRedriveLimit,
    daemonWorkerMinFailedAgeSeconds,
    daemonWorkerMaxCycles,
  ].forEach((element) => {{
    if (!element) return;
    element.disabled = !workerMode;
    if (element.closest) {{
      const label = element.closest('label');
      if (label) label.style.opacity = workerMode ? '1' : '0.55';
    }}
  }});
}}
function renderDaemon(payload) {{
  setDaemonState(`${{payload.loaded ? 'loaded' : (payload.installed ? 'installed' : 'not installed')}} (${{payload.mode || 'panel'}})`);
  if (daemonLabelCode) {{
    daemonLabelCode.textContent = payload.label || 'ai.bait-engine.panel';
  }}
  if (daemonPolicySummary) {{
    if ((payload.mode || 'panel') === 'worker') {{
      daemonPolicySummary.textContent = `${{payload.include_failed_redrive ? 'redrive on' : 'redrive off'}} · limit ${{payload.redrive_limit ?? 10}} · cooldown ${{payload.min_failed_age_seconds ?? 0}}s`;
    }} else {{
      daemonPolicySummary.textContent = 'panel mode — no worker retry policy active';
    }}
  }}
  if (daemonConfigSummary) {{
    if ((payload.mode || 'panel') === 'worker') {{
      daemonConfigSummary.textContent = `dispatch ${{payload.dispatch_limit ?? 25}} · driver ${{payload.driver || 'auto'}} · every ${{payload.interval_seconds ?? 30}}s · max cycles ${{payload.max_cycles ?? 0}} · out ${{payload.out_dir || ''}}`;
    }} else {{
      daemonConfigSummary.textContent = 'panel mode — worker launch config inactive';
    }}
  }}
  if (daemonWarningsSummary) {{
    const warnings = Array.isArray(payload.daemon_warnings) ? payload.daemon_warnings : [];
    daemonWarningsSummary.textContent = warnings.length ? warnings.join(' | ') : 'none';
  }}
  if (daemonModeSelect && payload.mode) {{
    daemonModeSelect.value = payload.mode;
  }}
  if (daemonWorkerDispatchLimit && payload.dispatch_limit !== undefined) {{
    daemonWorkerDispatchLimit.value = String(payload.dispatch_limit);
  }}
  if (daemonWorkerDriver) {{
    const supportedDrivers = Array.isArray(payload.supported_drivers) && payload.supported_drivers.length
      ? payload.supported_drivers.map((item) => String(item))
      : ['auto'];
    const selectedDriver = String(payload.driver || 'auto');
    if (!supportedDrivers.includes(selectedDriver)) {{
      supportedDrivers.push(selectedDriver);
    }}
    daemonWorkerDriver.innerHTML = supportedDrivers
      .map((name) => `<option value="${{name}}"${{name === selectedDriver ? ' selected' : ''}}>${{name}}</option>`)
      .join('');
    daemonWorkerDriver.value = selectedDriver;
  }}
  if (daemonWorkerIntervalSeconds && payload.interval_seconds !== undefined) {{
    daemonWorkerIntervalSeconds.value = String(payload.interval_seconds);
  }}
  if (daemonWorkerOutDir && payload.out_dir !== undefined) {{
    daemonWorkerOutDir.value = String(payload.out_dir || '');
  }}
  if (daemonWorkerIncludeFailedRedrive) {{
    daemonWorkerIncludeFailedRedrive.checked = Boolean(payload.include_failed_redrive);
  }}
  if (daemonWorkerRedriveLimit && payload.redrive_limit !== undefined) {{
    daemonWorkerRedriveLimit.value = String(payload.redrive_limit);
  }}
  if (daemonWorkerMinFailedAgeSeconds && payload.min_failed_age_seconds !== undefined) {{
    daemonWorkerMinFailedAgeSeconds.value = String(payload.min_failed_age_seconds);
  }}
  if (daemonWorkerMaxCycles && payload.max_cycles !== undefined) {{
    daemonWorkerMaxCycles.value = String(payload.max_cycles);
  }}
  if (daemonWorkerDriver) {{
    updateDriverTip(daemonWorkerDriver.value);
  }}
  syncWorkerPolicyControls('daemon');
  applyDaemonMode();
  if (daemonPlistPre && payload.plist) {{
    daemonPlistPre.textContent = payload.plist;
  }}
}}
function restoreDraftDefaults() {{
  draftText.value = localStorage.getItem(STORAGE_KEYS.text) || draftText.value;
  draftPersona.value = localStorage.getItem(STORAGE_KEYS.persona) || draftPersona.value;
  draftPlatform.value = localStorage.getItem(STORAGE_KEYS.platform) || draftPlatform.value;
  draftCount.value = localStorage.getItem(STORAGE_KEYS.count) || draftCount.value;
  if (workerIncludeFailedRedrive) {{
    workerIncludeFailedRedrive.checked = localStorage.getItem(STORAGE_KEYS.workerIncludeFailedRedrive) === 'true';
  }}
  if (workerRedriveLimit) {{
    workerRedriveLimit.value = localStorage.getItem(STORAGE_KEYS.workerRedriveLimit) || workerRedriveLimit.value;
  }}
  if (workerMinFailedAgeSeconds) {{
    workerMinFailedAgeSeconds.value = localStorage.getItem(STORAGE_KEYS.workerMinFailedAgeSeconds) || workerMinFailedAgeSeconds.value;
  }}
  syncWorkerPolicyControls('worker');
  const lastRunId = localStorage.getItem(STORAGE_KEYS.lastRunId);
  if (lastRunId && openLatestRunLink) {{
    openLatestRunLink.href = `/?run_id=${{lastRunId}}`;
    openLatestRunLink.textContent = `Resume run #${{lastRunId}}`;
  }}
}}
function persistDraftDefaults() {{
  localStorage.setItem(STORAGE_KEYS.text, draftText.value || '');
  localStorage.setItem(STORAGE_KEYS.persona, draftPersona.value || 'dry_midwit_savant');
  localStorage.setItem(STORAGE_KEYS.platform, draftPlatform.value || 'reddit');
  localStorage.setItem(STORAGE_KEYS.count, draftCount.value || '5');
  if (workerIncludeFailedRedrive) {{
    localStorage.setItem(STORAGE_KEYS.workerIncludeFailedRedrive, workerIncludeFailedRedrive.checked ? 'true' : 'false');
  }}
  if (workerRedriveLimit) {{
    localStorage.setItem(STORAGE_KEYS.workerRedriveLimit, workerRedriveLimit.value || '10');
  }}
  if (workerMinFailedAgeSeconds) {{
    localStorage.setItem(STORAGE_KEYS.workerMinFailedAgeSeconds, workerMinFailedAgeSeconds.value || '0');
  }}
}}
async function submitDraft() {{
  persistDraftDefaults();
  const text = draftText.value.trim();
  if (!text) {{
    draftStatus.textContent = 'Need source text.';
    return;
  }}
  draftSubmitButton.disabled = true;
  draftStatus.textContent = 'Drafting...';
  try {{
    const response = await fetch('{base_url}/api/draft', {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify({{
        text,
        persona: draftPersona.value || 'dry_midwit_savant',
        platform: draftPlatform.value || 'reddit',
        count: Number(draftCount.value || 5),
        force_engage: true,
      }}),
    }});
    const payload = await response.json();
    if (!response.ok || !payload.ok) {{
      throw new Error(payload.error || `HTTP ${{response.status}}`);
    }}
    localStorage.setItem(STORAGE_KEYS.lastRunId, String(payload.run_id));
    window.location.href = `/?run_id=${{payload.run_id}}`;
  }} catch (error) {{
    draftSubmitButton.disabled = false;
    draftStatus.textContent = `Draft failed: ${{error.message}}`;
  }}
}}

async function saveCandidateResponse(text, persona, platform) {{
  const candidateText = String(text || '').trim();
  if (!candidateText) {{
    draftStatus.textContent = 'No response text to save.';
    return;
  }}
  draftText.value = candidateText;
  draftPersona.value = String(persona || draftPersona.value || 'dry_midwit_savant');
  draftPlatform.value = String(platform || draftPlatform.value || 'reddit');
  persistDraftDefaults();
  draftSubmitButton.disabled = true;
  draftStatus.textContent = 'Saving selected response...';
  try {{
    const response = await fetch('{base_url}/api/draft', {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify({{
        text: candidateText,
        persona: draftPersona.value || 'dry_midwit_savant',
        platform: draftPlatform.value || 'reddit',
        count: Number(draftCount.value || 5),
        force_engage: true,
      }}),
    }});
    const payload = await response.json();
    if (!response.ok || !payload.ok) {{
      throw new Error(payload.error || `HTTP ${{response.status}}`);
    }}
    localStorage.setItem(STORAGE_KEYS.lastRunId, String(payload.run_id));
    window.location.href = `/?run_id=${{payload.run_id}}`;
  }} catch (error) {{
    draftSubmitButton.disabled = false;
    draftStatus.textContent = `Save failed: ${{error.message}}`;
  }}
}}
async function generateFromCombined(runId, combinedText, persona, platform) {{
  const runIdValue = Number(runId || 0);
  if (!runIdValue) {{
    draftStatus.textContent = 'Missing run id for combined generation.';
    return;
  }}
  const seedText = String(combinedText || '').trim();
  if (!seedText) {{
    draftStatus.textContent = 'No combined text to generate from.';
    return;
  }}
  draftStatus.textContent = `Generating response from combined top 3 for run #${{runIdValue}}...`;
  try {{
    const response = await fetch('{base_url}/api/draft', {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify({{
        text: seedText,
        persona: String(persona || draftPersona.value || 'dry_midwit_savant'),
        platform: String(platform || draftPlatform.value || 'reddit'),
        count: 3,
        force_engage: true,
      }}),
    }});
    const payload = await response.json();
    if (!response.ok || !payload.ok) {{
      throw new Error(payload.error || `HTTP ${{response.status}}`);
    }}
    const output = document.getElementById(`combined-generator-output-${{runIdValue}}`);
    if (output) {{
      output.style.display = 'block';
      const textTarget = output.querySelector('[data-combined-output-text]');
      if (textTarget) textTarget.textContent = String(payload.top_response || 'Generated run created. Open the run to inspect candidates.');
      const linkTarget = output.querySelector('[data-combined-output-link]');
      if (linkTarget) linkTarget.setAttribute('href', `/?run_id=${{payload.run_id}}`);
    }}
    draftStatus.textContent = `Generated run #${{payload.run_id}} from combined top 3.`;
  }} catch (error) {{
    draftStatus.textContent = `Combined generation failed: ${{error.message}}`;
  }}
}}

async function generateUnderRun(runId, text, persona, platform) {{
  const runIdValue = Number(runId || 0);
  if (!runIdValue) {{
    draftStatus.textContent = 'Missing run id for regenerate.';
    return;
  }}
  let candidateText = String(text || '').trim();
  if (!candidateText) {{
    const replayInput = document.querySelector(`[data-replay-response="${{runIdValue}}"]`);
    candidateText = String(replayInput?.value || '').trim();
  }}
  if (candidateText) {{
    draftText.value = candidateText;
  }}
  draftPersona.value = String(persona || draftPersona.value || 'dry_midwit_savant');
  draftPlatform.value = String(platform || draftPlatform.value || 'reddit');
  persistDraftDefaults();
  const replayStatus = document.querySelector(`[data-replay-status="${{runIdValue}}"]`);
  const replayResults = document.querySelector(`[data-replay-results="${{runIdValue}}"]`);
  draftStatus.textContent = `Generating another under run #${{runIdValue}}...`;
  if (replayStatus) replayStatus.textContent = 'Generating under original run...';
  try {{
    const response = await fetch('{base_url}/api/mutate-run', {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify({{
        run_id: runIdValue,
        variants_per_winner: Number(mutateVariantsPerWinner?.value || 3),
        strategy: 'controlled_v1',
      }}),
    }});
    const payload = await response.json();
    if (!response.ok || !payload.ok) {{
      throw new Error(payload.error || `HTTP ${{response.status}}`);
    }}
    const generatedCount = Number(payload.variant_count || 0);
    if (mutateStatus) mutateStatus.textContent = `Generated ${{generatedCount}} variants under run #${{runIdValue}}.`;
    if (replayStatus) replayStatus.textContent = `Generated ${{generatedCount}} variants under this run.`;
    if (replayResults) {{
      const previews = Array.isArray(payload.preview_variants) ? payload.preview_variants : [];
      if (previews.length) {{
        const lines = previews.slice(0, 3).map((item) => `<li>${{String(item || '')}}</li>`).join('');
        replayResults.innerHTML = `<div class="small">New connected variants:</div><ul style="margin:6px 0 0 18px;">${{lines}}</ul>`;
      }} else {{
        replayResults.innerHTML = '<div class="small">Generated. Refresh run to inspect new variants.</div>';
      }}
      replayResults.style.display = 'block';
    }}
    localStorage.setItem(STORAGE_KEYS.lastRunId, String(runIdValue));
    setTimeout(() => {{ window.location.href = `/?run_id=${{runIdValue}}`; }}, 300);
  }} catch (error) {{
    if (replayStatus) replayStatus.textContent = `Regenerate failed: ${{error.message}}`;
    draftStatus.textContent = `Regenerate failed: ${{error.message}}`;
  }}
}}

async function runMutationCycle() {{
  if (!mutateSubmitButton) return;
  mutateSubmitButton.disabled = true;
  if (mutateStatus) mutateStatus.textContent = 'Mutating winners...';
  try {{
    const response = await fetch('{base_url}/api/mutate-winners', {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify({{
        winner_limit: Number(mutateWinnerLimit?.value || 5),
        variants_per_winner: Number(mutateVariantsPerWinner?.value || 5),
        days: Number(mutateDays?.value || 30),
        require_reply: Boolean(mutateRequireReply?.checked),
        persona: (draftPersona?.value || '').trim() || null,
        platform: (draftPlatform?.value || '').trim() || null,
      }}),
    }});
    const payload = await response.json();
    if (!response.ok || !payload.ok) {{
      throw new Error(payload.error || `HTTP ${{response.status}}`);
    }}
    if (mutateStatus) {{
      mutateStatus.textContent = `Mutated ${{payload.variant_count || 0}} variants from ${{payload.winner_count || 0}} winners.`;
    }}
    window.setTimeout(() => window.location.reload(), 300);
  }} catch (error) {{
    if (mutateStatus) mutateStatus.textContent = `Mutation failed: ${{error.message}}`;
  }} finally {{
    mutateSubmitButton.disabled = false;
  }}
}}

async function updateOutboxStatus(emitId, status, notes = undefined, notesMode = 'append') {{
  setOutboxStatus(`Updating emit #${{emitId}}...`);
  try {{
    const body = {{ emit_id: Number(emitId), status }};
    if (notes !== undefined) {{
      body.notes = notes;
      body.notes_mode = notesMode;
    }}
    const response = await fetch('{base_url}/api/outbox-status', {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify(body),
    }});
    const payload = await response.json();
    if (!response.ok || !payload.ok) {{
      throw new Error(payload.error || `HTTP ${{response.status}}`);
    }}
    const statusNode = document.querySelector(`[data-emit-status="${{emitId}}"]`);
    if (statusNode) {{
      statusNode.textContent = payload.emit?.status || status;
    }}
    const notesNode = document.querySelector(`[data-outbox-notes="${{emitId}}"]`);
    if (notesNode && payload.emit?.notes !== undefined) {{
      notesNode.value = payload.emit.notes || '';
    }}
    setOutboxStatus(`Emit #${{emitId}} saved as ${{payload.emit?.status || status}}.`);
    window.setTimeout(() => window.location.reload(), 250);
  }} catch (error) {{
    setOutboxStatus(`Outbox update failed: ${{error.message}}`);
  }}
}}
async function saveOutboxNotes(emitId, currentStatus) {{
  const notesNode = document.querySelector(`[data-outbox-notes="${{emitId}}"]`);
  await updateOutboxStatus(emitId, currentStatus, notesNode ? notesNode.value : '', 'replace');
}}
async function dispatchEmit(emitId) {{
  setOutboxStatus(`Dispatching emit #${{emitId}}...`);
  try {{
    const response = await fetch('{base_url}/api/dispatch-emit', {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify({{ emit_id: Number(emitId) }}),
    }});
    const payload = await response.json();
    if (!response.ok || !payload.ok) {{
      throw new Error(payload.error || `HTTP ${{response.status}}`);
    }}
    setOutboxStatus(`Emit #${{emitId}} dispatched to ${{payload.dispatch?.response?.artifact_path || payload.dispatch?.artifact_path || 'artifact'}}.`);
    window.setTimeout(() => window.location.reload(), 250);
  }} catch (error) {{
    setOutboxStatus(`Dispatch failed: ${{error.message}}`);
  }}
}}
async function dispatchApproved(include_retry_due = false) {{
  setOutboxStatus(`Dispatching approved emits${{include_retry_due ? ' (including retries)' : ''}}...`);
  try {{
    const response = await fetch(`${{'{base_url}'}}/api/dispatch-approved`, {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify({{ include_retry_due: Boolean(include_retry_due) }}),
    }});
    const payload = await response.json();
    if (!response.ok || !payload.ok) {{
      throw new Error(payload.error || `HTTP ${{response.status}}`);
    }}
    setOutboxStatus(`Dispatched ${{payload.dispatched_count || 0}} approved emits.`);
    window.setTimeout(() => window.location.reload(), 250);
  }} catch (error) {{
    setOutboxStatus(`Batch dispatch failed: ${{error.message}}`);
  }}
}}
async function updateDispatchStatus(dispatchId, status, notes = undefined, notesMode = 'append') {{
  setOutboxStatus(`Updating dispatch #${{dispatchId}}...`);
  try {{
    const body = {{ dispatch_id: Number(dispatchId), status }};
    if (notes !== undefined) {{
      body.notes = notes;
      body.notes_mode = notesMode;
    }}
    const response = await fetch('{base_url}/api/dispatch-status', {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify(body),
    }});
    const payload = await response.json();
    if (!response.ok || !payload.ok) {{
      throw new Error(payload.error || `HTTP ${{response.status}}`);
    }}
    const statusNode = document.querySelector(`[data-dispatch-status="${{dispatchId}}"]`);
    if (statusNode) {{
      statusNode.textContent = payload.dispatch?.status || status;
    }}
    const notesNode = document.querySelector(`[data-dispatch-notes="${{dispatchId}}"]`);
    if (notesNode && payload.dispatch?.notes !== undefined) {{
      notesNode.value = payload.dispatch.notes || '';
    }}
    setOutboxStatus(`Dispatch #${{dispatchId}} saved as ${{payload.dispatch?.status || status}}.`);
    window.setTimeout(() => window.location.reload(), 250);
  }} catch (error) {{
    setOutboxStatus(`Dispatch status failed: ${{error.message}}`);
  }}
}}
async function saveDispatchNotes(dispatchId, currentStatus) {{
  const notesNode = document.querySelector(`[data-dispatch-notes="${{dispatchId}}"]`);
  await updateDispatchStatus(dispatchId, currentStatus, notesNode ? notesNode.value : '', 'replace');
}}
async function redriveDispatch(dispatchId) {{
  setOutboxStatus(`Redriving dispatch #${{dispatchId}}...`);
  try {{
    const response = await fetch('{base_url}/api/dispatch-redrive', {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify({{ dispatch_id: Number(dispatchId) }}),
    }});
    const payload = await response.json();
    if (!response.ok || !payload.ok) {{
      throw new Error(payload.error || `HTTP ${{response.status}}`);
    }}
    setOutboxStatus(`Redrove dispatch #${{dispatchId}} into dispatch #${{payload.dispatch?.id || '?'}}.`);
    window.setTimeout(() => window.location.reload(), 250);
  }} catch (error) {{
    setOutboxStatus(`Redrive failed: ${{error.message}}`);
  }}
}}
async function runWorkerCycle() {{
  persistDraftDefaults();
  setOutboxStatus('Running worker cycle...');
  try {{
    const includeFailedRedrive = Boolean(workerIncludeFailedRedrive?.checked);
    const redriveLimit = Math.max(1, Number(workerRedriveLimit?.value || 10));
    const minFailedAgeSeconds = Math.max(0, Number(workerMinFailedAgeSeconds?.value || 0));
    const response = await fetch('{base_url}/api/worker-cycle', {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify({{
        include_failed_redrive: includeFailedRedrive,
        redrive_limit: redriveLimit,
        min_failed_age_seconds: minFailedAgeSeconds,
      }}),
    }});
    const payload = await response.json();
    if (!response.ok || !payload.ok) {{
      throw new Error(payload.error || `HTTP ${{response.status}}`);
    }}
    setOutboxStatus(`Worker cycle dispatched ${{payload.dispatched_count || 0}} emits and redrove ${{payload.redriven_count || 0}} failed dispatches after ${{payload.min_failed_age_seconds || 0}}s cooldown.`);
    window.setTimeout(() => window.location.reload(), 250);
  }} catch (error) {{
    setOutboxStatus(`Worker cycle failed: ${{error.message}}`);
  }}
}}
async function invokeDaemon(action) {{
  setDaemonState(`${{action}}...`);
  try {{
    const response = await fetch('{base_url}/api/daemon', {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify({{
        action,
        mode: daemonModeSelect?.value || 'panel',
        load: action === 'install',
        unload: action === 'uninstall',
        dispatch_limit: Math.max(1, Number(daemonWorkerDispatchLimit?.value || 25)),
        driver: String(daemonWorkerDriver?.value || 'auto'),
        out_dir: String(daemonWorkerOutDir?.value || ''),
        interval_seconds: Math.max(0, Number(daemonWorkerIntervalSeconds?.value || 30)),
        max_cycles: Math.max(0, Number(daemonWorkerMaxCycles?.value || 0)),
        include_failed_redrive: Boolean(daemonWorkerIncludeFailedRedrive?.checked),
        redrive_limit: Math.max(1, Number(daemonWorkerRedriveLimit?.value || 10)),
        min_failed_age_seconds: Math.max(0, Number(daemonWorkerMinFailedAgeSeconds?.value || 0)),
      }}),
    }});
    const payload = await response.json();
    if (!response.ok || !payload.ok) {{
      throw new Error(payload.error || `HTTP ${{response.status}}`);
    }}
    renderDaemon(payload.daemon || payload);
  }} catch (error) {{
    setDaemonState(`daemon error: ${{error.message}}`);
  }}
}}
async function scoreRun(runId) {{
  setOutboxStatus(`Scoring run #${{runId}}...`);
  try {{
    const body = {{
      run_id: runId,
      got_reply: document.getElementById(`got-reply-${{runId}}`).checked,
      engagement: Number(document.getElementById(`engagement-${{runId}}`).value),
      tone_shift: document.getElementById(`tone-${{runId}}`).value,
      notes: document.getElementById(`notes-${{runId}}`).value
    }};
    const response = await fetch(`${{base_url}}/api/score-run`, {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify(body),
    }});
    const payload = await response.json();
    if (!response.ok || !payload.ok) {{
      throw new Error(payload.error || `HTTP ${{response.status}}`);
    }}
    setOutboxStatus(`Run #${{runId}} scored!`);
    window.setTimeout(() => window.location.reload(), 250);
  }} catch (error) {{
    setOutboxStatus(`Scoring fail: ${{error.message}}`);
  }}
}}
restoreDraftDefaults();
document.querySelectorAll('[data-run-id]').forEach((link) => {{
  link.addEventListener('click', () => {{
    localStorage.setItem(STORAGE_KEYS.lastRunId, link.dataset.runId || '');
  }});
}});
[draftText, draftPersona, draftPlatform, draftCount, daemonModeSelect].filter(Boolean).forEach((element) => {{
  element.addEventListener('input', persistDraftDefaults);
  element.addEventListener('change', persistDraftDefaults);
}});
[
  [workerIncludeFailedRedrive, 'worker'],
  [workerRedriveLimit, 'worker'],
  [workerMinFailedAgeSeconds, 'worker'],
  [daemonWorkerDispatchLimit, 'daemon'],
  [daemonWorkerDriver, 'daemon'],
  [daemonWorkerIntervalSeconds, 'daemon'],
  [daemonWorkerOutDir, 'daemon'],
  [daemonWorkerIncludeFailedRedrive, 'daemon'],
  [daemonWorkerRedriveLimit, 'daemon'],
  [daemonWorkerMinFailedAgeSeconds, 'daemon'],
  [daemonWorkerMaxCycles, 'daemon'],
].forEach(([element, source]) => {{
  if (!element) return;
  const syncAndPersist = () => {{
    syncWorkerPolicyControls(source);
    persistDraftDefaults();
  }};
  element.addEventListener('input', syncAndPersist);
  element.addEventListener('change', syncAndPersist);
}});
document.querySelectorAll('[data-outbox-action]').forEach((button) => {{
  button.addEventListener('click', () => updateOutboxStatus(button.dataset.emitId, button.dataset.outboxAction));
}});
document.querySelectorAll('[data-outbox-save-notes]').forEach((button) => {{
  button.addEventListener('click', () => saveOutboxNotes(button.dataset.emitId, button.dataset.currentStatus || 'staged'));
}});
document.querySelectorAll('[data-dispatch-emit]').forEach((button) => {{
  button.addEventListener('click', () => dispatchEmit(button.dataset.emitId));
}});
document.querySelectorAll('[data-dispatch-approved]').forEach((button) => {{
  button.addEventListener('click', () => dispatchApproved(button.hasAttribute('data-include-retry-due')));
}});
document.querySelectorAll('[data-dispatch-action]').forEach((button) => {{
  button.addEventListener('click', () => updateDispatchStatus(button.dataset.dispatchId, button.dataset.dispatchAction));
}});
document.querySelectorAll('[data-dispatch-save-notes]').forEach((button) => {{
  button.addEventListener('click', () => saveDispatchNotes(button.dataset.dispatchId, button.dataset.currentStatus || 'dispatched'));
}});
document.querySelectorAll('[data-dispatch-redrive]').forEach((button) => {{
  button.addEventListener('click', () => redriveDispatch(button.dataset.dispatchId));
}});
document.querySelectorAll('[data-worker-cycle]').forEach((button) => {{
  button.addEventListener('click', () => runWorkerCycle());
}});
if (daemonModeSelect) {{
  daemonModeSelect.addEventListener('change', applyDaemonMode);
}}
applyDaemonMode();
if (daemonInstallButton) {{
  daemonInstallButton.addEventListener('click', () => invokeDaemon('install'));
}}
if (daemonRefreshButton) {{
  daemonRefreshButton.addEventListener('click', () => invokeDaemon('status'));
}}
if (daemonUninstallButton) {{
  daemonUninstallButton.addEventListener('click', () => invokeDaemon('uninstall'));
}}
if (mutateSubmitButton) {{
  mutateSubmitButton.addEventListener('click', () => runMutationCycle());
}}
draftSubmitButton.addEventListener('click', submitDraft);
</script>
</body>
</html>
"""


def _build_panel_payload(
    run_id: int,
    db_path: str | None = None,
    thread_id: str | None = None,
    reply_to_id: str | None = None,
    author_handle: str | None = None,
    context_json: str | None = None,
    include_all_presets: bool = True,
    include_strategy_variants: bool = False,
    include_filter_variants: bool = True,
    variant_limit: int | None = None,
    include_outcome_overlay: bool = True,
    history_limit: int = 50,
    review_bridge: dict[str, Any] | None = None,
) -> dict[str, Any]:
    repo = RunRepository(db_path)
    run = repo.get_run(run_id)
    history_runs = repo.list_full_runs(limit=history_limit) if include_outcome_overlay else None
    panel_reviews = repo.list_panel_reviews(limit=history_limit) if include_outcome_overlay else None
    reputation_data = repo.get_persona_reputation(str(run.get("persona") or ""), str(run.get("platform") or ""))
    return build_preview_panel(
        run,
        thread_id=thread_id,
        reply_to_id=reply_to_id,
        author_handle=author_handle,
        context=json.loads(context_json) if context_json else None,
        include_all_presets=include_all_presets,
        include_strategy_variants=include_strategy_variants,
        include_filter_variants=include_filter_variants,
        variant_limit=variant_limit,
        history_runs=history_runs,
        panel_reviews=panel_reviews,
        review_bridge=review_bridge,
        reputation_data=reputation_data,
    )


def cmd_panel_preview(
    run_id: int,
    db_path: str | None = None,
    thread_id: str | None = None,
    reply_to_id: str | None = None,
    author_handle: str | None = None,
    context_json: str | None = None,
    out_path: str | None = None,
    include_all_presets: bool = True,
    include_strategy_variants: bool = False,
    include_filter_variants: bool = True,
    variant_limit: int | None = None,
    include_outcome_overlay: bool = True,
    history_limit: int = 50,
) -> dict:
    panel = _build_panel_payload(
        run_id,
        db_path=db_path,
        thread_id=thread_id,
        reply_to_id=reply_to_id,
        author_handle=author_handle,
        context_json=context_json,
        include_all_presets=include_all_presets,
        include_strategy_variants=include_strategy_variants,
        include_filter_variants=include_filter_variants,
        variant_limit=variant_limit,
        include_outcome_overlay=include_outcome_overlay,
        history_limit=history_limit,
        review_bridge={
            "kind": "cli",
            "program": "python3",
            "argv_prefix": ["-m", "bait_engine.cli.main"],
            "cwd": str(Path(__file__).resolve().parents[2]),
            "db_path": db_path,
        },
    )
    if out_path is not None:
        path = Path(out_path)
        path.write_text(render_preview_panel_html(panel), encoding="utf-8")
        panel["html_path"] = str(path)
    return panel


def cmd_record_panel_review(
    run_id: int,
    disposition: str,
    selection_preset: str | None = None,
    selection_strategy: str | None = None,
    tactic: str | None = None,
    objective: str | None = None,
    notes: str | None = None,
    db_path: str | None = None,
) -> dict:
    repo = RunRepository(db_path)
    run = repo.get_run(run_id)
    return repo.record_panel_review(
        PanelReviewRecord(
            id=None,
            run_id=run_id,
            platform=str(run["platform"]),
            persona=str(run["persona"]),
            candidate_tactic=tactic,
            candidate_objective=objective,
            selection_preset=selection_preset,
            selection_strategy=selection_strategy,
            disposition=disposition,
            notes=notes,
        )
    )


def cmd_score_run(
    run_id: int,
    got_reply: bool,
    engagement: int = 0,
    tone_shift: str | None = None,
    notes: str | None = None,
    db_path: str | None = None,
) -> dict:
    repo = RunRepository(db_path)
    outcome = OutcomeRecord(
        id=None,
        run_id=run_id,
        got_reply=got_reply,
        reply_delay_seconds=None,
        reply_length=None,
        tone_shift=tone_shift,
        spectator_engagement=engagement,
        result_label=None,
        notes=notes,
    )
    return repo.record_outcome(outcome)


def cmd_record_outcome(
    run_id: int,
    got_reply: bool,
    reply_delay_seconds: int | None,
    reply_length: int | None,
    tone_shift: str | None,
    spectator_engagement: int | None,
    result_label: str | None,
    notes: str | None,
    db_path: str | None = None,
) -> dict:
    repo = RunRepository(db_path)
    return repo.record_outcome(
        OutcomeRecord(
            id=None,
            run_id=run_id,
            got_reply=got_reply,
            reply_delay_seconds=reply_delay_seconds,
            reply_length=reply_length,
            tone_shift=tone_shift,
            spectator_engagement=spectator_engagement,
            result_label=result_label,
            notes=notes,
        )
    )


def cmd_outbox(limit: int = 100, db_path: str | None = None, status: str | None = None) -> dict:
    repo = RunRepository(db_path)
    return {"emit_outbox": repo.list_emit_outbox(limit=limit, status=status), "status": status}


def cmd_dispatch_emit(
    emit_id: int,
    *,
    db_path: str | None = None,
    driver: str = "auto",
    out_dir: str | None = None,
    notes: str | None = None,
    force: bool = False,
) -> dict:
    repo = RunRepository(db_path)
    result = repo.dispatch_emit(
        emit_id,
        driver=driver,
        out_dir=out_dir,
        notes=notes,
        force=force,
    )
    return {
        "ok": True,
        "emit": result.get("emit"),
        "dispatch": result.get("dispatch"),
        "run_id": result.get("run", {}).get("id") if isinstance(result.get("run"), dict) else None,
    }


def cmd_dispatch_approved(
    *,
    limit: int = 25,
    db_path: str | None = None,
    driver: str = "auto",
    out_dir: str | None = None,
    notes: str | None = None,
    include_retry_due: bool = False,
) -> dict:
    repo = RunRepository(db_path)
    result = repo.dispatch_approved(
        limit=limit,
        driver=driver,
        out_dir=out_dir,
        notes=notes,
        include_retry_due=include_retry_due,
    )
    return {"ok": True, **result}


def cmd_update_dispatch_status(
    dispatch_id: int,
    *,
    status: str,
    db_path: str | None = None,
    notes: str | None = None,
    notes_mode: str = "append",
) -> dict:
    repo = RunRepository(db_path)
    result = repo.update_emit_dispatch(dispatch_id, status=status, notes=notes, notes_mode=notes_mode)
    return {
        "ok": True,
        "emit": result.get("emit"),
        "dispatch": result.get("dispatch"),
        "run_id": result.get("run", {}).get("id") if isinstance(result.get("run"), dict) else None,
    }


def cmd_redrive_dispatch(
    dispatch_id: int,
    *,
    db_path: str | None = None,
    driver: str | None = None,
    out_dir: str | None = None,
    notes: str | None = None,
) -> dict:
    repo = RunRepository(db_path)
    result = repo.redrive_dispatch(dispatch_id, driver=driver, out_dir=out_dir, notes=notes)
    return {
        "ok": True,
        "previous_dispatch": result.get("previous_dispatch"),
        "emit": result.get("emit"),
        "dispatch": result.get("dispatch"),
        "run_id": result.get("run", {}).get("id") if isinstance(result.get("run"), dict) else None,
    }


def cmd_worker_cycle(
    *,
    db_path: str | None = None,
    dispatch_limit: int = 25,
    driver: str = "auto",
    out_dir: str | None = None,
    notes: str | None = None,
    include_failed_redrive: bool = False,
    redrive_limit: int = 10,
    min_failed_age_seconds: float = 0.0,
    max_actions_per_hour: int = 0,
    max_actions_per_day: int = 0,
    min_seconds_between_actions: float = 0.0,
    quiet_hours_start: int | None = None,
    quiet_hours_end: int | None = None,
) -> dict:
    repo = RunRepository(db_path)
    result = repo.worker_cycle(
        dispatch_limit=dispatch_limit,
        driver=driver,
        out_dir=out_dir,
        notes=notes,
        include_failed_redrive=include_failed_redrive,
        redrive_limit=redrive_limit,
        min_failed_age_seconds=min_failed_age_seconds,
        max_actions_per_hour=max_actions_per_hour,
        max_actions_per_day=max_actions_per_day,
        min_seconds_between_actions=min_seconds_between_actions,
        quiet_hours_start=quiet_hours_start,
        quiet_hours_end=quiet_hours_end,
    )
    return {"ok": True, **result}


def cmd_worker_run(
    *,
    db_path: str | None = None,
    dispatch_limit: int = 25,
    driver: str = "auto",
    out_dir: str | None = None,
    notes: str | None = None,
    interval_seconds: float = 30.0,
    max_cycles: int = 0,
    include_failed_redrive: bool = False,
    redrive_limit: int = 10,
    min_failed_age_seconds: float = 0.0,
    max_actions_per_hour: int = 0,
    max_actions_per_day: int = 0,
    min_seconds_between_actions: float = 0.0,
    quiet_hours_start: int | None = None,
    quiet_hours_end: int | None = None,
) -> dict:
    cycles: list[dict[str, Any]] = []
    cycle_count = 0
    while True:
        cycle_count += 1
        cycle = cmd_worker_cycle(
            db_path=db_path,
            dispatch_limit=dispatch_limit,
            driver=driver,
            out_dir=out_dir,
            notes=notes,
            include_failed_redrive=include_failed_redrive,
            redrive_limit=redrive_limit,
            min_failed_age_seconds=min_failed_age_seconds,
            max_actions_per_hour=max_actions_per_hour,
            max_actions_per_day=max_actions_per_day,
            min_seconds_between_actions=min_seconds_between_actions,
            quiet_hours_start=quiet_hours_start,
            quiet_hours_end=quiet_hours_end,
        )
        cycles.append(cycle)
        if max_cycles and cycle_count >= max_cycles:
            break
        time.sleep(interval_seconds)
    return {
        "ok": True,
        "cycles_run": cycle_count,
        "interval_seconds": interval_seconds,
        "max_cycles": max_cycles,
        "cycles": cycles,
    }


def _create_panel_http_server(
    run_id: int | None,
    *,
    db_path: str | None = None,
    thread_id: str | None = None,
    reply_to_id: str | None = None,
    author_handle: str | None = None,
    context_json: str | None = None,
    include_all_presets: bool = True,
    include_strategy_variants: bool = False,
    include_filter_variants: bool = True,
    variant_limit: int | None = None,
    include_outcome_overlay: bool = True,
    history_limit: int = 50,
    host: str = "127.0.0.1",
    port: int = 0,
) -> ThreadingHTTPServer:
    panel_kwargs = {
        "db_path": db_path,
        "thread_id": thread_id,
        "reply_to_id": reply_to_id,
        "author_handle": author_handle,
        "context_json": context_json,
        "include_all_presets": include_all_presets,
        "include_strategy_variants": include_strategy_variants,
        "include_filter_variants": include_filter_variants,
        "variant_limit": variant_limit,
        "include_outcome_overlay": include_outcome_overlay,
        "history_limit": history_limit,
    }

    def _active_run_id_from_request(path: str) -> int:
        parsed = urlparse(path)
        query = parse_qs(parsed.query)
        requested = query.get("run_id", [None])[0]
        if requested in {None, ""}:
            return _resolve_panel_run_id(run_id, db_path)
        return _resolve_panel_run_id(int(requested), db_path)

    class PanelHTTPRequestHandler(BaseHTTPRequestHandler):
        def _write_json(self, status_code: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, indent=2).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)

        def _write_html(self, status_code: int, html_text: str) -> None:
            body = html_text.encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_OPTIONS(self) -> None:  # noqa: N802
            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            base_url = f"http://{self.server.server_address[0]}:{self.server.server_address[1]}"
            if parsed.path in {"/api/runs", "/dashboard", "/api/outbox", "/api/daemon"}:
                query = parse_qs(parsed.query)
                outbox_status = query.get("outbox_status", [None])[0] or None
                try:
                    active_run_id = _active_run_id_from_request(self.path)
                except KeyError:
                    active_run_id = None
                dashboard = _build_dashboard_payload(db_path=db_path, active_run_id=active_run_id, outbox_status=outbox_status)
                if parsed.path == "/api/daemon":
                    self._write_json(200, dashboard.get("daemon") or {})
                elif parsed.path in {"/api/runs", "/api/outbox"}:
                    self._write_json(200, dashboard)
                else:
                    self._write_html(200, _render_dashboard_html(dashboard, base_url))
                return
            try:
                active_run_id = _active_run_id_from_request(self.path)
                panel = _build_panel_payload(
                    active_run_id,
                    **panel_kwargs,
                    review_bridge={
                        "kind": "http",
                        "submit_url": f"{base_url}/api/review",
                        "panel_url": f"{base_url}/?run_id={active_run_id}",
                        "dashboard_url": f"{base_url}/dashboard?run_id={active_run_id}",
                    },
                )
            except KeyError:
                if parsed.path in {"/", "/index.html", "/dashboard", "/api/runs", "/api/outbox", "/api/daemon"}:
                    query = parse_qs(parsed.query)
                    outbox_status = query.get("outbox_status", [None])[0] or None
                    dashboard = _build_dashboard_payload(db_path=db_path, active_run_id=None, outbox_status=outbox_status)
                    if parsed.path == "/api/daemon":
                        self._write_json(200, dashboard.get("daemon") or {})
                    elif parsed.path in {"/api/runs", "/api/outbox"}:
                        self._write_json(200, dashboard)
                    else:
                        self._write_html(200, _render_dashboard_html(dashboard, base_url))
                    return
                self._write_json(404, {"error": "no runs available"})
                return
            if parsed.path in {"/", "/index.html"}:
                self._write_html(200, render_preview_panel_html(panel))
                return
            if parsed.path == "/panel.json":
                self._write_json(200, panel)
                return
            self._write_json(404, {"error": f"unknown path: {parsed.path}"})

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path not in {"/api/review", "/api/draft", "/api/stage-emit", "/api/outbox-status", "/api/daemon", "/api/dispatch-emit", "/api/dispatch-approved", "/api/dispatch-status", "/api/dispatch-redrive", "/api/worker-cycle", "/api/mutate-winners", "/api/mutate-run"}:
                self._write_json(404, {"error": f"unknown path: {parsed.path}"})
                return
            content_length = int(self.headers.get("Content-Length") or 0)
            raw_body = self.rfile.read(content_length)
            try:
                payload = json.loads(raw_body.decode("utf-8") or "{}")
            except json.JSONDecodeError as exc:
                self._write_json(400, {"error": f"invalid json: {exc}"})
                return
            if parsed.path == "/api/draft":
                try:
                    created = cmd_draft(
                        text=str(payload["text"]),
                        persona_name=str(payload.get("persona") or "dry_midwit_savant"),
                        candidate_count=int(payload.get("count") or 5),
                        save=True,
                        db_path=db_path,
                        platform=str(payload.get("platform") or "reddit"),
                        force_engage=bool(payload.get("force_engage", True)),
                    )
                except (KeyError, TypeError, ValueError) as exc:
                    self._write_json(400, {"error": str(exc)})
                    return
                top_response = None
                created_candidates = created.get("candidates") if isinstance(created, dict) else None
                if isinstance(created_candidates, list) and created_candidates:
                    top_response = str((created_candidates[0] or {}).get("text") or "") or None
                self._write_json(
                    200,
                    {
                        "ok": True,
                        "run_id": created["run_id"],
                        "persona": created.get("plan", {}).get("persona") if isinstance(created.get("plan"), dict) else None,
                        "platform": payload.get("platform") or "reddit",
                        "top_response": top_response,
                    },
                )
                return
            if parsed.path == "/api/mutate-winners":
                try:
                    result = cmd_mutate_winners(
                        db_path=db_path,
                        winner_limit=int(payload.get("winner_limit") or 5),
                        variants_per_winner=int(payload.get("variants_per_winner") or 5),
                        persona=str(payload.get("persona")) if payload.get("persona") else None,
                        platform=str(payload.get("platform")) if payload.get("platform") else None,
                        tactic=str(payload.get("tactic")) if payload.get("tactic") else None,
                        objective=str(payload.get("objective")) if payload.get("objective") else None,
                        days=int(payload.get("days") or 30),
                        require_reply=bool(payload.get("require_reply", True)),
                        strategy=str(payload.get("strategy") or "controlled_v1"),
                    )
                except (KeyError, TypeError, ValueError) as exc:
                    self._write_json(400, {"error": str(exc)})
                    return
                self._write_json(200, result)
                return
            if parsed.path == "/api/mutate-run":
                try:
                    result = cmd_mutate_run(
                        int(payload["run_id"]),
                        db_path=db_path,
                        variants_per_winner=int(payload.get("variants_per_winner") or 3),
                        strategy=str(payload.get("strategy") or "controlled_v1"),
                    )
                except (KeyError, TypeError, ValueError) as exc:
                    self._write_json(400, {"error": str(exc)})
                    return
                self._write_json(200, {"ok": True, **result})
                return
            if parsed.path == "/api/daemon":
                try:
                    daemon = cmd_daemon_manage(
                        str(payload.get("action") or "status"),
                        db_path=db_path,
                        load=bool(payload.get("load")),
                        unload=bool(payload.get("unload")),
                        mode=str(payload.get("mode") or "panel"),
                        dispatch_limit=int(payload.get("dispatch_limit") or 25),
                        driver=str(payload.get("driver") or "auto"),
                        out_dir=str(payload.get("out_dir")) if payload.get("out_dir") is not None else None,
                        interval_seconds=float(payload.get("interval_seconds") or 30.0),
                        max_cycles=int(payload.get("max_cycles") or 0),
                        include_failed_redrive=bool(payload.get("include_failed_redrive")),
                        redrive_limit=int(payload.get("redrive_limit") or 10),
                        min_failed_age_seconds=float(payload.get("min_failed_age_seconds") or 0.0),
                        max_actions_per_hour=int(payload.get("max_actions_per_hour") or 0),
                        max_actions_per_day=int(payload.get("max_actions_per_day") or 0),
                        min_seconds_between_actions=float(payload.get("min_seconds_between_actions") or 0.0),
                        quiet_hours_start=int(payload["quiet_hours_start"]) if payload.get("quiet_hours_start") is not None else None,
                        quiet_hours_end=int(payload["quiet_hours_end"]) if payload.get("quiet_hours_end") is not None else None,
                    )
                except (KeyError, TypeError, ValueError) as exc:
                    self._write_json(400, {"error": str(exc)})
                    return
                self._write_json(200, {"ok": True, "daemon": daemon})
                return
            if parsed.path == "/api/stage-emit":
                try:
                    repo = RunRepository(db_path)
                    run_id_value = _resolve_panel_run_id(payload.get("run_id"), db_path)
                    staged = repo.stage_emit(
                        EmitOutboxRecord(
                            id=None,
                            run_id=run_id_value,
                            platform=str(payload["platform"]),
                            transport=str((payload.get("emit_request") or {})["transport"]),
                            selection_preset=payload.get("selection_preset"),
                            selection_strategy=payload.get("selection_strategy"),
                            tactic=payload.get("tactic"),
                            objective=payload.get("objective"),
                            status=str(payload.get("status") or "staged"),
                            envelope_json=json.dumps(payload.get("envelope") or {}, ensure_ascii=False),
                            emit_request_json=json.dumps(payload.get("emit_request") or {}, ensure_ascii=False),
                            notes=payload.get("notes"),
                        )
                    )
                except (KeyError, TypeError, ValueError) as exc:
                    self._write_json(400, {"error": str(exc)})
                    return
                latest_emit = (staged.get("emit_outbox") or [None])[0]
                self._write_json(
                    200,
                    {
                        "ok": True,
                        "run_id": run_id_value,
                        "staged_emit": latest_emit,
                        "pending_emit_count": sum(1 for item in staged.get("emit_outbox") or [] if item.get("status") == "staged"),
                    },
                )
                return
            if parsed.path == "/api/outbox-status":
                try:
                    repo = RunRepository(db_path)
                    updated_run = repo.update_emit_outbox(
                        int(payload["emit_id"]),
                        status=str(payload.get("status")) if payload.get("status") is not None else None,
                        notes=payload.get("notes") if "notes" in payload else None,
                        notes_mode=str(payload.get("notes_mode") or "append"),
                    )
                except (KeyError, TypeError, ValueError) as exc:
                    self._write_json(400, {"error": str(exc)})
                    return
                emit_id_value = int(payload["emit_id"])
                emit = next((item for item in updated_run.get("emit_outbox") or [] if int(item.get("id") or 0) == emit_id_value), None)
                self._write_json(
                    200,
                    {
                        "ok": True,
                        "emit": emit,
                        "pending_emit_count": sum(1 for item in updated_run.get("emit_outbox") or [] if item.get("status") == "staged"),
                    },
                )
                return
            if parsed.path == "/api/dispatch-emit":
                try:
                    dispatched = cmd_dispatch_emit(
                        int(payload["emit_id"]),
                        db_path=db_path,
                        driver=str(payload.get("driver") or "auto"),
                        out_dir=str(payload.get("out_dir")) if payload.get("out_dir") is not None else None,
                        notes=payload.get("notes"),
                        force=bool(payload.get("force")),
                    )
                except (KeyError, TypeError, ValueError) as exc:
                    self._write_json(400, {"error": str(exc)})
                    return
                self._write_json(200, dispatched)
                return
            if parsed.path == "/api/dispatch-approved":
                try:
                    dispatched = cmd_dispatch_approved(
                        limit=int(payload.get("limit") or 25),
                        db_path=db_path,
                        driver=str(payload.get("driver") or "auto"),
                        out_dir=str(payload.get("out_dir")) if payload.get("out_dir") is not None else None,
                        notes=payload.get("notes"),
                        include_retry_due=Boolean(payload.get("include_retry_due")),
                    )
                except (KeyError, TypeError, ValueError) as exc:
                    self._write_json(400, {"error": str(exc)})
                    return
                self._write_json(200, dispatched)
                return
            if parsed.path == "/api/dispatch-status":
                try:
                    dispatched = cmd_update_dispatch_status(
                        int(payload["dispatch_id"]),
                        status=str(payload.get("status") or "dispatched"),
                        db_path=db_path,
                        notes=payload.get("notes") if "notes" in payload else None,
                        notes_mode=str(payload.get("notes_mode") or "append"),
                    )
                except (KeyError, TypeError, ValueError) as exc:
                    self._write_json(400, {"error": str(exc)})
                    return
                self._write_json(200, dispatched)
                return
            if parsed.path == "/api/dispatch-redrive":
                try:
                    dispatched = cmd_redrive_dispatch(
                        int(payload["dispatch_id"]),
                        db_path=db_path,
                        driver=str(payload.get("driver")) if payload.get("driver") is not None else None,
                        out_dir=str(payload.get("out_dir")) if payload.get("out_dir") is not None else None,
                        notes=payload.get("notes"),
                    )
                except (KeyError, TypeError, ValueError) as exc:
                    self._write_json(400, {"error": str(exc)})
                    return
                self._write_json(200, dispatched)
                return
            if parsed.path == "/api/worker-cycle":
                try:
                    dispatched = cmd_worker_cycle(
                        db_path=db_path,
                        dispatch_limit=int(payload.get("dispatch_limit") or 25),
                        driver=str(payload.get("driver") or "auto"),
                        out_dir=str(payload.get("out_dir")) if payload.get("out_dir") is not None else None,
                        notes=payload.get("notes"),
                        include_failed_redrive=bool(payload.get("include_failed_redrive")),
                        redrive_limit=int(payload.get("redrive_limit") or 10),
                        min_failed_age_seconds=float(payload.get("min_failed_age_seconds") or 0.0),
                        max_actions_per_hour=int(payload.get("max_actions_per_hour") or 0),
                        max_actions_per_day=int(payload.get("max_actions_per_day") or 0),
                        min_seconds_between_actions=float(payload.get("min_seconds_between_actions") or 0.0),
                        quiet_hours_start=int(payload["quiet_hours_start"]) if payload.get("quiet_hours_start") is not None else None,
                        quiet_hours_end=int(payload["quiet_hours_end"]) if payload.get("quiet_hours_end") is not None else None,
                    )
                except (KeyError, TypeError, ValueError) as exc:
                    self._write_json(400, {"error": str(exc)})
                    return
                self._write_json(200, dispatched)
                return
            if self.path == "/api/score-run":
                try:
                    outcome = cmd_score_run(
                        run_id=int(payload["run_id"]),
                        got_reply=bool(payload.get("got_reply")),
                        engagement=int(payload.get("engagement") or 0),
                        tone_shift=payload.get("tone_shift"),
                        notes=payload.get("notes"),
                        db_path=db_path,
                    )
                    self._write_json(200, {"ok": True, "outcome": outcome})
                except Exception as e:
                    self._write_json(400, {"error": str(e)})
                return
            try:
                updated_run = cmd_record_panel_review(
                    _resolve_panel_run_id(payload.get("run_id"), db_path),
                    disposition=str(payload["disposition"]),
                    selection_preset=payload.get("selection_preset"),
                    selection_strategy=payload.get("selection_strategy"),
                    tactic=payload.get("tactic"),
                    objective=payload.get("objective"),
                    notes=payload.get("notes"),
                    db_path=db_path,
                )
            except (KeyError, TypeError, ValueError) as exc:
                self._write_json(400, {"error": str(exc)})
                return
            self._write_json(
                200,
                {
                    "ok": True,
                    "run_id": updated_run["id"],
                    "panel_review_count": len(updated_run.get("panel_reviews") or []),
                    "latest_review": (updated_run.get("panel_reviews") or [None])[0],
                },
            )

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            return

    return ThreadingHTTPServer((host, port), PanelHTTPRequestHandler)


def cmd_panel_serve(
    run_id: int | None = None,
    db_path: str | None = None,
    thread_id: str | None = None,
    reply_to_id: str | None = None,
    author_handle: str | None = None,
    context_json: str | None = None,
    include_all_presets: bool = True,
    include_strategy_variants: bool = False,
    include_filter_variants: bool = True,
    variant_limit: int | None = None,
    include_outcome_overlay: bool = True,
    history_limit: int = 50,
    host: str = "127.0.0.1",
    port: int = 8765,
    open_browser: bool = False,
) -> None:
    server = _create_panel_http_server(
        run_id,
        db_path=db_path,
        thread_id=thread_id,
        reply_to_id=reply_to_id,
        author_handle=author_handle,
        context_json=context_json,
        include_all_presets=include_all_presets,
        include_strategy_variants=include_strategy_variants,
        include_filter_variants=include_filter_variants,
        variant_limit=variant_limit,
        include_outcome_overlay=include_outcome_overlay,
        history_limit=history_limit,
        host=host,
        port=port,
    )
    try:
        address = f"http://{server.server_address[0]}:{server.server_address[1]}"
        active_run_id = None
        try:
            active_run_id = _resolve_panel_run_id(run_id, db_path)
        except KeyError:
            active_run_id = None
        print(json.dumps({"ok": True, "server_url": address, "run_id": active_run_id, "auto_opened": open_browser}, indent=2), flush=True)
        if open_browser:
            target_url = f"{address}/dashboard" if active_run_id is None else f"{address}/?run_id={active_run_id}"
            webbrowser.open(target_url)
        server.serve_forever()
    finally:
        server.server_close()


def main() -> None:
    parser = argparse.ArgumentParser(prog="bait-engine")
    sub = parser.add_subparsers(dest="command", required=True)

    parser.add_argument("--db", default=None)

    analyze_p = sub.add_parser("analyze")
    analyze_p.add_argument("text")
    analyze_p.add_argument("--platform", default="cli")

    plan_p = sub.add_parser("plan")
    plan_p.add_argument("text")
    plan_p.add_argument("--persona", default="dry_midwit_savant")
    plan_p.add_argument("--platform", default="cli")

    draft_p = sub.add_parser("draft")
    draft_p.add_argument("text")
    draft_p.add_argument("--persona", default="dry_midwit_savant")
    draft_p.add_argument("--count", type=int, default=5)
    draft_p.add_argument("--save", action="store_true")
    draft_p.add_argument("--platform", default="cli")
    draft_p.add_argument("--heuristic-only", action="store_true")
    draft_p.add_argument("--model", default=None)
    draft_p.add_argument("--base-url", default=None)
    draft_p.add_argument("--timeout-seconds", type=int, default=30)
    draft_p.add_argument("--mutation-source", choices=["auto", "none"], default="auto")

    runs_p = sub.add_parser("runs")
    runs_p.add_argument("--limit", type=int, default=20)

    sub.add_parser("personas")

    show_p = sub.add_parser("show-run")
    show_p.add_argument("run_id", type=int)

    replay_p = sub.add_parser("replay")
    replay_p.add_argument("run_id", type=int)
    replay_p.add_argument("--count", type=int, default=None)
    replay_p.add_argument("--heuristic-only", action="store_true")
    replay_p.add_argument("--model", default=None)
    replay_p.add_argument("--base-url", default=None)
    replay_p.add_argument("--timeout-seconds", type=int, default=30)
    replay_p.add_argument("--mutation-source", choices=["auto", "none"], default="auto")

    autopsy_p = sub.add_parser("autopsy")
    autopsy_p.add_argument("run_id", type=int)

    autopsy_many_p = sub.add_parser("autopsy-many")
    autopsy_many_p.add_argument("--limit", type=int, default=20)
    autopsy_many_p.add_argument("--persona", default=None)
    autopsy_many_p.add_argument("--platform", default=None)
    autopsy_many_p.add_argument("--verdict", choices=["engaged", "no_bite", "pending"], default=None)

    scoreboard_p = sub.add_parser("scoreboard")
    scoreboard_p.add_argument("--limit", type=int, default=100)
    scoreboard_p.add_argument("--persona", default=None)
    scoreboard_p.add_argument("--platform", default=None)
    scoreboard_p.add_argument("--verdict", choices=["engaged", "no_bite", "pending"], default=None)
    scoreboard_p.add_argument("--since-hours", type=float, default=None)

    report_p = sub.add_parser("report")
    report_p.add_argument("--limit", type=int, default=100)
    report_p.add_argument("--section-limit", type=int, default=5)
    report_p.add_argument("--persona", default=None)
    report_p.add_argument("--platform", default=None)
    report_p.add_argument("--verdict", choices=["engaged", "no_bite", "pending"], default=None)
    report_p.add_argument("--since-hours", type=float, default=None)

    report_md_p = sub.add_parser("report-markdown")
    report_md_p.add_argument("--limit", type=int, default=100)
    report_md_p.add_argument("--section-limit", type=int, default=5)
    report_md_p.add_argument("--persona", default=None)
    report_md_p.add_argument("--platform", default=None)
    report_md_p.add_argument("--verdict", choices=["engaged", "no_bite", "pending"], default=None)
    report_md_p.add_argument("--since-hours", type=float, default=None)
    report_md_p.add_argument("--out", default=None)

    report_csv_p = sub.add_parser("report-csv")
    report_csv_p.add_argument("--limit", type=int, default=100)
    report_csv_p.add_argument("--section-limit", type=int, default=5)
    report_csv_p.add_argument("--persona", default=None)
    report_csv_p.add_argument("--platform", default=None)
    report_csv_p.add_argument("--verdict", choices=["engaged", "no_bite", "pending"], default=None)
    report_csv_p.add_argument("--since-hours", type=float, default=None)
    report_csv_p.add_argument("--out", default=None)

    operator_status_p = sub.add_parser("operator-status")
    operator_status_p.add_argument("--db", default=None)

    preflight_p = sub.add_parser("preflight")
    preflight_p.add_argument("--db", default=None)
    preflight_p.add_argument("--dead-letter-fail-threshold", type=int, default=10)
    preflight_p.add_argument("--waiting-retry-fail-threshold", type=int, default=50)
    preflight_p.add_argument("--critical-alert-fail-threshold", type=int, default=1)

    adapters_p = sub.add_parser("adapters")

    adapter_p = sub.add_parser("adapter")
    adapter_p.add_argument("platform")

    context_preview_p = sub.add_parser("context-preview")
    context_preview_p.add_argument("platform")
    context_preview_p.add_argument("thread_id")
    context_preview_p.add_argument("--subject", default=None)
    context_preview_p.add_argument("--messages-json", default=None)

    target_preview_p = sub.add_parser("target-preview")
    target_preview_p.add_argument("platform")
    target_preview_p.add_argument("--thread-id", default=None)
    target_preview_p.add_argument("--reply-to-id", default=None)
    target_preview_p.add_argument("--author-handle", default=None)

    adapter_preview_p = sub.add_parser("adapter-preview")
    adapter_preview_p.add_argument("run_id", type=int)
    adapter_preview_p.add_argument("--candidate-rank-index", type=int, default=1)
    adapter_preview_p.add_argument("--selection-strategy", choices=["rank", "top_score", "highest_bite", "highest_audience", "lowest_penalty", "auto_best", "blend_top3"], default="rank")
    adapter_preview_p.add_argument("--selection-preset", default=None)
    adapter_preview_p.add_argument("--tactic", default=None)
    adapter_preview_p.add_argument("--objective", default=None)
    adapter_preview_p.add_argument("--thread-id", default=None)
    adapter_preview_p.add_argument("--reply-to-id", default=None)
    adapter_preview_p.add_argument("--author-handle", default=None)
    adapter_preview_p.add_argument("--context-json", default=None)

    recommend_p = sub.add_parser("recommend-preset")
    recommend_p.add_argument("platform")
    recommend_p.add_argument("--context-json", default=None)

    emit_preview_p = sub.add_parser("emit-preview")
    emit_preview_p.add_argument("run_id", type=int)
    emit_preview_p.add_argument("--candidate-rank-index", type=int, default=1)
    emit_preview_p.add_argument("--selection-strategy", choices=["rank", "top_score", "highest_bite", "highest_audience", "lowest_penalty", "auto_best", "blend_top3"], default="rank")
    emit_preview_p.add_argument("--selection-preset", default=None)
    emit_preview_p.add_argument("--tactic", default=None)
    emit_preview_p.add_argument("--objective", default=None)
    emit_preview_p.add_argument("--thread-id", default=None)
    emit_preview_p.add_argument("--reply-to-id", default=None)
    emit_preview_p.add_argument("--author-handle", default=None)
    emit_preview_p.add_argument("--context-json", default=None)

    panel_preview_p = sub.add_parser("panel-preview")
    panel_preview_p.add_argument("run_id", type=int)
    panel_preview_p.add_argument("--thread-id", default=None)
    panel_preview_p.add_argument("--reply-to-id", default=None)
    panel_preview_p.add_argument("--author-handle", default=None)
    panel_preview_p.add_argument("--context-json", default=None)
    panel_preview_p.add_argument("--out", default=None)
    panel_preview_p.add_argument("--include-all-presets", action=argparse.BooleanOptionalAction, default=True)
    panel_preview_p.add_argument("--include-strategy-variants", action=argparse.BooleanOptionalAction, default=False)
    panel_preview_p.add_argument("--include-filter-variants", action=argparse.BooleanOptionalAction, default=True)
    panel_preview_p.add_argument("--variant-limit", type=int, default=None)
    panel_preview_p.add_argument("--include-outcome-overlay", action=argparse.BooleanOptionalAction, default=True)
    panel_preview_p.add_argument("--history-limit", type=int, default=50)

    panel_serve_p = sub.add_parser("panel-serve")
    panel_serve_p.add_argument("run_id", type=int, nargs="?", default=None)
    panel_serve_p.add_argument("--thread-id", default=None)
    panel_serve_p.add_argument("--reply-to-id", default=None)
    panel_serve_p.add_argument("--author-handle", default=None)
    panel_serve_p.add_argument("--context-json", default=None)
    panel_serve_p.add_argument("--include-all-presets", action=argparse.BooleanOptionalAction, default=True)
    panel_serve_p.add_argument("--include-strategy-variants", action=argparse.BooleanOptionalAction, default=False)
    panel_serve_p.add_argument("--include-filter-variants", action=argparse.BooleanOptionalAction, default=True)
    panel_serve_p.add_argument("--variant-limit", type=int, default=None)
    panel_serve_p.add_argument("--include-outcome-overlay", action=argparse.BooleanOptionalAction, default=True)
    panel_serve_p.add_argument("--history-limit", type=int, default=50)
    panel_serve_p.add_argument("--host", default="127.0.0.1")
    panel_serve_p.add_argument("--port", type=int, default=8765)
    panel_serve_p.add_argument("--open", action=argparse.BooleanOptionalAction, default=False)

    daemon_p = sub.add_parser("daemon")
    daemon_p.add_argument("action", choices=["status", "install", "uninstall"])
    daemon_p.add_argument("--mode", choices=["panel", "worker"], default="panel")
    daemon_p.add_argument("--host", default="127.0.0.1")
    daemon_p.add_argument("--port", type=int, default=8765)
    daemon_p.add_argument("--label", default=None)
    daemon_p.add_argument("--launch-agents-dir", default=None)
    daemon_p.add_argument("--log-dir", default=None)
    daemon_p.add_argument("--load", action=argparse.BooleanOptionalAction, default=False)
    daemon_p.add_argument("--unload", action=argparse.BooleanOptionalAction, default=False)
    daemon_p.add_argument("--dispatch-limit", type=int, default=25)
    daemon_p.add_argument("--driver", default="auto")
    daemon_p.add_argument("--out-dir", default=None)
    daemon_p.add_argument("--interval-seconds", type=float, default=30.0)
    daemon_p.add_argument("--max-cycles", type=int, default=0)
    daemon_p.add_argument("--include-failed-redrive", action=argparse.BooleanOptionalAction, default=False)
    daemon_p.add_argument("--redrive-limit", type=int, default=10)
    daemon_p.add_argument("--min-failed-age-seconds", type=float, default=0.0)
    daemon_p.add_argument("--max-actions-per-hour", type=int, default=0)
    daemon_p.add_argument("--max-actions-per-day", type=int, default=0)
    daemon_p.add_argument("--min-seconds-between-actions", type=float, default=0.0)
    daemon_p.add_argument("--quiet-hours-start", type=int, default=None)
    daemon_p.add_argument("--quiet-hours-end", type=int, default=None)

    outbox_p = sub.add_parser("outbox")
    outbox_p.add_argument("--limit", type=int, default=100)
    outbox_p.add_argument("--status", default=None)

    dispatch_p = sub.add_parser("dispatch-emit")
    dispatch_p.add_argument("emit_id", type=int)
    dispatch_p.add_argument("--driver", default="auto")
    dispatch_p.add_argument("--out-dir", default=None)
    dispatch_p.add_argument("--notes", default=None)
    dispatch_p.add_argument("--force", action=argparse.BooleanOptionalAction, default=False)

    dispatch_approved_p = sub.add_parser("dispatch-approved")
    dispatch_approved_p.add_argument("--limit", type=int, default=25)
    dispatch_approved_p.add_argument("--driver", default="auto")
    dispatch_approved_p.add_argument("--out-dir", default=None)
    dispatch_approved_p.add_argument("--notes", default=None)
    dispatch_approved_p.add_argument("--include-retry-due", action=argparse.BooleanOptionalAction, default=False)

    dispatch_status_p = sub.add_parser("dispatch-status")
    dispatch_status_p.add_argument("dispatch_id", type=int)
    dispatch_status_p.add_argument("status", choices=["dispatched", "acknowledged", "delivered", "failed", "dead_letter"])
    dispatch_status_p.add_argument("--notes", default=None)
    dispatch_status_p.add_argument("--notes-mode", default="append", choices=["append", "replace"])

    redrive_dispatch_p = sub.add_parser("redrive-dispatch")
    redrive_dispatch_p.add_argument("dispatch_id", type=int)
    redrive_dispatch_p.add_argument("--driver", default=None)
    redrive_dispatch_p.add_argument("--out-dir", default=None)
    redrive_dispatch_p.add_argument("--notes", default=None)

    worker_cycle_p = sub.add_parser("worker-cycle")
    worker_cycle_p.add_argument("--dispatch-limit", type=int, default=25)
    worker_cycle_p.add_argument("--driver", default="auto")
    worker_cycle_p.add_argument("--out-dir", default=None)
    worker_cycle_p.add_argument("--notes", default=None)
    worker_cycle_p.add_argument("--include-failed-redrive", action=argparse.BooleanOptionalAction, default=False)
    worker_cycle_p.add_argument("--redrive-limit", type=int, default=10)
    worker_cycle_p.add_argument("--min-failed-age-seconds", type=float, default=0.0)
    worker_cycle_p.add_argument("--max-actions-per-hour", type=int, default=0)
    worker_cycle_p.add_argument("--max-actions-per-day", type=int, default=0)
    worker_cycle_p.add_argument("--min-seconds-between-actions", type=float, default=0.0)
    worker_cycle_p.add_argument("--quiet-hours-start", type=int, default=None)
    worker_cycle_p.add_argument("--quiet-hours-end", type=int, default=None)

    hunt_preview_p = sub.add_parser("hunt-preview")
    hunt_preview_p.add_argument("source", choices=supported_hunt_sources())
    hunt_preview_p.add_argument("--subreddit", default=None)
    hunt_preview_p.add_argument("--sort", default="new")
    hunt_preview_p.add_argument("--query", default=None)
    hunt_preview_p.add_argument("--limit", type=int, default=25)
    hunt_preview_p.add_argument("--file-path", default=None)
    hunt_preview_p.add_argument("--access-token", default=None)
    hunt_preview_p.add_argument("--bearer-token", default=None)
    hunt_preview_p.add_argument("--user-agent", default=None)
    hunt_preview_p.add_argument("--timeout-seconds", type=float, default=15.0)
    hunt_preview_p.add_argument("--persona", default="dry_midwit_savant")
    hunt_preview_p.add_argument("--prior-days", type=int, default=30)

    hunt_list_p = sub.add_parser("hunt-list")
    hunt_list_p.add_argument("--limit", type=int, default=100)
    hunt_list_p.add_argument("--status", default=None)
    hunt_list_p.add_argument("--platform", default=None)

    mutate_winners_p = sub.add_parser("mutate-winners")
    mutate_winners_p.add_argument("--winner-limit", type=int, default=5)
    mutate_winners_p.add_argument("--variants-per-winner", type=int, default=5)
    mutate_winners_p.add_argument("--persona", default=None)
    mutate_winners_p.add_argument("--platform", default=None)
    mutate_winners_p.add_argument("--tactic", default=None)
    mutate_winners_p.add_argument("--objective", default=None)
    mutate_winners_p.add_argument("--days", type=int, default=30)
    mutate_winners_p.add_argument("--require-reply", action=argparse.BooleanOptionalAction, default=True)
    mutate_winners_p.add_argument("--strategy", default="controlled_v1")

    mutate_run_p = sub.add_parser("mutate-run")
    mutate_run_p.add_argument("run_id", type=int)
    mutate_run_p.add_argument("--variants-per-winner", type=int, default=5)
    mutate_run_p.add_argument("--strategy", default="controlled_v1")

    mutation_report_p = sub.add_parser("mutation-report")
    mutation_report_p.add_argument("--limit", type=int, default=250)
    mutation_report_p.add_argument("--persona", default=None)
    mutation_report_p.add_argument("--platform", default=None)
    mutation_report_p.add_argument("--tactic", default=None)
    mutation_report_p.add_argument("--objective", default=None)
    mutation_report_p.add_argument("--status", default=None)

    hunt_promote_p = sub.add_parser("hunt-promote")
    hunt_promote_p.add_argument("target_id", type=int)
    hunt_promote_p.add_argument("--persona", default="dry_midwit_savant")
    hunt_promote_p.add_argument("--count", type=int, default=5)
    hunt_promote_p.add_argument("--heuristic-only", action=argparse.BooleanOptionalAction, default=None)
    hunt_promote_p.add_argument("--model", default=None)
    hunt_promote_p.add_argument("--base-url", default=None)
    hunt_promote_p.add_argument("--timeout-seconds", type=int, default=30)
    hunt_promote_p.add_argument("--stage-emit", action=argparse.BooleanOptionalAction, default=True)
    hunt_promote_p.add_argument("--approve-emit", action=argparse.BooleanOptionalAction, default=False)
    hunt_promote_p.add_argument("--selection-preset", default="auto")
    hunt_promote_p.add_argument("--selection-strategy", default="rank")
    hunt_promote_p.add_argument("--tactic", default=None)
    hunt_promote_p.add_argument("--objective", default=None)
    hunt_promote_p.add_argument("--force", action=argparse.BooleanOptionalAction, default=False)
    hunt_promote_p.add_argument("--dispatch-approved", action=argparse.BooleanOptionalAction, default=False)
    hunt_promote_p.add_argument("--dispatch-limit", type=int, default=25)
    hunt_promote_p.add_argument("--driver", default="auto")
    hunt_promote_p.add_argument("--out-dir", default=None)
    hunt_promote_p.add_argument("--notes", default=None)

    hunt_cycle_p = sub.add_parser("hunt-cycle")
    hunt_cycle_p.add_argument("source", choices=supported_hunt_sources())
    hunt_cycle_p.add_argument("--subreddit", default=None)
    hunt_cycle_p.add_argument("--sort", default="new")
    hunt_cycle_p.add_argument("--query", default=None)
    hunt_cycle_p.add_argument("--limit", type=int, default=25)
    hunt_cycle_p.add_argument("--file-path", default=None)
    hunt_cycle_p.add_argument("--access-token", default=None)
    hunt_cycle_p.add_argument("--bearer-token", default=None)
    hunt_cycle_p.add_argument("--user-agent", default=None)
    hunt_cycle_p.add_argument("--timeout-seconds", type=float, default=15.0)
    hunt_cycle_p.add_argument("--save-limit", type=int, default=None)
    hunt_cycle_p.add_argument("--promote-limit", type=int, default=3)
    hunt_cycle_p.add_argument("--persona", default="dry_midwit_savant")
    hunt_cycle_p.add_argument("--count", type=int, default=5)
    hunt_cycle_p.add_argument("--heuristic-only", action=argparse.BooleanOptionalAction, default=None)
    hunt_cycle_p.add_argument("--model", default=None)
    hunt_cycle_p.add_argument("--base-url", default=None)
    hunt_cycle_p.add_argument("--generation-timeout-seconds", type=int, default=30)
    hunt_cycle_p.add_argument("--stage-emit", action=argparse.BooleanOptionalAction, default=True)
    hunt_cycle_p.add_argument("--approve-emit", action=argparse.BooleanOptionalAction, default=False)
    hunt_cycle_p.add_argument("--selection-preset", default="auto")
    hunt_cycle_p.add_argument("--selection-strategy", default="rank")
    hunt_cycle_p.add_argument("--tactic", default=None)
    hunt_cycle_p.add_argument("--objective", default=None)
    hunt_cycle_p.add_argument("--dispatch-approved", action=argparse.BooleanOptionalAction, default=False)
    hunt_cycle_p.add_argument("--dispatch-limit", type=int, default=25)
    hunt_cycle_p.add_argument("--driver", default="auto")
    hunt_cycle_p.add_argument("--out-dir", default=None)
    hunt_cycle_p.add_argument("--notes", default=None)
    hunt_cycle_p.add_argument("--prior-days", type=int, default=30)

    hunt_run_p = sub.add_parser("hunt-run")
    hunt_run_p.add_argument("source", choices=supported_hunt_sources())
    hunt_run_p.add_argument("--subreddit", default=None)
    hunt_run_p.add_argument("--sort", default="new")
    hunt_run_p.add_argument("--query", default=None)
    hunt_run_p.add_argument("--limit", type=int, default=25)
    hunt_run_p.add_argument("--file-path", default=None)
    hunt_run_p.add_argument("--access-token", default=None)
    hunt_run_p.add_argument("--bearer-token", default=None)
    hunt_run_p.add_argument("--user-agent", default=None)
    hunt_run_p.add_argument("--timeout-seconds", type=float, default=15.0)
    hunt_run_p.add_argument("--save-limit", type=int, default=None)
    hunt_run_p.add_argument("--promote-limit", type=int, default=3)
    hunt_run_p.add_argument("--persona", default="dry_midwit_savant")
    hunt_run_p.add_argument("--count", type=int, default=5)
    hunt_run_p.add_argument("--heuristic-only", action=argparse.BooleanOptionalAction, default=None)
    hunt_run_p.add_argument("--model", default=None)
    hunt_run_p.add_argument("--base-url", default=None)
    hunt_run_p.add_argument("--generation-timeout-seconds", type=int, default=30)
    hunt_run_p.add_argument("--stage-emit", action=argparse.BooleanOptionalAction, default=True)
    hunt_run_p.add_argument("--approve-emit", action=argparse.BooleanOptionalAction, default=False)
    hunt_run_p.add_argument("--selection-preset", default="auto")
    hunt_run_p.add_argument("--selection-strategy", default="rank")
    hunt_run_p.add_argument("--tactic", default=None)
    hunt_run_p.add_argument("--objective", default=None)
    hunt_run_p.add_argument("--dispatch-approved", action=argparse.BooleanOptionalAction, default=False)
    hunt_run_p.add_argument("--dispatch-limit", type=int, default=25)
    hunt_run_p.add_argument("--driver", default="auto")
    hunt_run_p.add_argument("--out-dir", default=None)
    hunt_run_p.add_argument("--notes", default=None)
    hunt_run_p.add_argument("--prior-days", type=int, default=30)
    hunt_run_p.add_argument("--interval-seconds", type=float, default=60.0)
    hunt_run_p.add_argument("--max-cycles", type=int, default=0)

    worker_run_p = sub.add_parser("worker-run")
    worker_run_p.add_argument("--dispatch-limit", type=int, default=25)
    worker_run_p.add_argument("--driver", default="auto")
    worker_run_p.add_argument("--out-dir", default=None)
    worker_run_p.add_argument("--notes", default=None)
    worker_run_p.add_argument("--interval-seconds", type=float, default=30.0)
    worker_run_p.add_argument("--max-cycles", type=int, default=0)
    worker_run_p.add_argument("--include-failed-redrive", action=argparse.BooleanOptionalAction, default=False)
    worker_run_p.add_argument("--redrive-limit", type=int, default=10)
    worker_run_p.add_argument("--min-failed-age-seconds", type=float, default=0.0)
    worker_run_p.add_argument("--max-actions-per-hour", type=int, default=0)
    worker_run_p.add_argument("--max-actions-per-day", type=int, default=0)
    worker_run_p.add_argument("--min-seconds-between-actions", type=float, default=0.0)
    worker_run_p.add_argument("--quiet-hours-start", type=int, default=None)
    worker_run_p.add_argument("--quiet-hours-end", type=int, default=None)

    panel_review_p = sub.add_parser("record-panel-review")
    panel_review_p.add_argument("run_id", type=int)
    panel_review_p.add_argument("--disposition", choices=["promote", "favorite", "avoid"], required=True)
    panel_review_p.add_argument("--selection-preset", default=None)
    panel_review_p.add_argument("--selection-strategy", default=None)
    panel_review_p.add_argument("--tactic", default=None)
    panel_review_p.add_argument("--objective", default=None)
    panel_review_p.add_argument("--notes", default=None)

    outcome_p = sub.add_parser("record-outcome")
    outcome_p.add_argument("run_id", type=int)
    outcome_p.add_argument("--got-reply", action="store_true")
    outcome_p.add_argument("--reply-delay-seconds", type=int, default=None)
    outcome_p.add_argument("--reply-length", type=int, default=None)
    outcome_p.add_argument("--tone-shift", default=None)
    outcome_p.add_argument("--spectator-engagement", type=int, default=None)
    outcome_p.add_argument("--result-label", default=None)
    outcome_p.add_argument("--notes", default=None)

    args = parser.parse_args()

    if args.command == "analyze":
        out = cmd_analyze(args.text, platform=args.platform)
    elif args.command == "plan":
        out = cmd_plan(args.text, args.persona, platform=args.platform)
    elif args.command == "draft":
        out = cmd_draft(
            args.text,
            args.persona,
            args.count,
            save=args.save,
            db_path=args.db,
            platform=args.platform,
            heuristic_only=args.heuristic_only,
            model=args.model,
            base_url=args.base_url,
            timeout_seconds=args.timeout_seconds,
            mutation_source=args.mutation_source,
        )
    elif args.command == "hunt-preview":
        out = cmd_hunt_preview(
            args.source,
            subreddit=args.subreddit,
            sort=args.sort,
            query=args.query,
            limit=args.limit,
            file_path=args.file_path,
            access_token=args.access_token,
            bearer_token=args.bearer_token,
            user_agent=args.user_agent,
            timeout_seconds=args.timeout_seconds,
            db_path=args.db,
            persona_name=args.persona,
            prior_days=args.prior_days,
        )
    elif args.command == "hunt-list":
        out = cmd_hunt_list(db_path=args.db, limit=args.limit, status=args.status, platform=args.platform)
    elif args.command == "mutate-winners":
        out = cmd_mutate_winners(
            db_path=args.db,
            winner_limit=args.winner_limit,
            variants_per_winner=args.variants_per_winner,
            persona=args.persona,
            platform=args.platform,
            tactic=args.tactic,
            objective=args.objective,
            days=args.days,
            require_reply=args.require_reply,
            strategy=args.strategy,
        )
    elif args.command == "mutate-run":
        out = cmd_mutate_run(
            args.run_id,
            db_path=args.db,
            variants_per_winner=args.variants_per_winner,
            strategy=args.strategy,
        )
    elif args.command == "mutation-report":
        out = cmd_mutation_report(
            db_path=args.db,
            limit=args.limit,
            persona=args.persona,
            platform=args.platform,
            tactic=args.tactic,
            objective=args.objective,
            status=args.status,
        )
    elif args.command == "hunt-promote":
        out = cmd_hunt_promote(
            args.target_id,
            db_path=args.db,
            persona_name=args.persona,
            candidate_count=args.count,
            heuristic_only=args.heuristic_only,
            model=args.model,
            base_url=args.base_url,
            timeout_seconds=args.timeout_seconds,
            stage_emit=args.stage_emit,
            approve_emit=args.approve_emit,
            selection_preset=args.selection_preset,
            selection_strategy=args.selection_strategy,
            tactic=args.tactic,
            objective=args.objective,
            force=args.force,
            dispatch_approved=args.dispatch_approved,
            dispatch_limit=args.dispatch_limit,
            driver=args.driver,
            out_dir=args.out_dir,
            notes=args.notes,
        )
    elif args.command == "hunt-cycle":
        out = cmd_hunt_cycle(
            args.source,
            db_path=args.db,
            subreddit=args.subreddit,
            sort=args.sort,
            query=args.query,
            limit=args.limit,
            file_path=args.file_path,
            access_token=args.access_token,
            bearer_token=args.bearer_token,
            user_agent=args.user_agent,
            timeout_seconds=args.timeout_seconds,
            save_limit=args.save_limit,
            promote_limit=args.promote_limit,
            persona_name=args.persona,
            candidate_count=args.count,
            heuristic_only=args.heuristic_only,
            model=args.model,
            base_url=args.base_url,
            generation_timeout_seconds=args.generation_timeout_seconds,
            stage_emit=args.stage_emit,
            approve_emit=args.approve_emit,
            selection_preset=args.selection_preset,
            selection_strategy=args.selection_strategy,
            tactic=args.tactic,
            objective=args.objective,
            dispatch_approved=args.dispatch_approved,
            dispatch_limit=args.dispatch_limit,
            driver=args.driver,
            out_dir=args.out_dir,
            notes=args.notes,
            prior_days=args.prior_days,
        )
    elif args.command == "hunt-run":
        out = cmd_hunt_run(
            args.source,
            db_path=args.db,
            subreddit=args.subreddit,
            sort=args.sort,
            query=args.query,
            limit=args.limit,
            file_path=args.file_path,
            access_token=args.access_token,
            bearer_token=args.bearer_token,
            user_agent=args.user_agent,
            timeout_seconds=args.timeout_seconds,
            save_limit=args.save_limit,
            promote_limit=args.promote_limit,
            persona_name=args.persona,
            candidate_count=args.count,
            heuristic_only=args.heuristic_only,
            model=args.model,
            base_url=args.base_url,
            generation_timeout_seconds=args.generation_timeout_seconds,
            stage_emit=args.stage_emit,
            approve_emit=args.approve_emit,
            selection_preset=args.selection_preset,
            selection_strategy=args.selection_strategy,
            tactic=args.tactic,
            objective=args.objective,
            dispatch_approved=args.dispatch_approved,
            dispatch_limit=args.dispatch_limit,
            driver=args.driver,
            out_dir=args.out_dir,
            notes=args.notes,
            prior_days=args.prior_days,
            interval_seconds=args.interval_seconds,
            max_cycles=args.max_cycles,
        )
    elif args.command == "runs":
        out = cmd_runs(args.limit, db_path=args.db)
    elif args.command == "personas":
        out = cmd_personas()
    elif args.command == "show-run":
        out = cmd_show_run(args.run_id, db_path=args.db)
    elif args.command == "replay":
        out = cmd_replay(
            args.run_id,
            candidate_count=args.count,
            db_path=args.db,
            heuristic_only=args.heuristic_only,
            model=args.model,
            base_url=args.base_url,
            timeout_seconds=args.timeout_seconds,
            mutation_source=args.mutation_source,
        )
    elif args.command == "autopsy":
        out = cmd_autopsy(args.run_id, db_path=args.db)
    elif args.command == "autopsy-many":
        out = cmd_autopsy_many(args.limit, db_path=args.db, persona=args.persona, platform=args.platform, verdict=args.verdict)
    elif args.command == "scoreboard":
        out = cmd_scoreboard(
            args.limit,
            db_path=args.db,
            persona=args.persona,
            platform=args.platform,
            verdict=args.verdict,
            since_hours=args.since_hours,
        )
    elif args.command == "report":
        out = cmd_report(
            args.limit,
            section_limit=args.section_limit,
            db_path=args.db,
            persona=args.persona,
            platform=args.platform,
            verdict=args.verdict,
            since_hours=args.since_hours,
        )
    elif args.command == "report-markdown":
        out = cmd_report_markdown(
            args.limit,
            section_limit=args.section_limit,
            db_path=args.db,
            persona=args.persona,
            platform=args.platform,
            verdict=args.verdict,
            since_hours=args.since_hours,
            out_path=args.out,
        )
    elif args.command == "report-csv":
        out = cmd_report_csv(
            args.limit,
            section_limit=args.section_limit,
            db_path=args.db,
            persona=args.persona,
            platform=args.platform,
            verdict=args.verdict,
            since_hours=args.since_hours,
            out_path=args.out,
        )
    elif args.command == "operator-status":
        out = cmd_operator_status(db_path=args.db)
    elif args.command == "preflight":
        out = cmd_preflight(
            db_path=args.db,
            dead_letter_fail_threshold=args.dead_letter_fail_threshold,
            waiting_retry_fail_threshold=args.waiting_retry_fail_threshold,
            critical_alert_fail_threshold=args.critical_alert_fail_threshold,
        )
    elif args.command == "adapters":
        out = cmd_adapters()
    elif args.command == "adapter":
        out = cmd_adapter(args.platform)
    elif args.command == "context-preview":
        out = cmd_context_preview(args.platform, args.thread_id, subject=args.subject, messages_json=args.messages_json)
    elif args.command == "target-preview":
        out = cmd_target_preview(args.platform, thread_id=args.thread_id, reply_to_id=args.reply_to_id, author_handle=args.author_handle)
    elif args.command == "adapter-preview":
        out = cmd_adapter_preview(
            args.run_id,
            candidate_rank_index=args.candidate_rank_index,
            selection_strategy=args.selection_strategy,
            selection_preset=args.selection_preset,
            tactic=args.tactic,
            objective=args.objective,
            db_path=args.db,
            thread_id=args.thread_id,
            reply_to_id=args.reply_to_id,
            author_handle=args.author_handle,
            context_json=args.context_json,
        )
    elif args.command == "recommend-preset":
        out = cmd_recommend_preset(args.platform, context_json=args.context_json)
    elif args.command == "emit-preview":
        out = cmd_emit_preview(
            args.run_id,
            candidate_rank_index=args.candidate_rank_index,
            selection_strategy=args.selection_strategy,
            selection_preset=args.selection_preset,
            tactic=args.tactic,
            objective=args.objective,
            db_path=args.db,
            thread_id=args.thread_id,
            reply_to_id=args.reply_to_id,
            author_handle=args.author_handle,
            context_json=args.context_json,
        )
    elif args.command == "panel-preview":
        out = cmd_panel_preview(
            args.run_id,
            db_path=args.db,
            thread_id=args.thread_id,
            reply_to_id=args.reply_to_id,
            author_handle=args.author_handle,
            context_json=args.context_json,
            out_path=args.out,
            include_all_presets=args.include_all_presets,
            include_strategy_variants=args.include_strategy_variants,
            include_filter_variants=args.include_filter_variants,
            variant_limit=args.variant_limit,
            include_outcome_overlay=args.include_outcome_overlay,
            history_limit=args.history_limit,
        )
    elif args.command == "panel-serve":
        cmd_panel_serve(
            args.run_id,
            db_path=args.db,
            thread_id=args.thread_id,
            reply_to_id=args.reply_to_id,
            author_handle=args.author_handle,
            context_json=args.context_json,
            include_all_presets=args.include_all_presets,
            include_strategy_variants=args.include_strategy_variants,
            include_filter_variants=args.include_filter_variants,
            variant_limit=args.variant_limit,
            include_outcome_overlay=args.include_outcome_overlay,
            history_limit=args.history_limit,
            host=args.host,
            port=args.port,
            open_browser=args.open,
        )
        return
    elif args.command == "daemon":
        out = cmd_daemon_manage(
            args.action,
            db_path=args.db,
            host=args.host,
            port=args.port,
            label=args.label,
            launch_agents_dir=args.launch_agents_dir,
            log_dir=args.log_dir,
            load=args.load,
            unload=args.unload,
            mode=args.mode,
            dispatch_limit=args.dispatch_limit,
            driver=args.driver,
            out_dir=args.out_dir,
            interval_seconds=args.interval_seconds,
            max_cycles=args.max_cycles,
            include_failed_redrive=args.include_failed_redrive,
            redrive_limit=args.redrive_limit,
            min_failed_age_seconds=args.min_failed_age_seconds,
            max_actions_per_hour=args.max_actions_per_hour,
            max_actions_per_day=args.max_actions_per_day,
            min_seconds_between_actions=args.min_seconds_between_actions,
            quiet_hours_start=args.quiet_hours_start,
            quiet_hours_end=args.quiet_hours_end,
        )
    elif args.command == "outbox":
        out = cmd_outbox(limit=args.limit, db_path=args.db, status=args.status)
    elif args.command == "dispatch-emit":
        out = cmd_dispatch_emit(args.emit_id, db_path=args.db, driver=args.driver, out_dir=args.out_dir, notes=args.notes, force=args.force)
    elif args.command == "dispatch-approved":
        out = cmd_dispatch_approved(
            limit=args.limit,
            db_path=args.db,
            driver=args.driver,
            out_dir=args.out_dir,
            notes=args.notes,
            include_retry_due=args.include_retry_due,
        )
    elif args.command == "dispatch-status":
        out = cmd_update_dispatch_status(args.dispatch_id, status=args.status, db_path=args.db, notes=args.notes, notes_mode=args.notes_mode)
    elif args.command == "redrive-dispatch":
        out = cmd_redrive_dispatch(args.dispatch_id, db_path=args.db, driver=args.driver, out_dir=args.out_dir, notes=args.notes)
    elif args.command == "worker-cycle":
        out = cmd_worker_cycle(
            db_path=args.db,
            dispatch_limit=args.dispatch_limit,
            driver=args.driver,
            out_dir=args.out_dir,
            notes=args.notes,
            include_failed_redrive=args.include_failed_redrive,
            redrive_limit=args.redrive_limit,
            min_failed_age_seconds=args.min_failed_age_seconds,
            max_actions_per_hour=args.max_actions_per_hour,
            max_actions_per_day=args.max_actions_per_day,
            min_seconds_between_actions=args.min_seconds_between_actions,
            quiet_hours_start=args.quiet_hours_start,
            quiet_hours_end=args.quiet_hours_end,
        )
    elif args.command == "worker-run":
        out = cmd_worker_run(
            db_path=args.db,
            dispatch_limit=args.dispatch_limit,
            driver=args.driver,
            out_dir=args.out_dir,
            notes=args.notes,
            interval_seconds=args.interval_seconds,
            max_cycles=args.max_cycles,
            include_failed_redrive=args.include_failed_redrive,
            redrive_limit=args.redrive_limit,
            min_failed_age_seconds=args.min_failed_age_seconds,
            max_actions_per_hour=args.max_actions_per_hour,
            max_actions_per_day=args.max_actions_per_day,
            min_seconds_between_actions=args.min_seconds_between_actions,
            quiet_hours_start=args.quiet_hours_start,
            quiet_hours_end=args.quiet_hours_end,
        )
    elif args.command == "record-panel-review":
        out = cmd_record_panel_review(
            args.run_id,
            disposition=args.disposition,
            selection_preset=args.selection_preset,
            selection_strategy=args.selection_strategy,
            tactic=args.tactic,
            objective=args.objective,
            notes=args.notes,
            db_path=args.db,
        )
    else:
        out = cmd_record_outcome(
            args.run_id,
            got_reply=args.got_reply,
            reply_delay_seconds=args.reply_delay_seconds,
            reply_length=args.reply_length,
            tone_shift=args.tone_shift,
            spectator_engagement=args.spectator_engagement,
            result_label=args.result_label,
            notes=args.notes,
            db_path=args.db,
        )

    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
