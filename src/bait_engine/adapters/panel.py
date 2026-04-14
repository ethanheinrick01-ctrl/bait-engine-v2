from __future__ import annotations

import html
import json
import shlex
from typing import Any

from bait_engine.adapters.compiler import build_reply_envelope
from bait_engine.adapters.emitters import build_emit_request
from bait_engine.adapters.inbound import InboundThreadContext, summarize_thread_context
from bait_engine.adapters.recommend import recommend_selection_preset
from bait_engine.adapters.registry import DEFAULT_ADAPTERS
from bait_engine.adapters.select import SelectionStrategy
from bait_engine.storage import summarize_run

_SELECTION_STRATEGIES: tuple[SelectionStrategy, ...] = (
    "rank",
    "top_score",
    "highest_bite",
    "highest_audience",
    "lowest_penalty",
    "auto_best",
    "blend_top3",
    "mega_bait",
)


def _classify_history_confidence(matching_runs: int) -> str:
    if matching_runs >= 6:
        return "high"
    if matching_runs >= 3:
        return "medium"
    if matching_runs >= 1:
        return "low"
    return "none"


def _extract_filter_options(run: dict[str, Any]) -> dict[str, list[str]]:
    candidates = run.get("candidates") or []
    tactics = sorted({str(candidate["tactic"]) for candidate in candidates if candidate.get("tactic")})
    objectives = sorted({str(candidate["objective"]) for candidate in candidates if candidate.get("objective")})
    return {
        "tactics": tactics,
        "objectives": objectives,
    }


def _build_outcome_overlay(
    run: dict[str, Any],
    envelope: dict[str, Any],
    history_runs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if not history_runs:
        return {
            "history_runs_considered": 0,
            "matching_runs": 0,
            "engaged": 0,
            "no_bite": 0,
            "pending": 0,
            "engagement_rate": 0.0,
            "avg_reply_delay_seconds": None,
            "avg_spectator_engagement": None,
            "coverage_score": 0.0,
            "confidence": "none",
            "historical_score": 0.0,
            "recent_matches": [],
            "basis": {
                "platform": run.get("platform"),
                "persona": run.get("persona"),
                "objective": envelope.get("metadata", {}).get("candidate_objective"),
                "tactic": envelope.get("tactic"),
            },
        }

    basis = {
        "platform": run.get("platform"),
        "persona": run.get("persona"),
        "objective": envelope.get("metadata", {}).get("candidate_objective"),
        "tactic": envelope.get("tactic"),
    }
    matches: list[dict[str, Any]] = []
    for item in history_runs:
        if item.get("id") == run.get("id"):
            continue
        summary = summarize_run(item)
        if summary.get("platform") != basis["platform"]:
            continue
        if summary.get("persona") != basis["persona"]:
            continue
        if basis["objective"] is not None and summary.get("objective") != basis["objective"]:
            continue
        if basis["tactic"] is not None and summary.get("tactic") != basis["tactic"]:
            continue
        matches.append(summary)

    engaged = sum(1 for match in matches if match.get("verdict") == "engaged")
    no_bite = sum(1 for match in matches if match.get("verdict") == "no_bite")
    pending = sum(1 for match in matches if match.get("verdict") == "pending")
    completed = engaged + no_bite

    reply_delays = [
        int(match["outcome"]["reply_delay_seconds"])
        for match in matches
        if match.get("outcome") and match["outcome"].get("reply_delay_seconds") is not None
    ]
    spectator_counts = [
        int(match["outcome"]["spectator_engagement"])
        for match in matches
        if match.get("outcome") and match["outcome"].get("spectator_engagement") is not None
    ]

    recent_matches = [
        {
            "run_id": match["run_id"],
            "verdict": match["verdict"],
            "created_at": match.get("created_at"),
            "top_candidate_rank_score": match.get("top_candidate_rank_score"),
            "result_label": match.get("outcome", {}).get("result_label") if match.get("outcome") else None,
        }
        for match in matches[:5]
    ]
    matching_runs = len(matches)
    engagement_rate = round((engaged / completed) if completed else 0.0, 4)
    avg_reply_delay_seconds = round(sum(reply_delays) / len(reply_delays), 2) if reply_delays else None
    avg_spectator_engagement = round(sum(spectator_counts) / len(spectator_counts), 2) if spectator_counts else None
    coverage_score = round(min(matching_runs / 5, 1.0), 4)
    spectator_score = round(min((avg_spectator_engagement or 0.0) / 5, 1.0), 4)
    speed_score = round(
        max(0.0, 1.0 - (min(avg_reply_delay_seconds, 3600.0) / 3600.0)) if avg_reply_delay_seconds is not None else 0.0,
        4,
    )
    historical_score = round(
        (engagement_rate * 0.65) + (coverage_score * 0.2) + (spectator_score * 0.1) + (speed_score * 0.05),
        4,
    )

    return {
        "history_runs_considered": len(history_runs),
        "matching_runs": matching_runs,
        "engaged": engaged,
        "no_bite": no_bite,
        "pending": pending,
        "engagement_rate": engagement_rate,
        "avg_reply_delay_seconds": avg_reply_delay_seconds,
        "avg_spectator_engagement": avg_spectator_engagement,
        "coverage_score": coverage_score,
        "confidence": _classify_history_confidence(matching_runs),
        "historical_score": historical_score,
        "recent_matches": recent_matches,
        "basis": basis,
    }


def _build_history_rationale(overlay: dict[str, Any]) -> str:
    confidence = overlay.get("confidence", "none")
    matching_runs = int(overlay.get("matching_runs") or 0)
    engagement_rate = float(overlay.get("engagement_rate") or 0.0)
    avg_reply_delay_seconds = overlay.get("avg_reply_delay_seconds")
    avg_spectator_engagement = overlay.get("avg_spectator_engagement")

    fragments = [f"{confidence} confidence from {matching_runs} matching run(s)"]
    fragments.append(f"engagement rate {engagement_rate:.2%}")
    if avg_reply_delay_seconds is not None:
        fragments.append(f"avg reply delay {avg_reply_delay_seconds}s")
    if avg_spectator_engagement is not None:
        fragments.append(f"avg spectator engagement {avg_spectator_engagement}")
    return "; ".join(fragments)


def _build_review_overlay(
    run: dict[str, Any],
    envelope: dict[str, Any],
    panel_reviews: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    metadata = envelope.get("metadata") or {}
    basis = {
        "platform": run.get("platform"),
        "persona": run.get("persona"),
        "objective": metadata.get("candidate_objective"),
        "tactic": metadata.get("candidate_tactic") or envelope.get("tactic"),
    }
    if not panel_reviews:
        return {
            "matching_reviews": 0,
            "promote": 0,
            "favorite": 0,
            "avoid": 0,
            "operator_score": 0.0,
            "latest_disposition": None,
            "recent_reviews": [],
            "basis": basis,
        }

    matches: list[dict[str, Any]] = []
    for review in panel_reviews:
        if review.get("run_id") == run.get("id"):
            continue
        if review.get("platform") != basis["platform"]:
            continue
        if review.get("persona") != basis["persona"]:
            continue
        if basis["objective"] is not None and review.get("candidate_objective") != basis["objective"]:
            continue
        if basis["tactic"] is not None and review.get("candidate_tactic") != basis["tactic"]:
            continue
        matches.append(review)

    promote = sum(1 for review in matches if review.get("disposition") == "promote")
    favorite = sum(1 for review in matches if review.get("disposition") == "favorite")
    avoid = sum(1 for review in matches if review.get("disposition") == "avoid")
    operator_score = round((promote * 1.0) + (favorite * 0.5) - (avoid * 1.0), 4)
    recent_reviews = [
        {
            "run_id": review.get("run_id"),
            "disposition": review.get("disposition"),
            "selection_preset": review.get("selection_preset"),
            "selection_strategy": review.get("selection_strategy"),
            "created_at": review.get("created_at"),
        }
        for review in matches[:5]
    ]
    return {
        "matching_reviews": len(matches),
        "promote": promote,
        "favorite": favorite,
        "avoid": avoid,
        "operator_score": operator_score,
        "latest_disposition": matches[0].get("disposition") if matches else None,
        "recent_reviews": recent_reviews,
        "basis": basis,
    }


def _build_review_rationale(overlay: dict[str, Any]) -> str:
    matching_reviews = int(overlay.get("matching_reviews") or 0)
    operator_score = float(overlay.get("operator_score") or 0.0)
    if matching_reviews <= 0:
        return "no matching operator review history"
    return (
        f"operator score {operator_score:+.2f} from {matching_reviews} matching review(s)"
        f" (promote={int(overlay.get('promote') or 0)}, favorite={int(overlay.get('favorite') or 0)}, avoid={int(overlay.get('avoid') or 0)})"
    )


def _simplify_recent_matches(recent_matches: list[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for match in recent_matches:
        result_label = match.get("result_label") or "unlabeled"
        created_at = match.get("created_at") or "unknown-time"
        lines.append(
            f"run #{match.get('run_id')} {match.get('verdict')} ({result_label}) @ {created_at}"
        )
    return lines


def _build_review_action_templates(
    run: dict[str, Any],
    *,
    selection_preset: str | None,
    selection_strategy: str,
    tactic: str | None,
    objective: str | None,
    review_bridge: dict[str, Any] | None = None,
) -> dict[str, Any]:
    base_args = {
        "run_id": run.get("id"),
        "selection_preset": selection_preset,
        "selection_strategy": selection_strategy,
        "tactic": tactic,
        "objective": objective,
        "notes": None,
    }
    templates: dict[str, Any] = {}
    for disposition in ("promote", "favorite", "avoid"):
        args = {**base_args, "disposition": disposition}
        template: dict[str, Any] = {
            "command": "record-panel-review",
            "args": args,
        }
        if review_bridge is not None:
            bridge_kind = str(review_bridge.get("kind") or "cli")
            if bridge_kind == "http":
                template["bridge_request"] = {
                    "kind": "http",
                    "method": "POST",
                    "submit_url": review_bridge.get("submit_url"),
                    "payload": args,
                }
            else:
                program = str(review_bridge.get("program") or "python3")
                argv_prefix = [str(item) for item in (review_bridge.get("argv_prefix") or ["-m", "bait_engine.cli.main"])]
                cli_argv = list(argv_prefix)
                db_path = review_bridge.get("db_path")
                if db_path:
                    cli_argv.extend(["--db", str(db_path)])
                cli_argv.extend(["record-panel-review", str(run.get("id")), "--disposition", disposition])
                if selection_preset is not None:
                    cli_argv.extend(["--selection-preset", str(selection_preset)])
                if selection_strategy is not None:
                    cli_argv.extend(["--selection-strategy", str(selection_strategy)])
                if tactic is not None:
                    cli_argv.extend(["--tactic", str(tactic)])
                if objective is not None:
                    cli_argv.extend(["--objective", str(objective)])
                cwd = review_bridge.get("cwd")
                template["bridge_request"] = {
                    "kind": "cli",
                    "program": program,
                    "argv": cli_argv,
                    "cwd": cwd,
                    "notes_flag": "--notes",
                }
                template["shell_command"] = " ".join(shlex.quote(part) for part in [program, *cli_argv])
        templates[disposition] = template
    return templates


def _build_panel_variant(
    run: dict[str, Any],
    *,
    name: str,
    label: str,
    thread_id: str | None,
    reply_to_id: str | None,
    author_handle: str | None,
    context: dict[str, Any] | None,
    selection_preset: str | None = None,
    selection_strategy: str = "rank",
    tactic: str | None = None,
    objective: str | None = None,
    history_runs: list[dict[str, Any]] | None = None,
    panel_reviews: list[dict[str, Any]] | None = None,
    review_bridge: dict[str, Any] | None = None,
    reputation_data: dict[str, Any] | None = None,
    combine_top_candidates: bool = False,
) -> dict[str, Any]:
    envelope = build_reply_envelope(
        run,
        selection_preset=selection_preset,
        selection_strategy=selection_strategy,
        tactic=tactic,
        objective=objective,
        thread_id=thread_id,
        reply_to_id=reply_to_id,
        author_handle=author_handle,
        context=context,
        reputation_data=reputation_data,
        combine_top_candidates=combine_top_candidates,
        allow_incomplete_target=True,
    )
    outcome_overlay = _build_outcome_overlay(run, envelope, history_runs=history_runs)
    review_overlay = _build_review_overlay(run, envelope, panel_reviews=panel_reviews)
    metadata = envelope.get("metadata") or {}
    resolved_preset = metadata.get("selection_preset") or selection_preset
    resolved_strategy = metadata.get("selection_strategy") or selection_strategy
    resolved_tactic = metadata.get("candidate_tactic") or tactic
    resolved_objective = metadata.get("candidate_objective") or objective
    review_action_templates = _build_review_action_templates(
        run,
        selection_preset=resolved_preset,
        selection_strategy=resolved_strategy,
        tactic=resolved_tactic,
        objective=resolved_objective,
        review_bridge=review_bridge,
    )
    emit_request = build_emit_request(envelope)
    selected_candidate_text = str(metadata.get("selected_candidate_text") or envelope.get("body") or "")
    envelope_body = str(envelope.get("body") or "")
    emit_request_body = str(emit_request.get("body") or "")
    return {
        "name": name,
        "label": label,
        "selection": {
            "preset": resolved_preset,
            "strategy": resolved_strategy,
            "tactic": resolved_tactic,
            "objective": resolved_objective,
            "auto_best_rationale": metadata.get("auto_best_rationale"),
            "selection_filter_fallback": metadata.get("selection_filter_fallback"),
            "selected_candidate_rank_index": metadata.get("selected_candidate_rank_index"),
        },
        "outcome_overlay": outcome_overlay,
        "review_overlay": review_overlay,
        "history_rationale": _build_history_rationale(outcome_overlay),
        "review_rationale": _build_review_rationale(review_overlay),
        "recent_match_lines": _simplify_recent_matches(outcome_overlay.get("recent_matches", [])),
        "review_action_templates": review_action_templates,
        "envelope": envelope,
        "emit_request": emit_request,
        "body_alignment": {
            "selected_candidate_rank_index": metadata.get("selected_candidate_rank_index"),
            "selected_candidate_text": selected_candidate_text,
            "envelope_body": envelope_body,
            "emit_request_body": emit_request_body,
            "selection_filter_fallback": metadata.get("selection_filter_fallback"),
            "combined_top_candidates": metadata.get("combined_top_candidates"),
            "emitted_body_differs_from_selected_candidate": emit_request_body != selected_candidate_text,
            "envelope_body_differs_from_selected_candidate": envelope_body != selected_candidate_text,
        },
    }


def _variant_dedup_key(variant: dict[str, Any]) -> tuple[str, str | None, str | None, str | None]:
    envelope = variant.get("envelope") or {}
    metadata = envelope.get("metadata") or {}
    return (
        str(envelope.get("body") or ""),
        metadata.get("candidate_tactic"),
        metadata.get("candidate_objective"),
        envelope.get("platform"),
    )


def _variant_sort_key(variant: dict[str, Any]) -> tuple[float, float, float, int, float, int, int, tuple[str, str, str, str]]:
    overlay = variant.get("outcome_overlay") or {}
    review_overlay = variant.get("review_overlay") or {}
    envelope = variant.get("envelope") or {}
    metadata = envelope.get("metadata") or {}
    selection = variant.get("selection") or {}
    strategy_name = str(selection.get("strategy") or "")
    selection_key = (
        str(selection.get("preset") or ""),
        strategy_name,
        str(selection.get("tactic") or ""),
        str(selection.get("objective") or ""),
    )
    return (
        float(overlay.get("historical_score") or 0.0),
        float(review_overlay.get("operator_score") or 0.0),
        float(overlay.get("engagement_rate") or 0.0),
        int(overlay.get("matching_runs") or 0),
        float(metadata.get("candidate_rank_score") or 0.0),
        int(variant.get("name") == "recommended"),
        int(strategy_name == "auto_best"),
        selection_key,
    )


def _dedupe_variants(variants: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[str, str | None, str | None, str | None], dict[str, Any]] = {}
    for variant in variants:
        key = _variant_dedup_key(variant)
        incumbent = deduped.get(key)
        if incumbent is None or _variant_sort_key(variant) > _variant_sort_key(incumbent):
            deduped[key] = variant
    return list(deduped.values())


def _rank_variants(variants: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ranked = sorted(variants, key=_variant_sort_key, reverse=True)
    for index, variant in enumerate(ranked, start=1):
        variant["history_rank"] = index
        variant["is_primary"] = index == 1
    return ranked


def _compare_variants(winner: dict[str, Any], challenger: dict[str, Any]) -> dict[str, Any]:
    winner_overlay = winner.get("outcome_overlay") or {}
    challenger_overlay = challenger.get("outcome_overlay") or {}
    winner_metadata = (winner.get("envelope") or {}).get("metadata") or {}
    challenger_metadata = (challenger.get("envelope") or {}).get("metadata") or {}

    winner_review_overlay = winner.get("review_overlay") or {}
    challenger_review_overlay = challenger.get("review_overlay") or {}

    score_delta = round(float(winner_overlay.get("historical_score") or 0.0) - float(challenger_overlay.get("historical_score") or 0.0), 4)
    operator_score_delta = round(float(winner_review_overlay.get("operator_score") or 0.0) - float(challenger_review_overlay.get("operator_score") or 0.0), 4)
    engagement_delta = round(float(winner_overlay.get("engagement_rate") or 0.0) - float(challenger_overlay.get("engagement_rate") or 0.0), 4)
    matching_runs_delta = int(winner_overlay.get("matching_runs") or 0) - int(challenger_overlay.get("matching_runs") or 0)
    candidate_rank_score_delta = round(float(winner_metadata.get("candidate_rank_score") or 0.0) - float(challenger_metadata.get("candidate_rank_score") or 0.0), 4)

    comparison_metrics = [
        {
            "key": "historical_score",
            "label": "Historical score",
            "delta": score_delta,
            "winner_value": round(float(winner_overlay.get("historical_score") or 0.0), 4),
            "challenger_value": round(float(challenger_overlay.get("historical_score") or 0.0), 4),
            "format": "score",
        },
        {
            "key": "operator_score",
            "label": "Operator score",
            "delta": operator_score_delta,
            "winner_value": round(float(winner_review_overlay.get("operator_score") or 0.0), 4),
            "challenger_value": round(float(challenger_review_overlay.get("operator_score") or 0.0), 4),
            "format": "score",
        },
        {
            "key": "engagement_rate",
            "label": "Engagement rate",
            "delta": engagement_delta,
            "winner_value": round(float(winner_overlay.get("engagement_rate") or 0.0), 4),
            "challenger_value": round(float(challenger_overlay.get("engagement_rate") or 0.0), 4),
            "format": "percent",
        },
        {
            "key": "matching_runs",
            "label": "Matching runs",
            "delta": matching_runs_delta,
            "winner_value": int(winner_overlay.get("matching_runs") or 0),
            "challenger_value": int(challenger_overlay.get("matching_runs") or 0),
            "format": "count",
        },
        {
            "key": "candidate_rank_score",
            "label": "Candidate rank score",
            "delta": candidate_rank_score_delta,
            "winner_value": round(float(winner_metadata.get("candidate_rank_score") or 0.0), 4),
            "challenger_value": round(float(challenger_metadata.get("candidate_rank_score") or 0.0), 4),
            "format": "score",
        },
    ]

    positive_metrics = [metric for metric in comparison_metrics if float(metric["delta"]) > 0]
    positive_metrics.sort(key=lambda metric: float(metric["delta"]), reverse=True)
    dominant_advantage = positive_metrics[0]["key"] if positive_metrics else "tie_break"

    reasons: list[str] = []
    if score_delta > 0:
        reasons.append(f"historical score +{score_delta:.4f}")
    if operator_score_delta > 0:
        reasons.append(f"operator score +{operator_score_delta:.2f}")
    if engagement_delta > 0:
        reasons.append(f"engagement rate +{engagement_delta:.2%}")
    if matching_runs_delta > 0:
        reasons.append(f"coverage +{matching_runs_delta} matching run(s)")
    if candidate_rank_score_delta > 0:
        reasons.append(f"candidate rank score +{candidate_rank_score_delta:.4f}")
    if not reasons:
        reasons.append("won on deterministic tie-break ordering")

    return {
        "winner": winner.get("label"),
        "challenger": challenger.get("label"),
        "score_delta": score_delta,
        "operator_score_delta": operator_score_delta,
        "engagement_rate_delta": engagement_delta,
        "matching_runs_delta": matching_runs_delta,
        "candidate_rank_score_delta": candidate_rank_score_delta,
        "dominant_advantage": dominant_advantage,
        "metrics": comparison_metrics,
        "summary": "; ".join(reasons),
    }


def _append_variant(
    variants: list[dict[str, Any]],
    seen_variant_keys: set[tuple[str | None, str | None, str | None, str | None]],
    run: dict[str, Any],
    *,
    name: str,
    label: str,
    thread_id: str | None,
    reply_to_id: str | None,
    author_handle: str | None,
    context: dict[str, Any] | None,
    selection_preset: str | None = None,
    selection_strategy: str = "rank",
    tactic: str | None = None,
    objective: str | None = None,
    history_runs: list[dict[str, Any]] | None = None,
    panel_reviews: list[dict[str, Any]] | None = None,
    review_bridge: dict[str, Any] | None = None,
    reputation_data: dict[str, Any] | None = None,
    variant_limit: int | None = None,
) -> None:
    variant_key = (selection_preset, selection_strategy, tactic, objective)
    if variant_key in seen_variant_keys:
        return
    if variant_limit is not None and len(variants) >= variant_limit:
        return
    variants.append(
        _build_panel_variant(
            run,
            name=name,
            label=label,
            selection_preset=selection_preset,
            selection_strategy=selection_strategy,
            tactic=tactic,
            objective=objective,
            thread_id=thread_id,
            reply_to_id=reply_to_id,
            author_handle=author_handle,
            context=context,
            history_runs=history_runs,
            panel_reviews=panel_reviews,
            review_bridge=review_bridge,
            reputation_data=reputation_data,
        )
    )
    seen_variant_keys.add(variant_key)


def build_preview_panel(
    run: dict[str, Any],
    thread_id: str | None = None,
    reply_to_id: str | None = None,
    author_handle: str | None = None,
    context: dict[str, Any] | None = None,
    *,
    include_all_presets: bool = True,
    include_strategy_variants: bool = False,
    include_filter_variants: bool = True,
    variant_limit: int | None = None,
    history_runs: list[dict[str, Any]] | None = None,
    panel_reviews: list[dict[str, Any]] | None = None,
    review_bridge: dict[str, Any] | None = None,
    reputation_data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    platform = run.get("platform") or "unknown"
    descriptor = DEFAULT_ADAPTERS[platform]
    thread_context = InboundThreadContext.model_validate(context) if context is not None else None
    recommendation = recommend_selection_preset(platform, thread_context)
    envelope = build_reply_envelope(
        run,
        selection_preset=str(recommendation["name"]),
        thread_id=thread_id,
        reply_to_id=reply_to_id,
        author_handle=author_handle,
        context=context,
        reputation_data=reputation_data,
        allow_incomplete_target=True,
    )
    emit_request = build_emit_request(envelope)
    filter_options = _extract_filter_options(run)
    variants: list[dict[str, Any]] = [
        _build_panel_variant(
            run,
            name="recommended",
            label=f"Recommended: {recommendation['name']}",
            selection_preset=str(recommendation["name"]),
            thread_id=thread_id,
            reply_to_id=reply_to_id,
            author_handle=author_handle,
            context=context,
            history_runs=history_runs,
            panel_reviews=panel_reviews,
            review_bridge=review_bridge,
            reputation_data=reputation_data,
        )
    ]

    seen_variant_keys = {
        (
            variants[0]["selection"]["preset"],
            variants[0]["selection"]["strategy"],
            variants[0]["selection"]["tactic"],
            variants[0]["selection"]["objective"],
        )
    }

    if include_all_presets:
        for preset in descriptor.selection_presets:
            _append_variant(
                variants,
                seen_variant_keys,
                run,
                name=f"preset:{preset.name}",
                label=f"Preset: {preset.name}",
                selection_preset=preset.name,
                thread_id=thread_id,
                reply_to_id=reply_to_id,
                author_handle=author_handle,
                context=context,
                history_runs=history_runs,
                panel_reviews=panel_reviews,
                review_bridge=review_bridge,
                reputation_data=reputation_data,
                variant_limit=variant_limit,
            )

    if include_strategy_variants:
        for strategy in _SELECTION_STRATEGIES:
            _append_variant(
                variants,
                seen_variant_keys,
                run,
                name=f"strategy:{strategy}",
                label=f"Strategy: {strategy}",
                selection_strategy=strategy,
                thread_id=thread_id,
                reply_to_id=reply_to_id,
                author_handle=author_handle,
                context=context,
                history_runs=history_runs,
                panel_reviews=panel_reviews,
                review_bridge=review_bridge,
                reputation_data=reputation_data,
                variant_limit=variant_limit,
            )

    if include_filter_variants:
        if filter_options["tactics"] or filter_options["objectives"]:
            _append_variant(
                variants,
                seen_variant_keys,
                run,
                name="top-score-filtered",
                label="Top score (candidate filters only)",
                selection_strategy="top_score",
                tactic=filter_options["tactics"][0] if filter_options["tactics"] else None,
                objective=filter_options["objectives"][0] if filter_options["objectives"] else None,
                thread_id=thread_id,
                reply_to_id=reply_to_id,
                author_handle=author_handle,
                context=context,
                history_runs=history_runs,
                panel_reviews=panel_reviews,
                review_bridge=review_bridge,
                reputation_data=reputation_data,
                variant_limit=variant_limit,
            )

    variants = _rank_variants(_dedupe_variants(variants))
    primary_variant = variants[0]
    runner_up = variants[1] if len(variants) > 1 else None
    primary_comparison = _compare_variants(primary_variant, runner_up) if runner_up is not None else None

    return {
        "adapter": descriptor.model_dump(mode="json"),
        "recommended_preset": recommendation,
        "context_summary": summarize_thread_context(thread_context) if thread_context is not None else None,
        "primary_variant": {
            "name": primary_variant["name"],
            "label": primary_variant["label"],
            "history_rank": primary_variant["history_rank"],
            "selection": primary_variant["selection"],
            "history_rationale": primary_variant["history_rationale"],
            "review_rationale": primary_variant["review_rationale"],
            "recent_match_lines": primary_variant["recent_match_lines"],
            "review_action_templates": primary_variant["review_action_templates"],
            "comparison_to_runner_up": primary_comparison,
        },
        "outcome_overlay": primary_variant["outcome_overlay"],
        "review_overlay": primary_variant["review_overlay"],
        "body_alignment": primary_variant["body_alignment"],
        "review_bridge": review_bridge,
        "controls": {
            "selection_strategies": list(_SELECTION_STRATEGIES),
            "selection_presets": [preset.model_dump(mode="json") for preset in descriptor.selection_presets],
            "tactics": filter_options["tactics"],
            "objectives": filter_options["objectives"],
            "review_dispositions": ["promote", "favorite", "avoid"],
        },
        "variant_generation": {
            "include_all_presets": include_all_presets,
            "include_strategy_variants": include_strategy_variants,
            "include_filter_variants": include_filter_variants,
            "variant_limit": variant_limit,
            "ranking_basis": "historical_score, operator_score, engagement_rate, matching_runs, candidate_rank_score",
            "dedupe_basis": "body, candidate_tactic, candidate_objective, platform",
            "comparison_basis": "primary vs runner-up deltas on historical score, operator score, engagement rate, matching runs, candidate rank score",
        },
        "envelope": primary_variant["envelope"],
        "emit_request": primary_variant["emit_request"],
        "variants": variants,
    }


def render_preview_panel_html(panel: dict[str, Any]) -> str:
    adapter = panel["adapter"]
    preset = panel["recommended_preset"]
    primary_variant = panel.get("primary_variant", {})
    comparison_to_runner_up = primary_variant.get("comparison_to_runner_up") or {}
    envelope = panel["envelope"]
    emit_request = panel["emit_request"]
    metrics = preset.get("metrics", {})
    controls = panel.get("controls", {})
    variant_generation = panel.get("variant_generation", {})
    outcome_overlay = panel.get("outcome_overlay", {})
    review_overlay = panel.get("review_overlay", {})
    variants = panel.get("variants", [])
    notes = "".join(f"<li>{html.escape(note)}</li>" for note in adapter.get("notes", []))
    presets = "".join(
        f"<li><b>{html.escape(item['name'])}</b>: {html.escape(str(item['strategy']))}</li>"
        for item in adapter.get("selection_presets", [])
    )
    metric_rows = "".join(
        f"<li><b>{html.escape(str(key))}</b>: {html.escape(str(value))}</li>"
        for key, value in metrics.items()
    )
    generation_rows = "".join(
        f"<li><b>{html.escape(str(key))}</b>: {html.escape(str(value))}</li>"
        for key, value in variant_generation.items()
    )
    outcome_rows = "".join(
        f"<li><b>{html.escape(str(key))}</b>: {html.escape(str(value))}</li>"
        for key, value in outcome_overlay.items()
        if key != "recent_matches"
    )
    review_rows = "".join(
        f"<li><b>{html.escape(str(key))}</b>: {html.escape(str(value))}</li>"
        for key, value in review_overlay.items()
        if key not in {"recent_reviews", "basis"}
    )
    review_bridge = panel.get("review_bridge") or {}
    dashboard_url = review_bridge.get("dashboard_url") or review_bridge.get("panel_url") or "#"
    recent_match_rows = "".join(
        f"<li>{html.escape(str(line))}</li>"
        for line in primary_variant.get("recent_match_lines", [])
    )
    comparison_summary = html.escape(str(comparison_to_runner_up.get("summary", "No runner-up comparison available.")))
    body_alignment = primary_variant.get("body_alignment") or {}
    comparison_rows = "".join(
        f"<li><b>{html.escape(str(item.get('label') or item.get('key') or 'metric'))}</b>: "
        f"Δ {html.escape(str(item.get('delta')))} · winner {html.escape(str(item.get('winner_value')))} · "
        f"runner-up {html.escape(str(item.get('challenger_value')))}</li>"
        for item in comparison_to_runner_up.get("metrics", [])
    )
    review_bridge = panel.get("review_bridge") or {}
    review_actions_json = html.escape(json.dumps(primary_variant.get("review_action_templates", {}), indent=2))
    review_bridge_kind = html.escape(str(review_bridge.get("kind") or "cli"))
    tactic_options = "".join(
        f"<option value=\"{html.escape(str(item))}\">{html.escape(str(item))}</option>"
        for item in controls.get("tactics", [])
    )
    objective_options = "".join(
        f"<option value=\"{html.escape(str(item))}\">{html.escape(str(item))}</option>"
        for item in controls.get("objectives", [])
    )
    strategy_options = "".join(
        f"<option value=\"{html.escape(str(item))}\">{html.escape(str(item))}</option>"
        for item in controls.get("selection_strategies", [])
    )
    preset_options = "".join(
        f"<option value=\"{html.escape(str(item['name']))}\">{html.escape(str(item['name']))}</option>"
        for item in controls.get("selection_presets", [])
    )
    panel_json = html.escape(json.dumps(panel, indent=2))
    return f"""<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\">
  <title>Bait Engine Preview Panel</title>
  <style>
    body {{ font-family: -apple-system, sans-serif; margin: 24px; background: #111; color: #eee; }}
    pre {{ background: #1b1b1b; padding: 12px; border-radius: 8px; overflow-x: auto; white-space: pre-wrap; }}
    .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
    .card {{ background: #181818; padding: 16px; border-radius: 10px; border: 1px solid #333; }}
    .controls {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-top: 16px; }}
    label {{ display: block; font-size: 12px; color: #bbb; margin-bottom: 6px; }}
    select {{ width: 100%; background: #0f0f0f; color: #eee; border: 1px solid #444; border-radius: 8px; padding: 8px; }}
    button {{ background: #2d5bff; color: white; border: none; padding: 10px 14px; border-radius: 8px; cursor: pointer; }}
    .variant-list {{ display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 12px; }}
    .variant-button {{ background: #222; border: 1px solid #444; }}
    .badge-row {{ display: flex; gap: 8px; flex-wrap: wrap; margin: 8px 0; }}
    .badge {{ display: inline-block; padding: 4px 8px; border-radius: 999px; font-size: 12px; border: 1px solid #444; background: #151515; }}
    .badge-win {{ border-color: #2e8b57; color: #9ef0bc; }}
    .badge-loss {{ border-color: #8b2e3d; color: #ffb3bf; }}
    .badge-pending {{ border-color: #8b7a2e; color: #f4df8d; }}
    .muted {{ color: #aaa; font-size: 12px; }}
  </style>
</head>
<body>
  <div style=\"display:flex; justify-content:space-between; align-items:center; gap:12px; margin-bottom:16px;\">
    <h1 style=\"margin:0;\">Bait Engine Preview Panel</h1>
    <a id=\"dashboardLink\" href=\"{html.escape(str(dashboard_url))}\" style=\"color:#9cd1ff;\">Back to dashboard</a>
  </div>
  <div class=\"grid\">
    <div class=\"card\">
      <h2>Adapter</h2>
      <p><b>{html.escape(adapter['name'])}</b> ({html.escape(adapter['platform'])})</p>
      <p>Default preset: <b>{html.escape(adapter['default_selection_preset'])}</b></p>
      <ul>{notes}</ul>
      <h3>Available presets</h3>
      <ul>{presets}</ul>
    </div>
    <div class=\"card\">
      <h2>Recommendation</h2>
      <p><b>{html.escape(str(preset['name']))}</b> via <b>{html.escape(str(preset['strategy']))}</b></p>
      <p>{html.escape(str(preset['reason']))}</p>
      <p>Messages: {html.escape(str(preset['message_count']))} | Hostility hits: {html.escape(str(preset['hostility_hits']))}</p>
      <h3>Metrics</h3>
      <ul>{metric_rows}</ul>
      <h3>Variant generation</h3>
      <ul>{generation_rows}</ul>
      <p>Primary variant: <b>{html.escape(str(primary_variant.get('label', 'recommended')))}</b> (rank {html.escape(str(primary_variant.get('history_rank', 1)))})</p>
      <p id="selectedCandidateStatus" class="muted">Selected candidate rank: {html.escape(str(body_alignment.get('selected_candidate_rank_index') or primary_variant.get('history_rank', 1)))}</p>
      <p id="selectionFallbackStatus" class="muted">Selection fallback: {html.escape('yes' if body_alignment.get('selection_filter_fallback') else 'no')}</p>
      <p id="bodyAlignmentStatus" class="muted">Emit body differs from selected candidate: {html.escape('yes' if body_alignment.get('emitted_body_differs_from_selected_candidate') else 'no')}</p>
      <p id="combinedTopCandidatesStatus" class="muted">Combined top candidates: {html.escape('yes' if body_alignment.get('combined_top_candidates') else 'no')}</p>
      <p id="historyRationale" class="muted">{html.escape(str(primary_variant.get('history_rationale', '')))}</p>
      <p id="reviewRationale" class="muted">{html.escape(str(primary_variant.get('review_rationale', '')))}</p>
      <h3>Comparison deltas</h3>
      <p id="comparisonSummary" class="muted">{comparison_summary}</p>
      <p id="comparisonDominant" class="muted">Dominant advantage: {html.escape(str(comparison_to_runner_up.get('dominant_advantage') or 'none'))}</p>
      <ul id="comparisonDeltaList">{comparison_rows or '<li>No runner-up comparison available.</li>'}</ul>
      <div class="badge-row">
        <span class="badge badge-win" id="engagedBadge">engaged: {html.escape(str(outcome_overlay.get('engaged', 0)))}</span>
        <span class="badge badge-loss" id="noBiteBadge">no_bite: {html.escape(str(outcome_overlay.get('no_bite', 0)))}</span>
        <span class="badge badge-pending" id="pendingBadge">pending: {html.escape(str(outcome_overlay.get('pending', 0)))}</span>
      </div>
      <h3>Outcome overlay</h3>
      <ul id="outcomeOverlayList">{outcome_rows}</ul>
      <h3>Operator review overlay</h3>
      <ul id="reviewOverlayList">{review_rows}</ul>
      <h3>Recent matches</h3>
      <ul id="recentMatchesList">{recent_match_rows or '<li>none</li>'}</ul>
    </div>
  </div>

  <div class=\"card\" style=\"margin-top: 16px;\">
    <h2>Local Controls</h2>
    <p class=\"muted\">This is a local inspection panel. Controls swap between precomputed variants so you can inspect selection behavior without posting anything.</p>
    <div class=\"variant-list\" id=\"variantButtons\"></div>
    <div class=\"controls\">
      <div>
        <label for=\"presetSelect\">Preset</label>
        <select id=\"presetSelect\"><option value=\"\">(none)</option>{preset_options}</select>
      </div>
      <div>
        <label for=\"strategySelect\">Strategy</label>
        <select id=\"strategySelect\"><option value=\"\">(none)</option>{strategy_options}</select>
      </div>
      <div>
        <label for=\"tacticSelect\">Tactic</label>
        <select id=\"tacticSelect\"><option value=\"\">(none)</option>{tactic_options}</select>
      </div>
      <div>
        <label for=\"objectiveSelect\">Objective</label>
        <select id=\"objectiveSelect\"><option value=\"\">(none)</option>{objective_options}</select>
      </div>
    </div>
    <p class=\"muted\" id=\"variantSummary\"></p>
  </div>

  <div class=\"grid\" style=\"margin-top: 16px;\">
    <div class=\"card\">
      <h2>Envelope</h2>
      <div style=\"margin: 10px 0 12px 0; display:flex; gap:8px; flex-wrap:wrap;\">
        <button id=\"copyEnvelopeButton\" type=\"button\">Copy envelope JSON</button>
        <button id=\"downloadEnvelopeButton\" type=\"button\">Download envelope JSON</button>
      </div>
      <pre id=\"envelopePre\">{html.escape(str(envelope))}</pre>
    </div>
    <div class=\"card\">
      <h2>Emit Request</h2>
      <div style=\"margin: 10px 0 12px 0; display:flex; gap:8px; flex-wrap:wrap;\">
        <button id=\"copyEmitButton\" type=\"button\">Copy emit JSON</button>
        <button id=\"downloadEmitButton\" type=\"button\">Download emit JSON</button>
        <button id=\"stageEmitButton\" type=\"button\">Stage emit locally</button>
        <span id=\"emitActionStatus\" class=\"muted\">Ready.</span>
      </div>
      <pre id=\"emitPre\">{html.escape(str(emit_request))}</pre>
    </div>
  </div>

  <div class=\"card\" style=\"margin-top: 16px;\">
    <h2>Review Submission Bridge</h2>
    <p class=\"muted\">Pick a disposition and optional note. Current bridge mode: <b>{review_bridge_kind}</b>.</p>
    <div class=\"controls\" style=\"grid-template-columns: 1fr 1fr;\">
      <div>
        <label for=\"reviewDispositionSelect\">Disposition</label>
        <select id=\"reviewDispositionSelect\"><option value=\"promote\">promote</option><option value=\"favorite\">favorite</option><option value=\"avoid\">avoid</option></select>
      </div>
      <div>
        <label for=\"reviewNotesInput\">Notes</label>
        <input id=\"reviewNotesInput\" type=\"text\" placeholder=\"optional operator note\" style=\"width: 100%; background: #0f0f0f; color: #eee; border: 1px solid #444; border-radius: 8px; padding: 8px;" />
      </div>
    </div>
    <p class=\"muted\" id=\"reviewBridgeStatus\">Bridge request updates with the selected variant.</p>
    <div style=\"margin: 10px 0 12px 0;\">
      <button id=\"reviewSubmitButton\" type=\"button\" disabled>Submit review directly</button>
    </div>
    <pre id=\"reviewActionsPre\">{review_actions_json}</pre>
  </div>

  <div class=\"card\" style=\"margin-top: 16px;\">
    <h2>Raw Panel Payload</h2>
    <pre>{panel_json}</pre>
  </div>

<script>
const panel = {json.dumps(panel)};
const PANEL_STORAGE_KEYS = {{
  lastRunId: 'bait-engine:last-run-id',
  lastDisposition: 'bait-engine:last-disposition',
  lastReviewNote: 'bait-engine:last-review-note',
}};
const variants = panel.variants || [];
const variantButtons = document.getElementById('variantButtons');
const envelopePre = document.getElementById('envelopePre');
const emitPre = document.getElementById('emitPre');
const copyEnvelopeButton = document.getElementById('copyEnvelopeButton');
const downloadEnvelopeButton = document.getElementById('downloadEnvelopeButton');
const copyEmitButton = document.getElementById('copyEmitButton');
const downloadEmitButton = document.getElementById('downloadEmitButton');
const stageEmitButton = document.getElementById('stageEmitButton');
const emitActionStatus = document.getElementById('emitActionStatus');
const reviewActionsPre = document.getElementById('reviewActionsPre');
const reviewDispositionSelect = document.getElementById('reviewDispositionSelect');
const reviewNotesInput = document.getElementById('reviewNotesInput');
const reviewBridgeStatus = document.getElementById('reviewBridgeStatus');
const reviewSubmitButton = document.getElementById('reviewSubmitButton');
const dashboardLink = document.getElementById('dashboardLink');
const outcomeOverlayList = document.getElementById('outcomeOverlayList');
const reviewOverlayList = document.getElementById('reviewOverlayList');
const recentMatchesList = document.getElementById('recentMatchesList');
const historyRationale = document.getElementById('historyRationale');
const reviewRationale = document.getElementById('reviewRationale');
const selectedCandidateStatus = document.getElementById('selectedCandidateStatus');
const selectionFallbackStatus = document.getElementById('selectionFallbackStatus');
const bodyAlignmentStatus = document.getElementById('bodyAlignmentStatus');
const combinedTopCandidatesStatus = document.getElementById('combinedTopCandidatesStatus');
const comparisonSummary = document.getElementById('comparisonSummary');
const engagedBadge = document.getElementById('engagedBadge');
const noBiteBadge = document.getElementById('noBiteBadge');
const pendingBadge = document.getElementById('pendingBadge');
const variantSummary = document.getElementById('variantSummary');
const presetSelect = document.getElementById('presetSelect');
const strategySelect = document.getElementById('strategySelect');
const tacticSelect = document.getElementById('tacticSelect');
const objectiveSelect = document.getElementById('objectiveSelect');
let currentVariant = null;
let currentReviewPayload = null;

function formatSelection(selection) {{
  return `preset=${{selection.preset || '(none)'}}, strategy=${{selection.strategy || '(none)'}}, tactic=${{selection.tactic || '(none)'}}, objective=${{selection.objective || '(none)'}}`;
}}

function renderOutcomeOverlay(overlay) {{
  const rows = Object.entries(overlay || {{}})
    .filter(([key]) => key !== 'recent_matches')
    .map(([key, value]) => `<li><b>${{key}}</b>: ${{typeof value === 'object' ? JSON.stringify(value) : String(value)}}</li>`)
    .join('');
  outcomeOverlayList.innerHTML = rows;
}}

function renderReviewOverlay(overlay) {{
  const rows = Object.entries(overlay || {{}})
    .filter(([key]) => !['recent_reviews', 'basis'].includes(key))
    .map(([key, value]) => `<li><b>${{key}}</b>: ${{typeof value === 'object' ? JSON.stringify(value) : String(value)}}</li>`)
    .join('');
  reviewOverlayList.innerHTML = rows;
}}

function setEmitStatus(message) {{
  if (emitActionStatus) emitActionStatus.textContent = message;
}}

async function copyJsonToClipboard(value, label) {{
  const jsonText = JSON.stringify(value, null, 2);
  try {{
    await navigator.clipboard.writeText(jsonText);
    setEmitStatus(`${{label}} copied.`);
  }} catch (error) {{
    setEmitStatus(`Copy failed: ${{error.message}}`);
  }}
}}

function downloadJson(value, filename, label) {{
  const blob = new Blob([JSON.stringify(value, null, 2)], {{ type: 'application/json' }});
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
  setEmitStatus(`${{label}} downloaded.`);
}}

async function stageCurrentEmit() {{
  const selected = currentVariant || panel.primary_variant || null;
  const emitRequest = selected?.emit_request || panel.emit_request;
  const envelope = selected?.envelope || panel.envelope;
  if (!emitRequest || !envelope) {{
    setEmitStatus('Nothing to stage.');
    return;
  }}
  stageEmitButton.disabled = true;
  setEmitStatus('Staging emit...');
  try {{
    const response = await fetch('/api/stage-emit', {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify({{
        run_id: envelope.run_id,
        platform: panel.adapter?.platform || envelope.target?.platform,
        selection_preset: selected?.selection?.preset || envelope.metadata?.selection_preset || null,
        selection_strategy: selected?.selection?.strategy || envelope.metadata?.selection_strategy || null,
        tactic: selected?.selection?.tactic || envelope.metadata?.candidate_tactic || null,
        objective: selected?.selection?.objective || envelope.metadata?.candidate_objective || null,
        status: 'staged',
        envelope,
        emit_request: emitRequest,
        notes: reviewNotesInput.value || null,
      }}),
    }});
    const payload = await response.json();
    if (!response.ok || !payload.ok) {{
      throw new Error(payload.error || `HTTP ${{response.status}}`);
    }}
    setEmitStatus(`Staged emit #${{payload.staged_emit?.id || '?'}}.`);
  }} catch (error) {{
    setEmitStatus(`Stage failed: ${{error.message}}`);
  }} finally {{
    stageEmitButton.disabled = false;
  }}
}}

function renderRecentMatches(lines) {{
  const rows = (lines || []).map((line) => `<li>${{line}}</li>`).join('');
  recentMatchesList.innerHTML = rows || '<li>none</li>';
}}

function renderReviewBridge(variant) {{
  const disposition = reviewDispositionSelect.value || 'promote';
  const notes = reviewNotesInput.value || null;
  const template = (variant.review_action_templates || {{}})[disposition] || null;
  if (!template) {{
    currentReviewPayload = null;
    reviewActionsPre.textContent = JSON.stringify({{}}, null, 2);
    reviewBridgeStatus.textContent = 'No bridge template for that disposition.';
    reviewSubmitButton.disabled = true;
    return;
  }}
  const payload = JSON.parse(JSON.stringify(template));
  if (payload.args) {{
    payload.args.notes = notes;
  }}
  if (payload.bridge_request) {{
    if (payload.bridge_request.kind === 'http') {{
      payload.bridge_request.payload = {{ ...(payload.bridge_request.payload || {{}}), notes }};
    }} else {{
      payload.bridge_request.notes = notes;
      if (Array.isArray(payload.bridge_request.argv) && notes) {{
        payload.bridge_request.argv = [...payload.bridge_request.argv, '--notes', notes];
      }}
    }}
  }}
  if (payload.shell_command && notes) {{
    payload.shell_command = `${{payload.shell_command}} --notes ${{JSON.stringify(notes)}}`;
  }}
  currentReviewPayload = payload;
  reviewActionsPre.textContent = JSON.stringify(payload, null, 2);
  if (payload.bridge_request?.kind === 'http') {{
    reviewSubmitButton.disabled = false;
    reviewBridgeStatus.textContent = `Ready to POST to ${{payload.bridge_request.submit_url}}`;
  }} else {{
    reviewSubmitButton.disabled = true;
    reviewBridgeStatus.textContent = payload.shell_command
      ? `Ready: ${{payload.shell_command}}`
      : `Ready disposition: ${{disposition}}`;
  }}
}}

function renderVariant(variant) {{
  currentVariant = variant;
  if (panel.envelope?.run_id) {{
    localStorage.setItem(PANEL_STORAGE_KEYS.lastRunId, String(panel.envelope.run_id));
  }}
  const bodyAlignment = variant.body_alignment || {{}};
  envelopePre.textContent = JSON.stringify(variant.envelope, null, 2);
  emitPre.textContent = JSON.stringify(variant.emit_request, null, 2);
  renderReviewBridge(variant);
  renderOutcomeOverlay(variant.outcome_overlay);
  renderReviewOverlay(variant.review_overlay);
  renderRecentMatches(variant.recent_match_lines);
  historyRationale.textContent = variant.history_rationale || '';
  reviewRationale.textContent = variant.review_rationale || '';
  selectedCandidateStatus.textContent = `Selected candidate rank: #${{bodyAlignment.selected_candidate_rank_index || variant.history_rank || '?'}}`;
  selectionFallbackStatus.textContent = `Selection fallback: ${{bodyAlignment.selection_filter_fallback ? 'yes' : 'no'}}`;
  bodyAlignmentStatus.textContent = bodyAlignment.emitted_body_differs_from_selected_candidate
    ? 'Emit body differs from selected candidate.'
    : 'Emit body matches the selected candidate.';
  combinedTopCandidatesStatus.textContent = `Combined top candidates: ${{bodyAlignment.combined_top_candidates ? 'yes' : 'no'}}`;
  setEmitStatus(`Selected ${{variant.label}}.`);
  const comparison = variant.is_primary ? (panel.primary_variant?.comparison_to_runner_up || null) : null;
  comparisonSummary.textContent = comparison ? comparison.summary : 'Comparison deltas are shown for the current winner only.';
  engagedBadge.textContent = `engaged: ${{variant.outcome_overlay?.engaged ?? 0}}`;
  noBiteBadge.textContent = `no_bite: ${{variant.outcome_overlay?.no_bite ?? 0}}`;
  pendingBadge.textContent = `pending: ${{variant.outcome_overlay?.pending ?? 0}}`;
  variantSummary.textContent = `#${{variant.history_rank || '?'}} ${{variant.label}} — ${{formatSelection(variant.selection)}}`;
  presetSelect.value = variant.selection.preset || '';
  strategySelect.value = variant.selection.strategy || '';
  tacticSelect.value = variant.selection.tactic || '';
  objectiveSelect.value = variant.selection.objective || '';
}}

function syncFromControls() {{
  const wanted = {{
    preset: presetSelect.value || null,
    strategy: strategySelect.value || null,
    tactic: tacticSelect.value || null,
    objective: objectiveSelect.value || null,
  }};
  const matched = variants.find((variant) =>
    (variant.selection.preset || null) === wanted.preset &&
    (variant.selection.strategy || null) === wanted.strategy &&
    (variant.selection.tactic || null) === wanted.tactic &&
    (variant.selection.objective || null) === wanted.objective
  );
  if (matched) {{
    renderVariant(matched);
  }} else {{
    variantSummary.textContent = `No precomputed variant for ${{formatSelection(wanted)}}`;
  }}
}}

async function submitCurrentReview() {{
  const bridgeRequest = currentReviewPayload?.bridge_request;
  if (!bridgeRequest || bridgeRequest.kind !== 'http') {{
    reviewBridgeStatus.textContent = 'No direct submission bridge is available for this panel.';
    return;
  }}
  reviewSubmitButton.disabled = true;
  reviewBridgeStatus.textContent = 'Submitting review...';
  try {{
    const response = await fetch(bridgeRequest.submit_url, {{
      method: bridgeRequest.method || 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify(bridgeRequest.payload || {{}}),
    }});
    const payload = await response.json();
    if (!response.ok || !payload.ok) {{
      throw new Error(payload.error || `HTTP ${{response.status}}`);
    }}
    reviewBridgeStatus.textContent = `Submitted: ${{payload.latest_review?.disposition || 'review'}} saved.`;
    window.location.reload();
  }} catch (error) {{
    reviewSubmitButton.disabled = false;
    reviewBridgeStatus.textContent = `Submission failed: ${{error.message}}`;
  }}
}}

variants.forEach((variant, index) => {{
  const button = document.createElement('button');
  button.className = 'variant-button';
  button.textContent = `#${{variant.history_rank || '?'}} ${{variant.label}}`;
  button.addEventListener('click', () => renderVariant(variant));
  variantButtons.appendChild(button);
  if (index === 0) renderVariant(variant);
}});

[presetSelect, strategySelect, tacticSelect, objectiveSelect].forEach((element) => {{
  element.addEventListener('change', syncFromControls);
}});

const savedDisposition = localStorage.getItem(PANEL_STORAGE_KEYS.lastDisposition);
if (savedDisposition) {{
  reviewDispositionSelect.value = savedDisposition;
}}
const savedReviewNote = localStorage.getItem(PANEL_STORAGE_KEYS.lastReviewNote);
if (savedReviewNote) {{
  reviewNotesInput.value = savedReviewNote;
}}
const lastRunId = localStorage.getItem(PANEL_STORAGE_KEYS.lastRunId);
if (lastRunId && dashboardLink && dashboardLink.href.includes('/dashboard')) {{
  dashboardLink.href = dashboardLink.href.replace(/run_id=\\d+/, `run_id=${{lastRunId}}`);
}}

[reviewDispositionSelect, reviewNotesInput].forEach((element) => {{
  element.addEventListener('input', () => {{
    localStorage.setItem(PANEL_STORAGE_KEYS.lastDisposition, reviewDispositionSelect.value || 'promote');
    localStorage.setItem(PANEL_STORAGE_KEYS.lastReviewNote, reviewNotesInput.value || '');
    if (currentVariant) renderReviewBridge(currentVariant);
  }});
  element.addEventListener('change', () => {{
    localStorage.setItem(PANEL_STORAGE_KEYS.lastDisposition, reviewDispositionSelect.value || 'promote');
    localStorage.setItem(PANEL_STORAGE_KEYS.lastReviewNote, reviewNotesInput.value || '');
    if (currentVariant) renderReviewBridge(currentVariant);
  }});
}});

copyEnvelopeButton.addEventListener('click', () => copyJsonToClipboard(currentVariant?.envelope || panel.envelope, 'Envelope'));
downloadEnvelopeButton.addEventListener('click', () => downloadJson(currentVariant?.envelope || panel.envelope, `bait-engine-envelope-run-${{panel.envelope?.run_id || 'unknown'}}.json`, 'Envelope'));
copyEmitButton.addEventListener('click', () => copyJsonToClipboard(currentVariant?.emit_request || panel.emit_request, 'Emit request'));
downloadEmitButton.addEventListener('click', () => downloadJson(currentVariant?.emit_request || panel.emit_request, `bait-engine-emit-run-${{panel.envelope?.run_id || 'unknown'}}.json`, 'Emit request'));
stageEmitButton.addEventListener('click', stageCurrentEmit);
reviewSubmitButton.addEventListener('click', submitCurrentReview);
</script>
</body>
</html>
"""
