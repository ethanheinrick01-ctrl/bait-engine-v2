from __future__ import annotations

import csv
import io
from typing import Any


VERDICT_EMOJI = {
    "engaged": "🟢",
    "no_bite": "🔴",
    "pending": "🟡",
}


def _safe_float(value: Any) -> float:
    return float(value) if value is not None else 0.0


def _safe_int(value: Any) -> int:
    return int(value) if value is not None else 0


def _latest_dispatch_observability(run: dict[str, Any]) -> dict[str, Any]:
    dispatches = run.get("emit_dispatches") or []
    if not isinstance(dispatches, list):
        return {}
    if not dispatches:
        return {}
    latest = max(
        (item for item in dispatches if isinstance(item, dict)),
        key=lambda item: int(item.get("id") or 0),
        default={},
    )
    response = latest.get("response") if isinstance(latest.get("response"), dict) else {}
    snapshot = response.get("observability_snapshot") if isinstance(response.get("observability_snapshot"), dict) else {}
    alerts = response.get("observability_alerts") if isinstance(response.get("observability_alerts"), list) else []
    return {
        "heartbeat_timestamp": snapshot.get("heartbeat_timestamp"),
        "no_output_stall": bool(snapshot.get("no_output_stall")) if snapshot else False,
        "failure_spike": bool(snapshot.get("failure_spike")) if snapshot else False,
        "failure_rate": _safe_float(snapshot.get("failure_rate")) if snapshot.get("failure_rate") is not None else None,
        "alert_count": len(alerts),
    }


def summarize_run(run: dict[str, Any]) -> dict[str, Any]:
    plan = run["plan"]
    candidates = run.get("candidates", [])
    top_candidate = candidates[0] if candidates else None
    outcome = run.get("outcome")

    observability = _latest_dispatch_observability(run)

    summary = {
        "run_id": run["id"],
        "created_at": run["created_at"],
        "platform": run.get("platform"),
        "persona": run["persona"],
        "objective": plan.get("selected_objective"),
        "tactic": plan.get("selected_tactic"),
        "exit_state": plan.get("exit_state"),
        "top_candidate": top_candidate["text"] if top_candidate else None,
        "top_candidate_rank_score": top_candidate["rank_score"] if top_candidate else None,
        "branch_forecast": plan.get("branch_forecast", []),
        "outcome": outcome,
        "obs_heartbeat_timestamp": observability.get("heartbeat_timestamp"),
        "obs_no_output_stall": observability.get("no_output_stall"),
        "obs_failure_spike": observability.get("failure_spike"),
        "obs_failure_rate": observability.get("failure_rate"),
        "obs_alert_count": observability.get("alert_count"),
    }

    if outcome:
        if outcome.get("got_reply"):
            summary["verdict"] = "engaged"
        else:
            summary["verdict"] = "no_bite"
    else:
        summary["verdict"] = "pending"

    return summary


def summarize_runs(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [summarize_run(run) for run in runs]


def filter_summaries(
    summaries: list[dict[str, Any]],
    persona: str | None = None,
    platform: str | None = None,
    verdict: str | None = None,
) -> list[dict[str, Any]]:
    filtered = summaries
    if persona is not None:
        filtered = [summary for summary in filtered if summary.get("persona") == persona]
    if platform is not None:
        filtered = [summary for summary in filtered if summary.get("platform") == platform]
    if verdict is not None:
        filtered = [summary for summary in filtered if summary.get("verdict") == verdict]
    return filtered


def _build_bucket(stats_key: str, summaries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for summary in summaries:
        value = summary.get(stats_key) or "unknown"
        bucket = grouped.setdefault(value, {"runs": 0, "engaged": 0, "no_bite": 0, "pending": 0})
        bucket["runs"] += 1
        bucket[summary["verdict"]] += 1

    return sorted(
        (
            {
                stats_key: value,
                **stats,
                "engagement_rate": round((stats["engaged"] / stats["runs"]) if stats["runs"] else 0.0, 4),
            }
            for value, stats in grouped.items()
        ),
        key=lambda item: (item["engagement_rate"], item["engaged"], -item["pending"], item[stats_key]),
        reverse=True,
    )



def _router_metrics(summaries: list[dict[str, Any]], runs: list[dict[str, Any]]) -> dict[str, Any]:
    by_run_id = {int(run.get("id") or 0): run for run in runs}
    confidence_values: list[float] = []
    auto_engaged = 0
    auto_no_bite = 0
    auto_runs = 0
    forced_runs = 0
    unknown_mode_runs = 0
    auto_persona_sequence: list[str] = []

    for summary in summaries:
        run = by_run_id.get(int(summary.get("run_id") or 0)) or {}
        plan = run.get("plan") if isinstance(run.get("plan"), dict) else {}
        persona_selection = plan.get("persona_selection") if isinstance(plan.get("persona_selection"), dict) else {}
        mode = str(persona_selection.get("mode") or "unknown")
        if mode == "auto":
            auto_runs += 1
            auto_persona_sequence.append(str(summary.get("persona") or "unknown"))
            if summary.get("verdict") == "engaged":
                auto_engaged += 1
            elif summary.get("verdict") == "no_bite":
                auto_no_bite += 1
        elif mode == "forced":
            forced_runs += 1
        else:
            unknown_mode_runs += 1

        persona_router = plan.get("persona_router") if isinstance(plan.get("persona_router"), dict) else {}
        confidence = persona_router.get("confidence")
        if confidence is not None:
            try:
                confidence_values.append(float(confidence))
            except (TypeError, ValueError):
                pass

    auto_decided = auto_engaged + auto_no_bite
    auto_pick_accuracy = round((auto_engaged / auto_decided) if auto_decided else 0.0, 4)

    bins = {
        "lt_0_40": 0,
        "0_40_to_0_60": 0,
        "0_60_to_0_80": 0,
        "gte_0_80": 0,
    }
    for confidence in confidence_values:
        if confidence < 0.4:
            bins["lt_0_40"] += 1
        elif confidence < 0.6:
            bins["0_40_to_0_60"] += 1
        elif confidence < 0.8:
            bins["0_60_to_0_80"] += 1
        else:
            bins["gte_0_80"] += 1

    drift_changes = 0
    for index in range(1, len(auto_persona_sequence)):
        if auto_persona_sequence[index] != auto_persona_sequence[index - 1]:
            drift_changes += 1
    persona_drift = round((drift_changes / max(len(auto_persona_sequence) - 1, 1)) if len(auto_persona_sequence) > 1 else 0.0, 4)

    return {
        "auto_pick_accuracy": auto_pick_accuracy,
        "confidence_distribution": bins,
        "persona_drift": persona_drift,
        "auto_runs": auto_runs,
        "forced_runs": forced_runs,
        "unknown_mode_runs": unknown_mode_runs,
        "override_audit": {
            "forced_persona": forced_runs,
            "auto_persona": auto_runs,
            "unknown": unknown_mode_runs,
        },
    }


def build_outcome_scoreboard(
    runs: list[dict[str, Any]],
    persona: str | None = None,
    platform: str | None = None,
    verdict: str | None = None,
) -> dict[str, Any]:
    summaries = filter_summaries(summarize_runs(runs), persona=persona, platform=platform, verdict=verdict)
    total_runs = len(summaries)
    engaged = sum(1 for summary in summaries if summary["verdict"] == "engaged")
    no_bite = sum(1 for summary in summaries if summary["verdict"] == "no_bite")
    pending = sum(1 for summary in summaries if summary["verdict"] == "pending")

    heartbeat_values = [str(summary.get("obs_heartbeat_timestamp") or "") for summary in summaries if summary.get("obs_heartbeat_timestamp")]
    latest_heartbeat = max(heartbeat_values) if heartbeat_values else None

    return {
        "filters": {"persona": persona, "platform": platform, "verdict": verdict},
        "total_runs": total_runs,
        "engaged": engaged,
        "no_bite": no_bite,
        "pending": pending,
        "engagement_rate": round((engaged / total_runs) if total_runs else 0.0, 4),
        "personas": _build_bucket("persona", summaries),
        "platforms": _build_bucket("platform", summaries),
        "objectives": _build_bucket("objective", summaries),
        "tactics": _build_bucket("tactic", summaries),
        "exit_states": _build_bucket("exit_state", summaries),
        "router_metrics": _router_metrics(summaries, runs),
        "observability": {
            "latest_heartbeat_timestamp": latest_heartbeat,
            "stall_run_count": sum(1 for summary in summaries if bool(summary.get("obs_no_output_stall"))),
            "failure_spike_run_count": sum(1 for summary in summaries if bool(summary.get("obs_failure_spike"))),
            "total_alert_count": sum(_safe_int(summary.get("obs_alert_count")) for summary in summaries),
            "max_failure_rate": round(max((_safe_float(summary.get("obs_failure_rate")) for summary in summaries if summary.get("obs_failure_rate") is not None), default=0.0), 4),
        },
    }



def build_report(
    runs: list[dict[str, Any]],
    limit_per_section: int = 5,
    persona: str | None = None,
    platform: str | None = None,
    verdict: str | None = None,
) -> dict[str, Any]:
    summaries = filter_summaries(summarize_runs(runs), persona=persona, platform=platform, verdict=verdict)
    scoreboard = build_outcome_scoreboard(runs, persona=persona, platform=platform, verdict=verdict)

    best_runs = sorted(
        (summary for summary in summaries if summary["verdict"] == "engaged"),
        key=lambda summary: (
            _safe_int(summary["outcome"].get("spectator_engagement") if summary.get("outcome") else None),
            _safe_int(summary["outcome"].get("reply_length") if summary.get("outcome") else None),
            -_safe_int(summary["outcome"].get("reply_delay_seconds") if summary.get("outcome") else None),
            _safe_float(summary.get("top_candidate_rank_score")),
        ),
        reverse=True,
    )[:limit_per_section]

    worst_runs = sorted(
        (summary for summary in summaries if summary["verdict"] == "no_bite"),
        key=lambda summary: (
            _safe_float(summary.get("top_candidate_rank_score")),
            summary.get("persona") or "",
        ),
        reverse=True,
    )[:limit_per_section]

    pending_runs = sorted(
        (summary for summary in summaries if summary["verdict"] == "pending"),
        key=lambda summary: (
            _safe_float(summary.get("top_candidate_rank_score")),
            summary.get("created_at") or "",
        ),
        reverse=True,
    )[:limit_per_section]

    return {
        "scoreboard": scoreboard,
        "best_runs": best_runs,
        "worst_runs": worst_runs,
        "pending_runs": pending_runs,
    }



def _format_filter_value(value: str | None) -> str:
    return value if value is not None else "any"



def _format_bucket_section(title: str, key: str, rows: list[dict[str, Any]]) -> list[str]:
    lines = [f"## {title}"]
    if not rows:
        lines.append("- none")
        return lines
    for row in rows:
        lines.append(
            f"- `{row[key]}` — runs={row['runs']}, engaged={row['engaged']}, no_bite={row['no_bite']}, pending={row['pending']}, rate={row['engagement_rate']:.4f}"
        )
    return lines



def _format_run_line(summary: dict[str, Any]) -> str:
    verdict = summary.get("verdict") or "pending"
    emoji = VERDICT_EMOJI.get(verdict, "⚪")
    return (
        f"- {emoji} run #{summary['run_id']} | persona=`{summary.get('persona')}` | platform=`{summary.get('platform')}` | "
        f"objective=`{summary.get('objective')}` | tactic=`{summary.get('tactic')}` | score={_safe_float(summary.get('top_candidate_rank_score')):.4f} | "
        f"candidate: {summary.get('top_candidate') or 'none'}"
    )



def render_report_markdown(report: dict[str, Any]) -> str:
    scoreboard = report["scoreboard"]
    filters = scoreboard.get("filters", {})
    router_metrics = scoreboard.get("router_metrics") or {}
    observability = scoreboard.get("observability") or {}
    lines = [
        "# Bait Engine Report",
        "",
        f"Filters: persona=`{_format_filter_value(filters.get('persona'))}`, platform=`{_format_filter_value(filters.get('platform'))}`, verdict=`{_format_filter_value(filters.get('verdict'))}`",
        "",
        "## Summary",
        f"- total_runs={scoreboard['total_runs']}",
        f"- engaged={scoreboard['engaged']}",
        f"- no_bite={scoreboard['no_bite']}",
        f"- pending={scoreboard['pending']}",
        f"- engagement_rate={scoreboard['engagement_rate']:.4f}",
        f"- auto_pick_accuracy={float(router_metrics.get('auto_pick_accuracy') or 0.0):.4f}",
        f"- persona_drift={float(router_metrics.get('persona_drift') or 0.0):.4f}",
        f"- override_audit={router_metrics.get('override_audit') or {}}",
        f"- observability_latest_heartbeat={observability.get('latest_heartbeat_timestamp')}",
        f"- observability_stall_run_count={_safe_int(observability.get('stall_run_count'))}",
        f"- observability_failure_spike_run_count={_safe_int(observability.get('failure_spike_run_count'))}",
        f"- observability_total_alert_count={_safe_int(observability.get('total_alert_count'))}",
        f"- observability_max_failure_rate={_safe_float(observability.get('max_failure_rate')):.4f}",
        "",
    ]
    lines.extend(_format_bucket_section("Personas", "persona", scoreboard.get("personas", [])))
    lines.append("")
    lines.extend(_format_bucket_section("Platforms", "platform", scoreboard.get("platforms", [])))
    lines.append("")
    lines.extend(_format_bucket_section("Objectives", "objective", scoreboard.get("objectives", [])))
    lines.append("")
    lines.extend(_format_bucket_section("Tactics", "tactic", scoreboard.get("tactics", [])))
    lines.append("")
    lines.extend(_format_bucket_section("Exit States", "exit_state", scoreboard.get("exit_states", [])))
    lines.append("")
    lines.append("## Best Runs")
    lines.extend([_format_run_line(summary) for summary in report.get("best_runs", [])] or ["- none"])
    lines.append("")
    lines.append("## Worst Runs")
    lines.extend([_format_run_line(summary) for summary in report.get("worst_runs", [])] or ["- none"])
    lines.append("")
    lines.append("## Pending Runs")
    lines.extend([_format_run_line(summary) for summary in report.get("pending_runs", [])] or ["- none"])
    return "\n".join(lines)



def render_report_csv(report: dict[str, Any]) -> str:
    scoreboard = report["scoreboard"]
    filters = scoreboard.get("filters", {})
    fieldnames = [
        "record_type",
        "section",
        "filter_persona",
        "filter_platform",
        "filter_verdict",
        "label",
        "runs",
        "engaged",
        "no_bite",
        "pending",
        "engagement_rate",
        "obs_latest_heartbeat_timestamp",
        "obs_stall_run_count",
        "obs_failure_spike_run_count",
        "obs_total_alert_count",
        "obs_max_failure_rate",
        "obs_heartbeat_timestamp",
        "obs_no_output_stall",
        "obs_failure_spike",
        "obs_failure_rate",
        "obs_alert_count",
        "run_id",
        "created_at",
        "platform",
        "persona",
        "objective",
        "tactic",
        "exit_state",
        "verdict",
        "top_candidate_rank_score",
        "top_candidate",
    ]
    observability = scoreboard.get("observability") or {}
    rows: list[dict[str, Any]] = [
        {
            "record_type": "summary",
            "section": "summary",
            "filter_persona": filters.get("persona"),
            "filter_platform": filters.get("platform"),
            "filter_verdict": filters.get("verdict"),
            "label": "all",
            "runs": scoreboard.get("total_runs"),
            "engaged": scoreboard.get("engaged"),
            "no_bite": scoreboard.get("no_bite"),
            "pending": scoreboard.get("pending"),
            "engagement_rate": scoreboard.get("engagement_rate"),
            "obs_latest_heartbeat_timestamp": observability.get("latest_heartbeat_timestamp"),
            "obs_stall_run_count": observability.get("stall_run_count"),
            "obs_failure_spike_run_count": observability.get("failure_spike_run_count"),
            "obs_total_alert_count": observability.get("total_alert_count"),
            "obs_max_failure_rate": observability.get("max_failure_rate"),
        }
    ]

    router_metrics = scoreboard.get("router_metrics") or {}
    rows.append(
        {
            "record_type": "router_metric",
            "section": "router_metrics",
            "filter_persona": filters.get("persona"),
            "filter_platform": filters.get("platform"),
            "filter_verdict": filters.get("verdict"),
            "label": "auto_pick_accuracy",
            "engagement_rate": router_metrics.get("auto_pick_accuracy"),
            "runs": router_metrics.get("auto_runs"),
        }
    )
    rows.append(
        {
            "record_type": "router_metric",
            "section": "router_metrics",
            "filter_persona": filters.get("persona"),
            "filter_platform": filters.get("platform"),
            "filter_verdict": filters.get("verdict"),
            "label": "persona_drift",
            "engagement_rate": router_metrics.get("persona_drift"),
            "runs": router_metrics.get("auto_runs"),
        }
    )

    for section, key in (
        ("personas", "persona"),
        ("platforms", "platform"),
        ("objectives", "objective"),
        ("tactics", "tactic"),
        ("exit_states", "exit_state"),
    ):
        for item in scoreboard.get(section, []):
            rows.append(
                {
                    "record_type": "bucket",
                    "section": section,
                    "filter_persona": filters.get("persona"),
                    "filter_platform": filters.get("platform"),
                    "filter_verdict": filters.get("verdict"),
                    "label": item.get(key),
                    "runs": item.get("runs"),
                    "engaged": item.get("engaged"),
                    "no_bite": item.get("no_bite"),
                    "pending": item.get("pending"),
                    "engagement_rate": item.get("engagement_rate"),
                }
            )

    for section in ("best_runs", "worst_runs", "pending_runs"):
        for summary in report.get(section, []):
            rows.append(
                {
                    "record_type": "run",
                    "section": section,
                    "filter_persona": filters.get("persona"),
                    "filter_platform": filters.get("platform"),
                    "filter_verdict": filters.get("verdict"),
                    "run_id": summary.get("run_id"),
                    "created_at": summary.get("created_at"),
                    "platform": summary.get("platform"),
                    "persona": summary.get("persona"),
                    "objective": summary.get("objective"),
                    "tactic": summary.get("tactic"),
                    "exit_state": summary.get("exit_state"),
                    "verdict": summary.get("verdict"),
                    "top_candidate_rank_score": summary.get("top_candidate_rank_score"),
                    "top_candidate": summary.get("top_candidate"),
                    "obs_heartbeat_timestamp": summary.get("obs_heartbeat_timestamp"),
                    "obs_no_output_stall": summary.get("obs_no_output_stall"),
                    "obs_failure_spike": summary.get("obs_failure_spike"),
                    "obs_failure_rate": summary.get("obs_failure_rate"),
                    "obs_alert_count": summary.get("obs_alert_count"),
                }
            )

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()
