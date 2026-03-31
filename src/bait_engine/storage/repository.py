from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import urllib.error
import urllib.parse
import urllib.request
import hashlib

from bait_engine.adapters.emitters import build_emit_request
from bait_engine.adapters.compiler import build_reply_envelope
from bait_engine.adapters.select import select_dispatch_driver
from bait_engine.intake.contracts import compose_source_text
from bait_engine.analysis import AnalyzeInput, analyze_comment
from bait_engine.analysis.archetypes import BASE_ARCHETYPE_AXIS_WEIGHTS
from bait_engine.core.types import Archetype, DecisionPlan, RhetoricalAxis, TacticalObjective, TacticFamily
from bait_engine.generation import DraftRequest, MutationSeed, draft_candidates, draft_candidates_with_provider, generate_controlled_variants
from bait_engine.planning import DEFAULT_PERSONAS, build_plan, get_persona, select_persona
from bait_engine.providers import OpenAICompatibleProvider, TextGenerationProvider
from bait_engine.storage.db import open_db
from bait_engine.storage.models import EmitDispatchRecord, EmitOutboxRecord, IntakeTargetRecord, MutationFamilyRecord, MutationVariantRecord, OutcomeRecord, PanelReviewRecord, RunRecord, candidates_from_draft, to_json


logger = logging.getLogger(__name__)


class RunRepository:
    _REDACTED = "***REDACTED***"
    _SENSITIVE_KEY_FRAGMENTS = (
        "oauth_access_token",
        "access_token",
        "authorization",
        "api_key",
        "bearer",
        "secret",
    )

    def __init__(self, db_path: str | Path | None = None):
        self.path = db_path

    @staticmethod
    def _repo_root() -> Path:
        return Path(__file__).resolve().parents[3]

    @classmethod
    def default_dispatch_dir(cls) -> Path:
        return (cls._repo_root() / ".data" / "dispatches").resolve()

    @classmethod
    def _is_sensitive_key(cls, key: str) -> bool:
        lowered = str(key).lower()
        return any(fragment in lowered for fragment in cls._SENSITIVE_KEY_FRAGMENTS)

    @classmethod
    def _redact_sensitive(cls, value: Any) -> Any:
        if isinstance(value, dict):
            redacted: dict[str, Any] = {}
            for key, nested in value.items():
                if cls._is_sensitive_key(str(key)):
                    redacted[str(key)] = cls._REDACTED
                else:
                    redacted[str(key)] = cls._redact_sensitive(nested)
            return redacted
        if isinstance(value, list):
            return [cls._redact_sensitive(item) for item in value]
        return value

    def _fetch_mutation_seed_rows(
        self,
        *,
        persona: str,
        platform: str,
        tactic: str | None = None,
        objective: str | None = None,
        limit: int = 10,
        days: int = 30,
    ) -> list[dict[str, Any]]:
        clauses = ["mf.persona = ?", "mf.platform = ?"]
        params: list[Any] = [persona, platform]
        if days > 0:
            clauses.append("mv.created_at > datetime('now', ?)")
            params.append(f"-{int(days)} days")
        if tactic is not None:
            clauses.append("mf.tactic = ?")
            params.append(tactic)
        if objective is not None:
            clauses.append("mf.objective = ?")
            params.append(objective)
        sql = f"""
            SELECT
                mv.id AS variant_id,
                mv.family_id,
                mv.run_id,
                mv.transform,
                mv.variant_text,
                mv.status,
                mv.score_json,
                mv.lineage_json,
                mv.created_at,
                mf.persona AS family_persona,
                mf.platform AS family_platform,
                mf.tactic AS family_tactic,
                mf.objective AS family_objective,
                mf.winner_score
            FROM mutation_variants mv
            JOIN mutation_families mf ON mf.id = mv.family_id
            WHERE {' AND '.join(clauses)}
            ORDER BY COALESCE(mf.winner_score, 0) DESC, mv.created_at DESC, mv.id DESC
            LIMIT ?
        """
        params.append(limit)
        with open_db(self.path) as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def _parse_created_at(value: Any) -> float | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
            try:
                parsed = datetime.strptime(text, fmt)
                return parsed.replace(tzinfo=timezone.utc).timestamp()
            except ValueError:
                continue
        return None

    @staticmethod
    def _utc_sql_now(now_ts: float | None = None) -> str:
        ts = time.time() if now_ts is None else float(now_ts)
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _utc_sql_from_timestamp(ts: float) -> str:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _parse_circuit_breaker_policy(emit_request: Any) -> dict[str, Any]:
        if not isinstance(emit_request, dict):
            return {"enabled": False}
        policy_raw = emit_request.get("circuit_breaker") or {}
        if not isinstance(policy_raw, dict):
            return {"enabled": False}

        failure_threshold = max(0, int(policy_raw.get("failure_threshold") or 0))
        if failure_threshold <= 0:
            return {"enabled": False}

        return {
            "enabled": True,
            "failure_threshold": failure_threshold,
            "failure_window_seconds": max(1, int(policy_raw.get("failure_window_seconds") or 60)),
            "cooldown_seconds": max(1, int(policy_raw.get("cooldown_seconds") or 120)),
        }

    @staticmethod
    def _parse_dispatch_safety_policy(emit_request: Any) -> dict[str, Any]:
        if not isinstance(emit_request, dict):
            emit_request = {}

        safety_raw = emit_request.get("safety") or {}
        if not isinstance(safety_raw, dict):
            safety_raw = {}

        global_pause = bool(safety_raw.get("global_pause", False))
        paused_platforms_raw = safety_raw.get("paused_platforms") or []
        paused_platforms = sorted(
            {
                str(item).strip().lower()
                for item in paused_platforms_raw
                if str(item or "").strip()
            }
        )

        safe_mode = bool(safety_raw.get("safe_mode", False))
        allow_drivers_raw = safety_raw.get("safe_mode_allowed_drivers") or ["manual_copy", "jsonl_append"]
        safe_mode_allowed_drivers = sorted(
            {
                str(item).strip().lower()
                for item in allow_drivers_raw
                if str(item or "").strip()
            }
        )
        if not safe_mode_allowed_drivers:
            safe_mode_allowed_drivers = ["manual_copy", "jsonl_append"]

        override_source = str(safety_raw.get("override_source") or "none").strip().lower()
        if override_source not in {"manual", "system", "none"}:
            override_source = "none"

        return {
            "global_pause": global_pause,
            "paused_platforms": paused_platforms,
            "safe_mode": safe_mode,
            "safe_mode_allowed_drivers": safe_mode_allowed_drivers,
            "override_source": override_source,
        }

    @staticmethod
    def _evaluate_dispatch_safety(
        *,
        safety_policy: dict[str, Any],
        platform: str | None,
        driver: str,
    ) -> dict[str, Any]:
        normalized_platform = str(platform or "").strip().lower()
        paused_platforms = [str(item).strip().lower() for item in (safety_policy.get("paused_platforms") or []) if str(item).strip()]
        safe_mode_allowed = [str(item).strip().lower() for item in (safety_policy.get("safe_mode_allowed_drivers") or []) if str(item).strip()]

        if bool(safety_policy.get("global_pause")):
            return {
                "allowed": False,
                "block_reason": "global_pause",
                "precedence": 1,
            }

        if normalized_platform and normalized_platform in paused_platforms:
            return {
                "allowed": False,
                "block_reason": "platform_pause",
                "precedence": 2,
            }

        if bool(safety_policy.get("safe_mode")) and str(driver or "").strip().lower() not in set(safe_mode_allowed):
            return {
                "allowed": False,
                "block_reason": "safe_mode_restricted_driver",
                "precedence": 3,
            }

        return {
            "allowed": True,
            "block_reason": None,
            "precedence": 4,
        }

    @staticmethod
    def _safety_mode_state_payload(
        *,
        safety_policy: dict[str, Any],
        platform: str | None,
    ) -> dict[str, Any]:
        normalized_platform = str(platform or "").strip().lower()
        paused_platforms = [str(item).strip().lower() for item in (safety_policy.get("paused_platforms") or []) if str(item).strip()]
        return {
            "global_pause": bool(safety_policy.get("global_pause")),
            "platform": normalized_platform,
            "platform_paused": normalized_platform in set(paused_platforms) if normalized_platform else False,
            "paused_platforms": paused_platforms,
            "safe_mode": bool(safety_policy.get("safe_mode")),
            "safe_mode_allowed_drivers": [
                str(item).strip().lower()
                for item in (safety_policy.get("safe_mode_allowed_drivers") or [])
                if str(item).strip()
            ],
            "precedence": ["global_pause", "platform_pause", "safe_mode_allowlist", "allow"],
        }

    @staticmethod
    def _parse_observability_policy(emit_request: Any) -> dict[str, Any]:
        if not isinstance(emit_request, dict):
            emit_request = {}
        raw = emit_request.get("observability") or {}
        if not isinstance(raw, dict):
            raw = {}
        return {
            "stall_threshold_seconds": max(1.0, float(raw.get("stall_threshold_seconds") or 900.0)),
            "failure_window_seconds": max(10, int(raw.get("failure_window_seconds") or 900)),
            "failure_spike_threshold": min(max(float(raw.get("failure_spike_threshold") or 0.6), 0.0), 1.0),
            "failure_spike_min_events": max(1, int(raw.get("failure_spike_min_events") or 5)),
        }

    def _dispatch_observability_snapshot(
        self,
        conn: Any,
        *,
        now_ts: float,
        current_status: str,
        policy: dict[str, Any],
    ) -> dict[str, Any]:
        success_statuses = {"dispatched", "acknowledged", "delivered"}
        failure_statuses = {"failed", "dead_letter", "blocked"}

        last_success_row = conn.execute(
            """
            SELECT created_at
            FROM emit_dispatches
            WHERE status IN ('dispatched', 'acknowledged', 'delivered')
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()

        heartbeat_timestamp = last_success_row["created_at"] if last_success_row else None
        if current_status in success_statuses:
            heartbeat_timestamp = self._utc_sql_from_timestamp(now_ts)

        heartbeat_ts = self._parse_created_at(heartbeat_timestamp)
        seconds_since_last_success = (now_ts - heartbeat_ts) if heartbeat_ts is not None else None

        window_seconds = int(policy.get("failure_window_seconds") or 900)
        window_start = self._utc_sql_from_timestamp(now_ts - window_seconds)
        rows = conn.execute(
            "SELECT status FROM emit_dispatches WHERE created_at >= ?",
            (window_start,),
        ).fetchall()
        statuses = [str(row["status"] or "") for row in rows]
        statuses.append(str(current_status or ""))

        total_events = len(statuses)
        failure_events = sum(1 for status in statuses if status in failure_statuses)
        failure_rate = (failure_events / total_events) if total_events else 0.0

        stall_threshold = float(policy.get("stall_threshold_seconds") or 900.0)
        no_output_stall = seconds_since_last_success is None or seconds_since_last_success >= stall_threshold

        spike_threshold = float(policy.get("failure_spike_threshold") or 0.6)
        min_events = int(policy.get("failure_spike_min_events") or 5)
        failure_spike = total_events >= min_events and failure_rate >= spike_threshold

        return {
            "heartbeat_timestamp": heartbeat_timestamp,
            "seconds_since_last_success": round(seconds_since_last_success, 4) if seconds_since_last_success is not None else None,
            "no_output_stall": bool(no_output_stall),
            "stall_threshold_seconds": round(stall_threshold, 4),
            "failure_window_seconds": window_seconds,
            "failure_events": failure_events,
            "total_events": total_events,
            "failure_rate": round(failure_rate, 4),
            "failure_spike": bool(failure_spike),
            "failure_spike_threshold": round(spike_threshold, 4),
            "failure_spike_min_events": min_events,
        }

    @staticmethod
    def _dispatch_observability_alerts(*, snapshot: dict[str, Any], now_ts: float) -> list[dict[str, Any]]:
        triggered_at = datetime.fromtimestamp(now_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        alerts: list[dict[str, Any]] = []

        if bool(snapshot.get("no_output_stall")):
            alerts.append(
                {
                    "alert_type": "no_output_stall",
                    "severity": "critical",
                    "triggered_at": triggered_at,
                    "metric_snapshot": {
                        "seconds_since_last_success": snapshot.get("seconds_since_last_success"),
                        "stall_threshold_seconds": snapshot.get("stall_threshold_seconds"),
                        "heartbeat_timestamp": snapshot.get("heartbeat_timestamp"),
                    },
                    "recommended_action": "Inspect paused/blocked dispatches and run a manual low-risk dispatch probe.",
                }
            )

        if bool(snapshot.get("failure_spike")):
            severity = "critical" if float(snapshot.get("failure_rate") or 0.0) >= 0.85 else "warning"
            alerts.append(
                {
                    "alert_type": "failure_spike",
                    "severity": severity,
                    "triggered_at": triggered_at,
                    "metric_snapshot": {
                        "failure_rate": snapshot.get("failure_rate"),
                        "failure_events": snapshot.get("failure_events"),
                        "total_events": snapshot.get("total_events"),
                        "failure_spike_threshold": snapshot.get("failure_spike_threshold"),
                        "failure_window_seconds": snapshot.get("failure_window_seconds"),
                    },
                    "recommended_action": "Pause affected drivers, inspect recent dead-letter/failed responses, then redrive with safe-mode.",
                }
            )

        return alerts

    @staticmethod
    def _merge_control_overrides(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
        merged = dict(base)
        for key, value in (override or {}).items():
            if value is None:
                continue
            merged[key] = value
        return merged

    @staticmethod
    def _default_governor_settings() -> dict[str, Any]:
        return {
            "max_actions_per_hour": 0,
            "max_actions_per_day": 0,
            "min_seconds_between_actions": 0.0,
            "quiet_hours_start": None,
            "quiet_hours_end": None,
        }

    @staticmethod
    def _default_containment_settings() -> dict[str, Any]:
        return {
            "retry_policy": {
                "max_attempts": 3,
                "base_delay_seconds": 30.0,
                "backoff_multiplier": 2.0,
                "max_delay_seconds": 1800.0,
            },
            "circuit_breaker": {
                "enabled": False,
                "failure_threshold": 0,
                "failure_window_seconds": 60,
                "cooldown_seconds": 120,
            },
        }

    def _base_escalation_policy(self) -> dict[str, Any]:
        cheap_model = os.environ.get("BAIT_ENGINE_CHEAP_MODEL") or os.environ.get("BAIT_ENGINE_MODEL") or os.environ.get("OPENAI_MODEL") or "gpt-5.4-mini"
        hard_model = os.environ.get("BAIT_ENGINE_HARD_MODEL") or "gpt-5.4"
        return {
            "cheap_model": cheap_model,
            "hard_model": hard_model,
            "low_router_confidence_threshold": float(os.environ.get("BAIT_ENGINE_ESCALATE_ROUTER_CONFIDENCE_LT", "0.58")),
            "persona_duel_margin_threshold": float(os.environ.get("BAIT_ENGINE_ESCALATE_DUEL_MARGIN_LTE", "0.05")),
            "semantic_inversion_risk_threshold": float(os.environ.get("BAIT_ENGINE_ESCALATE_SEMANTIC_INVERSION_GTE", "0.55")),
            "high_value_opportunity_threshold": float(os.environ.get("BAIT_ENGINE_ESCALATE_OPPORTUNITY_GTE", "0.72")),
            "per_run_escalation_cap": max(0, int(os.environ.get("BAIT_ENGINE_ESCALATE_PER_RUN_CAP", "1"))),
            "daily_escalation_cap": max(0, int(os.environ.get("BAIT_ENGINE_ESCALATE_DAILY_CAP", "24"))),
        }

    @staticmethod
    def _default_safety_settings() -> dict[str, Any]:
        return {
            "global_pause": False,
            "paused_platforms": [],
            "safe_mode": False,
            "safe_mode_allowed_drivers": ["manual_copy", "jsonl_append"],
            "override_source": "none",
        }

    def _load_dispatch_control_state(self, conn: Any) -> dict[str, Any]:
        row = conn.execute("SELECT * FROM dispatch_control_state WHERE id = 1").fetchone()
        if not row:
            return {
                "governor": {},
                "containment": {},
                "escalation": {},
                "safety": {},
                "metadata": {},
                "updated_at": None,
            }
        return {
            "governor": json.loads(row["governor_json"] or "{}"),
            "containment": json.loads(row["containment_json"] or "{}"),
            "escalation": json.loads(row["escalation_json"] or "{}"),
            "safety": json.loads(row["safety_json"] or "{}"),
            "metadata": json.loads(row["metadata_json"] or "{}"),
            "updated_at": row["updated_at"],
        }

    def _save_dispatch_control_state(
        self,
        conn: Any,
        *,
        governor: dict[str, Any],
        containment: dict[str, Any],
        escalation: dict[str, Any],
        safety: dict[str, Any],
        metadata: dict[str, Any] | None = None,
    ) -> None:
        conn.execute(
            """
            INSERT INTO dispatch_control_state (id, updated_at, governor_json, containment_json, escalation_json, safety_json, metadata_json)
            VALUES (1, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                updated_at = excluded.updated_at,
                governor_json = excluded.governor_json,
                containment_json = excluded.containment_json,
                escalation_json = excluded.escalation_json,
                safety_json = excluded.safety_json,
                metadata_json = excluded.metadata_json
            """,
            (
                to_json(governor),
                to_json(containment),
                to_json(escalation),
                to_json(safety),
                to_json(metadata or {}),
            ),
        )

    def _effective_dispatch_controls(self, conn: Any) -> dict[str, Any]:
        stored = self._load_dispatch_control_state(conn)
        governor = self._merge_control_overrides(self._default_governor_settings(), stored.get("governor") or {})

        containment = self._default_containment_settings()
        stored_containment = stored.get("containment") or {}
        containment["retry_policy"] = self._merge_control_overrides(
            containment.get("retry_policy") or {},
            (stored_containment.get("retry_policy") if isinstance(stored_containment, dict) else {}) or {},
        )
        containment["circuit_breaker"] = self._merge_control_overrides(
            containment.get("circuit_breaker") or {},
            (stored_containment.get("circuit_breaker") if isinstance(stored_containment, dict) else {}) or {},
        )

        escalation = self._merge_control_overrides(self._base_escalation_policy(), stored.get("escalation") or {})
        safety = self._merge_control_overrides(self._default_safety_settings(), stored.get("safety") or {})

        return {
            "governor": governor,
            "containment": containment,
            "escalation": escalation,
            "safety": safety,
            "metadata": stored.get("metadata") or {},
            "updated_at": stored.get("updated_at"),
        }

    def _dispatch_health_snapshot(self, conn: Any, *, now_ts: float) -> dict[str, Any]:
        success_row = conn.execute(
            """
            SELECT created_at
            FROM emit_dispatches
            WHERE status IN ('dispatched', 'acknowledged', 'delivered')
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        heartbeat_timestamp = success_row["created_at"] if success_row else None
        heartbeat_ts = self._parse_created_at(heartbeat_timestamp)
        seconds_since_last_success = (now_ts - heartbeat_ts) if heartbeat_ts is not None else None

        window_seconds = 900
        window_start = self._utc_sql_from_timestamp(now_ts - window_seconds)
        rows = conn.execute(
            "SELECT status FROM emit_dispatches WHERE created_at >= ?",
            (window_start,),
        ).fetchall()
        statuses = [str(row["status"] or "") for row in rows]
        total_events = len(statuses)
        failure_events = sum(1 for status in statuses if status in {"failed", "dead_letter", "blocked"})
        failure_rate = (failure_events / total_events) if total_events else 0.0

        breaker_row = conn.execute(
            "SELECT COUNT(*) AS count FROM dispatch_circuit_breakers WHERE state = 'open'"
        ).fetchone()
        open_breaker_count = int((breaker_row["count"] if breaker_row else 0) or 0)

        no_output_stall = seconds_since_last_success is None or seconds_since_last_success >= 900.0
        failure_spike = total_events >= 5 and failure_rate >= 0.6
        success_total_row = conn.execute(
            "SELECT COUNT(*) AS count FROM emit_dispatches WHERE status IN ('dispatched', 'acknowledged', 'delivered')"
        ).fetchone()
        success_total = int((success_total_row["count"] if success_total_row else 0) or 0)

        return {
            "heartbeat_timestamp": heartbeat_timestamp,
            "seconds_since_last_success": round(seconds_since_last_success, 4) if seconds_since_last_success is not None else None,
            "no_output_stall": bool(no_output_stall),
            "failure_spike": bool(failure_spike),
            "failure_rate_15m": round(failure_rate, 4),
            "failure_events_15m": failure_events,
            "total_events_15m": total_events,
            "open_breaker_count": open_breaker_count,
            "success_events_total": success_total,
        }

    @staticmethod
    def _is_last_known_good(*, telemetry: dict[str, Any], controls: dict[str, Any]) -> bool:
        safety = controls.get("safety") or {}
        return (
            int(telemetry.get("success_events_total") or 0) > 0
            and not bool(telemetry.get("no_output_stall"))
            and not bool(telemetry.get("failure_spike"))
            and int(telemetry.get("open_breaker_count") or 0) == 0
            and not bool(safety.get("global_pause"))
        )

    def create_dispatch_control_checkpoint(self, *, reason: str | None = None) -> dict[str, Any]:
        now_ts = time.time()
        with open_db(self.path) as conn:
            controls = self._effective_dispatch_controls(conn)
            telemetry = self._dispatch_health_snapshot(conn, now_ts=now_ts)
            is_last_good = self._is_last_known_good(telemetry=telemetry, controls=controls)
            conn.execute(
                """
                INSERT INTO dispatch_control_checkpoints (
                    reason, governor_json, containment_json, escalation_json, safety_json, telemetry_json, is_last_good
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    reason,
                    to_json(controls.get("governor") or {}),
                    to_json(controls.get("containment") or {}),
                    to_json(controls.get("escalation") or {}),
                    to_json(controls.get("safety") or {}),
                    to_json(telemetry),
                    1 if is_last_good else 0,
                ),
            )
            rowid_row = conn.execute("SELECT last_insert_rowid()").fetchone()
            if not rowid_row or not rowid_row[0]:
                raise RuntimeError("Failed to retrieve inserted checkpoint rowid")
            checkpoint_id = int(rowid_row[0])
            row = conn.execute("SELECT * FROM dispatch_control_checkpoints WHERE id = ?", (checkpoint_id,)).fetchone()
            if not row:
                raise RuntimeError(f"Checkpoint {checkpoint_id} not found after insertion")
            return self._decode_dispatch_control_checkpoint(dict(row))

    @staticmethod
    def _decode_dispatch_control_checkpoint(row: dict[str, Any]) -> dict[str, Any]:
        decoded = dict(row)
        decoded["governor"] = json.loads(decoded.pop("governor_json") or "{}")
        decoded["containment"] = json.loads(decoded.pop("containment_json") or "{}")
        decoded["escalation"] = json.loads(decoded.pop("escalation_json") or "{}")
        decoded["safety"] = json.loads(decoded.pop("safety_json") or "{}")
        decoded["telemetry"] = json.loads(decoded.pop("telemetry_json") or "{}")
        decoded["is_last_good"] = bool(decoded.get("is_last_good"))
        return decoded

    def list_dispatch_control_checkpoints(self, *, limit: int = 25) -> list[dict[str, Any]]:
        with open_db(self.path) as conn:
            rows = conn.execute(
                "SELECT * FROM dispatch_control_checkpoints ORDER BY id DESC LIMIT ?",
                (max(1, int(limit)),),
            ).fetchall()
        return [self._decode_dispatch_control_checkpoint(dict(row)) for row in rows]

    def _apply_checkpoint_controls(self, conn: Any, *, checkpoint: dict[str, Any], source: str) -> None:
        self._save_dispatch_control_state(
            conn,
            governor=checkpoint.get("governor") or {},
            containment=checkpoint.get("containment") or {},
            escalation=checkpoint.get("escalation") or {},
            safety=checkpoint.get("safety") or {},
            metadata={"source": source, "checkpoint_id": checkpoint.get("id")},
        )

    def rollback_to_checkpoint(self, checkpoint_id: int) -> dict[str, Any]:
        with open_db(self.path) as conn:
            row = conn.execute(
                "SELECT * FROM dispatch_control_checkpoints WHERE id = ?",
                (int(checkpoint_id),),
            ).fetchone()
            if not row:
                return {
                    "applied": False,
                    "failed": True,
                    "reason": "checkpoint_not_found",
                    "checkpoint_id": int(checkpoint_id),
                }
            checkpoint = self._decode_dispatch_control_checkpoint(dict(row))
            self._apply_checkpoint_controls(conn, checkpoint=checkpoint, source="rollback_to_checkpoint")
            return {
                "applied": True,
                "failed": False,
                "reason": None,
                "checkpoint_id": int(checkpoint_id),
                "state": {
                    "governor": checkpoint.get("governor") or {},
                    "containment": checkpoint.get("containment") or {},
                    "escalation": checkpoint.get("escalation") or {},
                    "safety": checkpoint.get("safety") or {},
                },
            }

    def rollback_to_last_good_state(self) -> dict[str, Any]:
        with open_db(self.path) as conn:
            row = conn.execute(
                """
                SELECT *
                FROM dispatch_control_checkpoints
                WHERE is_last_good = 1
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            if not row:
                return {
                    "applied": False,
                    "failed": True,
                    "reason": "no_last_good_checkpoint",
                    "checkpoint_id": None,
                }
            checkpoint = self._decode_dispatch_control_checkpoint(dict(row))
            self._apply_checkpoint_controls(conn, checkpoint=checkpoint, source="rollback_to_last_good_state")
            return {
                "applied": True,
                "failed": False,
                "reason": None,
                "checkpoint_id": int(checkpoint.get("id") or 0),
                "state": {
                    "governor": checkpoint.get("governor") or {},
                    "containment": checkpoint.get("containment") or {},
                    "escalation": checkpoint.get("escalation") or {},
                    "safety": checkpoint.get("safety") or {},
                },
            }

    def get_dispatch_control_state(self) -> dict[str, Any]:
        with open_db(self.path) as conn:
            return self._effective_dispatch_controls(conn)

    def set_dispatch_control_state(
        self,
        *,
        governor: dict[str, Any] | None = None,
        containment: dict[str, Any] | None = None,
        escalation: dict[str, Any] | None = None,
        safety: dict[str, Any] | None = None,
        source: str = "manual",
    ) -> dict[str, Any]:
        with open_db(self.path) as conn:
            current = self._load_dispatch_control_state(conn)
            merged_governor = self._merge_control_overrides(current.get("governor") or {}, governor or {})
            current_containment = current.get("containment") if isinstance(current.get("containment"), dict) else {}
            merged_containment = dict(current_containment or {})
            merged_containment["retry_policy"] = self._merge_control_overrides(
                (current_containment or {}).get("retry_policy") or {},
                (containment or {}).get("retry_policy") if isinstance(containment, dict) else {},
            )
            merged_containment["circuit_breaker"] = self._merge_control_overrides(
                (current_containment or {}).get("circuit_breaker") or {},
                (containment or {}).get("circuit_breaker") if isinstance(containment, dict) else {},
            )
            merged_escalation = self._merge_control_overrides(current.get("escalation") or {}, escalation or {})
            merged_safety = self._merge_control_overrides(current.get("safety") or {}, safety or {})
            self._save_dispatch_control_state(
                conn,
                governor=merged_governor,
                containment=merged_containment,
                escalation=merged_escalation,
                safety=merged_safety,
                metadata={"source": source},
            )
        return self.get_dispatch_control_state()

    def _recent_observability_alert_summary(self, conn: Any, *, limit: int = 200) -> dict[str, Any]:
        rows = conn.execute(
            "SELECT response_json FROM emit_dispatches ORDER BY id DESC LIMIT ?",
            (max(1, int(limit)),),
        ).fetchall()
        severity_counts: dict[str, int] = {}
        type_counts: dict[str, int] = {}
        total = 0
        latest_triggered_at = None

        for row in rows:
            try:
                payload = json.loads(row["response_json"] or "{}")
            except Exception:
                continue
            alerts = payload.get("observability_alerts") if isinstance(payload, dict) else []
            if not isinstance(alerts, list):
                continue
            for alert in alerts:
                if not isinstance(alert, dict):
                    continue
                total += 1
                severity = str(alert.get("severity") or "unknown")
                alert_type = str(alert.get("alert_type") or "unknown")
                severity_counts[severity] = severity_counts.get(severity, 0) + 1
                type_counts[alert_type] = type_counts.get(alert_type, 0) + 1
                triggered_at = str(alert.get("triggered_at") or "")
                if triggered_at and (latest_triggered_at is None or triggered_at > latest_triggered_at):
                    latest_triggered_at = triggered_at

        return {
            "window_dispatches": max(1, int(limit)),
            "total_alerts": total,
            "severity_counts": dict(sorted(severity_counts.items())),
            "type_counts": dict(sorted(type_counts.items())),
            "latest_triggered_at": latest_triggered_at,
        }

    def operator_status_summary(self) -> dict[str, Any]:
        now_ts = time.time()
        with open_db(self.path) as conn:
            controls = self._effective_dispatch_controls(conn)
            stats = self._action_window_stats(now=now_ts)
            governor = self.can_execute_action(
                now_utc=datetime.fromtimestamp(now_ts, timezone.utc),
                actions_last_hour=int(stats.get("actions_last_hour") or 0),
                actions_last_day=int(stats.get("actions_last_day") or 0),
                seconds_since_last_action=stats.get("seconds_since_last_action"),
                max_actions_per_hour=int(controls.get("governor", {}).get("max_actions_per_hour") or 0),
                max_actions_per_day=int(controls.get("governor", {}).get("max_actions_per_day") or 0),
                min_seconds_between_actions=float(controls.get("governor", {}).get("min_seconds_between_actions") or 0.0),
                quiet_hours_start=controls.get("governor", {}).get("quiet_hours_start"),
                quiet_hours_end=controls.get("governor", {}).get("quiet_hours_end"),
            )
            retry_queue = self.get_retry_queue_summary(limit=100)
            breaker_rows = conn.execute(
                "SELECT state, COUNT(*) AS count FROM dispatch_circuit_breakers GROUP BY state"
            ).fetchall()
            breaker_state_counts = {
                str(row["state"] or "unknown"): int(row["count"] or 0)
                for row in breaker_rows
            }
            escalation_policy = self._escalation_policy(conn)
            escalation_budget = {
                "policy": escalation_policy,
                "daily_used": self._daily_hard_escalation_count(conn, now_ts=now_ts),
                "daily_remaining": max(
                    0,
                    int(escalation_policy.get("daily_escalation_cap") or 0)
                    - self._daily_hard_escalation_count(conn, now_ts=now_ts),
                ),
            }
            safety = self._parse_dispatch_safety_policy({"safety": controls.get("safety") or {}})
            observability = self._recent_observability_alert_summary(conn)
            active_checkpoint_meta = controls.get("metadata") if isinstance(controls.get("metadata"), dict) else {}
            latest_checkpoint_row = conn.execute(
                "SELECT * FROM dispatch_control_checkpoints ORDER BY id DESC LIMIT 1"
            ).fetchone()
            last_good_row = conn.execute(
                "SELECT * FROM dispatch_control_checkpoints WHERE is_last_good = 1 ORDER BY id DESC LIMIT 1"
            ).fetchone()

            return {
                "generated_at": self._utc_sql_from_timestamp(now_ts),
                "governor": {
                    "config": controls.get("governor") or {},
                    "allow": bool(governor.get("allow")),
                    "reason": governor.get("reason"),
                    "stats": stats,
                },
                "containment": {
                    "config": controls.get("containment") or {},
                    "breaker_state_counts": breaker_state_counts,
                    "retry_queue": retry_queue,
                },
                "escalation": escalation_budget,
                "safety": {
                    **safety,
                    "active": bool(safety.get("global_pause") or safety.get("safe_mode") or (safety.get("paused_platforms") or [])),
                },
                "observability": observability,
                "checkpoints": {
                    "active_checkpoint_id": active_checkpoint_meta.get("checkpoint_id"),
                    "active_source": active_checkpoint_meta.get("source"),
                    "latest": self._decode_dispatch_control_checkpoint(dict(latest_checkpoint_row)) if latest_checkpoint_row else None,
                    "last_good": self._decode_dispatch_control_checkpoint(dict(last_good_row)) if last_good_row else None,
                },
            }

    def preflight_autopilot_checklist(
        self,
        *,
        dead_letter_fail_threshold: int = 10,
        waiting_retry_fail_threshold: int = 50,
        critical_alert_fail_threshold: int = 1,
    ) -> dict[str, Any]:
        summary = self.operator_status_summary()
        containment = summary.get("containment") if isinstance(summary.get("containment"), dict) else {}
        retry_queue = containment.get("retry_queue") if isinstance(containment.get("retry_queue"), dict) else {}
        safety = summary.get("safety") if isinstance(summary.get("safety"), dict) else {}
        governor = summary.get("governor") if isinstance(summary.get("governor"), dict) else {}
        observability = summary.get("observability") if isinstance(summary.get("observability"), dict) else {}
        checkpoints = summary.get("checkpoints") if isinstance(summary.get("checkpoints"), dict) else {}
        last_good = checkpoints.get("last_good") if isinstance(checkpoints.get("last_good"), dict) else None

        open_breakers = int((containment.get("breaker_state_counts") or {}).get("open") or 0)
        dead_letter_count = int(retry_queue.get("dead_letter_count") or 0)
        waiting_retry_count = int(retry_queue.get("waiting_retry_count") or 0)
        critical_alerts = int((observability.get("severity_counts") or {}).get("critical") or 0)

        checklist = [
            {
                "item": "governor_allows_execution",
                "pass": bool(governor.get("allow")),
                "reason": "governor allow=true" if bool(governor.get("allow")) else str(governor.get("reason") or "governor_blocked"),
            },
            {
                "item": "safety_not_globally_paused",
                "pass": not bool(safety.get("global_pause")),
                "reason": "global_pause=false" if not bool(safety.get("global_pause")) else "global_pause=true",
            },
            {
                "item": "containment_breakers_closed",
                "pass": open_breakers == 0,
                "reason": "open_breakers=0" if open_breakers == 0 else f"open_breakers={open_breakers}",
            },
            {
                "item": "dead_letter_within_threshold",
                "pass": dead_letter_count <= max(0, int(dead_letter_fail_threshold)),
                "reason": f"dead_letter_count={dead_letter_count}, threshold={max(0, int(dead_letter_fail_threshold))}",
            },
            {
                "item": "retry_backlog_within_threshold",
                "pass": waiting_retry_count <= max(0, int(waiting_retry_fail_threshold)),
                "reason": f"waiting_retry_count={waiting_retry_count}, threshold={max(0, int(waiting_retry_fail_threshold))}",
            },
            {
                "item": "critical_alerts_within_threshold",
                "pass": critical_alerts < max(0, int(critical_alert_fail_threshold)),
                "reason": f"critical_alerts={critical_alerts}, threshold={max(0, int(critical_alert_fail_threshold))}",
            },
            {
                "item": "last_good_checkpoint_available",
                "pass": last_good is not None,
                "reason": "last_good checkpoint found" if last_good is not None else "no last_good checkpoint",
            },
        ]

        return {
            "generated_at": summary.get("generated_at"),
            "overall_pass": all(bool(item.get("pass")) for item in checklist),
            "items": checklist,
            "status": summary,
        }

    def _load_dispatch_circuit_breaker(self, conn: Any, *, scope_key: str) -> dict[str, Any]:
        row = conn.execute(
            "SELECT * FROM dispatch_circuit_breakers WHERE scope_key = ?",
            (scope_key,),
        ).fetchone()
        if not row:
            return {
                "scope_key": scope_key,
                "state": "closed",
                "failure_timestamps": [],
                "opened_at": None,
                "open_until": None,
            }
        parsed = dict(row)
        failures = json.loads(parsed.get("failure_timestamps_json") or "[]")
        failure_timestamps = [float(item) for item in failures if isinstance(item, (int, float, str)) and str(item).strip()]
        return {
            "scope_key": scope_key,
            "state": str(parsed.get("state") or "closed"),
            "failure_timestamps": failure_timestamps,
            "opened_at": parsed.get("opened_at"),
            "open_until": parsed.get("open_until"),
        }

    def _save_dispatch_circuit_breaker(self, conn: Any, *, state: dict[str, Any], now_ts: float) -> None:
        conn.execute(
            """
            INSERT INTO dispatch_circuit_breakers (scope_key, state, failure_timestamps_json, opened_at, open_until, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(scope_key) DO UPDATE SET
                state = excluded.state,
                failure_timestamps_json = excluded.failure_timestamps_json,
                opened_at = excluded.opened_at,
                open_until = excluded.open_until,
                updated_at = excluded.updated_at
            """,
            (
                state.get("scope_key"),
                state.get("state") or "closed",
                to_json(state.get("failure_timestamps") or []),
                state.get("opened_at"),
                state.get("open_until"),
                self._utc_sql_now(now_ts),
            ),
        )

    def _dispatch_breaker_preflight(
        self,
        conn: Any,
        *,
        scope_key: str,
        policy: dict[str, Any],
        now_ts: float,
    ) -> dict[str, Any]:
        state = self._load_dispatch_circuit_breaker(conn, scope_key=scope_key)
        window_seconds = int(policy.get("failure_window_seconds") or 60)
        cutoff = now_ts - window_seconds
        failures = [float(ts) for ts in state.get("failure_timestamps") or [] if float(ts) >= cutoff]
        state["failure_timestamps"] = failures

        breaker_state = str(state.get("state") or "closed")
        open_until_ts = self._parse_created_at(state.get("open_until"))

        blocked = False
        reason_code = None
        if breaker_state == "open":
            if open_until_ts is not None and now_ts < open_until_ts:
                blocked = True
                reason_code = "circuit_breaker_open"
            else:
                state["state"] = "half_open"
                state["open_until"] = None
                breaker_state = "half_open"

        self._save_dispatch_circuit_breaker(conn, state=state, now_ts=now_ts)
        return {
            "state": state,
            "blocked": blocked,
            "reason_code": reason_code,
            "phase": breaker_state,
        }

    def _dispatch_breaker_post_dispatch(
        self,
        conn: Any,
        *,
        state: dict[str, Any],
        policy: dict[str, Any],
        now_ts: float,
        dispatch_status: str,
        pre_phase: str,
    ) -> dict[str, Any]:
        window_seconds = int(policy.get("failure_window_seconds") or 60)
        threshold = int(policy.get("failure_threshold") or 1)
        cooldown_seconds = int(policy.get("cooldown_seconds") or 120)

        cutoff = now_ts - window_seconds
        failures = [float(ts) for ts in state.get("failure_timestamps") or [] if float(ts) >= cutoff]

        is_failure = dispatch_status in {"failed", "dead_letter"}
        breaker_opened = False
        reason_code = None

        if is_failure:
            failures.append(now_ts)
            failures = [float(ts) for ts in failures if float(ts) >= cutoff]
            if pre_phase == "half_open":
                state["state"] = "open"
                breaker_opened = True
                reason_code = "circuit_breaker_reopened"
            elif len(failures) >= threshold:
                state["state"] = "open"
                breaker_opened = True
                reason_code = "circuit_breaker_opened"
            else:
                state["state"] = "closed"
            if state.get("state") == "open":
                state["opened_at"] = self._utc_sql_from_timestamp(now_ts)
                state["open_until"] = self._utc_sql_from_timestamp(now_ts + cooldown_seconds)
        else:
            if pre_phase == "half_open":
                failures = []
                reason_code = "circuit_breaker_closed"
            state["state"] = "closed"
            state["opened_at"] = None
            state["open_until"] = None

        state["failure_timestamps"] = failures
        self._save_dispatch_circuit_breaker(conn, state=state, now_ts=now_ts)
        return {
            "breaker_state": state.get("state") or "closed",
            "breaker_opened": breaker_opened,
            "reason_code": reason_code,
            "failure_count_window": len(failures),
        }

    @classmethod
    def _mutation_seed_weight(
        cls,
        row: dict[str, Any],
        *,
        days: int,
        selected_transforms: set[str],
    ) -> float:
        lineage = json.loads(row.get("lineage_json") or "{}")
        score_data = json.loads(row.get("score_json") or "{}")

        winner_score = float(row.get("winner_score") or 0.0)
        seed_winner_score = float(score_data.get("seed_winner_score") or winner_score or 0.0)
        rank_score = float(score_data.get("seed_rank_score") or 0.0)

        metrics = lineage.get("mutation_metrics") if isinstance(lineage, dict) else {}
        delta_ratio = float(metrics.get("delta_ratio") or 0.0)
        novelty_ratio = float(metrics.get("novelty_ratio") or 0.0)

        delta_target = 0.33
        novelty_target = 0.45
        delta_quality = max(0.0, 1.0 - min(abs(delta_ratio - delta_target), 1.0))
        novelty_quality = max(0.0, 1.0 - min(abs(novelty_ratio - novelty_target), 1.0))

        status = str(row.get("status") or "drafted").lower()
        status_bonus = {
            "replied": 0.28,
            "delivered": 0.22,
            "promoted": 0.18,
            "approved": 0.14,
            "selected": 0.12,
            "drafted": 0.06,
            "rejected": -0.15,
            "failed": -0.25,
            "dead": -0.30,
        }.get(status, 0.0)

        created_ts = cls._parse_created_at(row.get("created_at"))
        recency_bonus = 0.0
        if created_ts is not None and days > 0:
            age_days = max(0.0, (time.time() - created_ts) / 86400.0)
            recency_bonus = max(0.0, 1.0 - min(age_days / max(float(days), 1.0), 1.0)) * 0.1

        transform = str(row.get("transform") or "").strip().lower()
        diversity_penalty = 0.12 if transform and transform in selected_transforms else 0.0

        return (
            (winner_score * 0.34)
            + (seed_winner_score * 0.18)
            + (rank_score * 0.10)
            + (delta_quality * 0.14)
            + (novelty_quality * 0.14)
            + status_bonus
            + recency_bonus
            - diversity_penalty
        )

    @staticmethod
    def _row_to_mutation_seed(row: dict[str, Any]) -> MutationSeed:
        lineage = json.loads(row.get("lineage_json") or "{}")
        return MutationSeed(
            text=str(row.get("variant_text") or "").strip(),
            variant_id=int(row.get("variant_id") or 0) or None,
            family_id=int(row.get("family_id") or 0) or None,
            run_id=int(row.get("run_id") or 0) or None,
            transform=str(row.get("transform") or "") or None,
            persona=str(row.get("family_persona") or "") or None,
            platform=str(row.get("family_platform") or "") or None,
            tactic=str(row.get("family_tactic") or "") or None,
            objective=str(row.get("family_objective") or "") or None,
            winner_score=float(row.get("winner_score") or 0.0) if row.get("winner_score") is not None else None,
            delta_ratio=float(lineage.get("mutation_metrics", {}).get("delta_ratio")) if lineage.get("mutation_metrics", {}).get("delta_ratio") is not None else None,
            novelty_ratio=float(lineage.get("mutation_metrics", {}).get("novelty_ratio")) if lineage.get("mutation_metrics", {}).get("novelty_ratio") is not None else None,
        )

    def _select_mutation_seeds(
        self,
        *,
        persona: str | None,
        platform: str | None,
        tactic: str | None = None,
        objective: str | None = None,
        limit: int = 3,
        days: int = 30,
    ) -> list[MutationSeed]:
        if not persona or not platform or limit <= 0:
            return []

        search_tiers: list[dict[str, str | None]] = []
        if tactic is not None and objective is not None:
            search_tiers.append({"tactic": tactic, "objective": objective})
        if objective is not None:
            search_tiers.append({"tactic": None, "objective": objective})
        if tactic is not None:
            search_tiers.append({"tactic": tactic, "objective": None})
        search_tiers.append({"tactic": None, "objective": None})

        ranked_rows: list[dict[str, Any]] = []
        seen_text: set[str] = set()
        for tier in search_tiers:
            rows = self._fetch_mutation_seed_rows(
                persona=persona,
                platform=platform,
                tactic=tier["tactic"],
                objective=tier["objective"],
                limit=max(limit * 6, 24),
                days=days,
            )
            for row in rows:
                text = str(row.get("variant_text") or "").strip()
                if not text:
                    continue
                key = text.lower()
                if key in seen_text:
                    continue
                seen_text.add(key)
                ranked_rows.append(row)

        seeds: list[MutationSeed] = []
        selected_transforms: set[str] = set()
        remaining = list(ranked_rows)
        while remaining and len(seeds) < limit:
            best_idx = -1
            best_score = float("-inf")
            for idx, row in enumerate(remaining):
                score = self._mutation_seed_weight(row, days=days, selected_transforms=selected_transforms)
                if score > best_score:
                    best_score = score
                    best_idx = idx
            if best_idx < 0:
                break
            chosen = remaining.pop(best_idx)
            seed = self._row_to_mutation_seed(chosen)
            if not seed.text:
                continue
            seeds.append(seed)
            transform = str(chosen.get("transform") or "").strip().lower()
            if transform:
                selected_transforms.add(transform)

        selected_variant_ids = [int(seed.variant_id) for seed in seeds if seed.variant_id is not None]
        if selected_variant_ids:
            placeholders = ", ".join("?" for _ in selected_variant_ids)
            with open_db(self.path) as conn:
                conn.execute(
                    f"UPDATE mutation_variants SET status = 'selected' WHERE id IN ({placeholders}) AND status = 'drafted'",
                    tuple(selected_variant_ids),
                )
        return seeds

    @staticmethod
    def _bounded_score(value: float) -> float:
        return max(0.0, min(1.0, float(value)))

    def _persona_router_priors(
        self,
        *,
        platform: str,
        days: int = 90,
        min_samples: int = 3,
    ) -> dict[str, dict[str, float | int]]:
        priors: dict[str, dict[str, float | int]] = {}
        for persona_name in sorted(DEFAULT_PERSONAS.keys()):
            reputation = self.get_persona_reputation(persona_name, platform=platform, days=days)
            total_runs = int(reputation.get("total_runs") or 0)
            if total_runs < min_samples:
                continue
            priors[persona_name] = {
                "score": float(reputation.get("reply_rate") or 0.0),
                "confidence": self._bounded_score(total_runs / max(float(min_samples * 3), 1.0)),
                "sample_count": total_runs,
            }
        return priors

    def persona_router_calibration(
        self,
        *,
        platform: str,
        objective: str | None,
        days: int = 90,
        min_samples: int = 8,
    ) -> dict[str, Any]:
        segment_objective = str(objective or "any")
        segment = f"{platform}:{segment_objective}"
        clauses = ["r.platform = ?"]
        params: list[Any] = [platform]
        if objective:
            clauses.append("r.selected_objective = ?")
            params.append(objective)
        if days > 0:
            clauses.append("r.created_at > datetime('now', ?)")
            params.append(f"-{int(max(days, 1))} days")

        sql = f"""
            SELECT
                r.persona,
                o.got_reply,
                o.spectator_engagement,
                o.reply_delay_seconds,
                o.reply_length,
                r.created_at
            FROM runs r
            JOIN outcomes o ON o.run_id = r.id
            WHERE {' AND '.join(clauses)}
            ORDER BY r.id ASC
        """
        with open_db(self.path) as conn:
            rows = [dict(row) for row in conn.execute(sql, tuple(params)).fetchall()]

        sample_size = len(rows)
        if sample_size < min_samples:
            return {
                "version": "phase12-artery2-v1",
                "timestamp": rows[-1].get("created_at") if rows else None,
                "segment": segment,
                "segment_confidence": 0.0,
                "enabled": False,
                "sample_size": sample_size,
                "min_samples": min_samples,
                "fallback_reason": "sparse_segment",
                "weights": {},
            }

        overall_quality_sum = 0.0
        persona_buckets: dict[str, dict[str, float]] = {}
        for row in rows:
            quality = self._outcome_quality_score(row)
            overall_quality_sum += quality
            key = str(row.get("persona") or "")
            bucket = persona_buckets.setdefault(key, {"quality_sum": 0.0, "count": 0.0})
            bucket["quality_sum"] += quality
            bucket["count"] += 1.0

        overall_mean = overall_quality_sum / max(sample_size, 1)
        segment_confidence = self._confidence_from_samples(sample_size, min_samples)

        weights: dict[str, dict[str, float | int]] = {}
        for persona_name in sorted(DEFAULT_PERSONAS.keys()):
            bucket = persona_buckets.get(persona_name)
            if not bucket:
                continue
            count = int(bucket["count"])
            persona_mean = bucket["quality_sum"] / max(bucket["count"], 1.0)
            lift = persona_mean - overall_mean
            score = self._bounded_score(0.5 + (lift * 0.85))
            confidence = self._bounded_score((count / max(float(min_samples), 1.0)) * segment_confidence)
            weights[persona_name] = {
                "score": round(score, 4),
                "confidence": round(confidence, 4),
                "sample_count": count,
                "mean_quality": round(persona_mean, 4),
                "lift": round(lift, 4),
            }

        latest_timestamp = str(rows[-1].get("created_at") or "") if rows else None
        return {
            "version": "phase12-artery2-v1",
            "timestamp": latest_timestamp or None,
            "segment": segment,
            "segment_confidence": round(segment_confidence, 4),
            "enabled": segment_confidence > 0 and bool(weights),
            "sample_size": sample_size,
            "min_samples": min_samples,
            "fallback_reason": None if segment_confidence > 0 else "confidence_floor",
            "weights": weights,
        }

    def get_lane_prior(
        self,
        *,
        persona: str,
        platform: str,
        tactic: str | None = None,
        objective: str | None = None,
        days: int = 30,
        min_samples: int = 6,
    ) -> dict[str, Any]:
        if not persona or not platform:
            return {
                "prior_score": 0.5,
                "confidence": 0.0,
                "sample_count": 0,
                "scope": "none",
                "components": {},
            }

        search_tiers: list[tuple[str | None, str | None, str]] = []
        if tactic is not None and objective is not None:
            search_tiers.append((tactic, objective, "tactic+objective"))
        if objective is not None:
            search_tiers.append((None, objective, "objective"))
        if tactic is not None:
            search_tiers.append((tactic, None, "tactic"))
        search_tiers.append((None, None, "persona+platform"))

        status_weight = {
            "replied": 1.0,
            "delivered": 0.82,
            "dispatched": 0.78,
            "approved": 0.70,
            "promoted": 0.64,
            "selected": 0.52,
            "drafted": 0.30,
            "rejected": 0.10,
            "failed": 0.06,
            "dead": 0.02,
        }

        for scoped_tactic, scoped_objective, scope_name in search_tiers:
            clauses = ["mf.persona = ?", "mf.platform = ?"]
            params: list[Any] = [persona, platform]
            if scoped_tactic is not None:
                clauses.append("mf.tactic = ?")
                params.append(scoped_tactic)
            if scoped_objective is not None:
                clauses.append("mf.objective = ?")
                params.append(scoped_objective)
            if days > 0:
                clauses.append("mv.created_at > datetime('now', ?)")
                params.append(f"-{int(max(days, 1))} days")

            sql = f"""
                SELECT mv.status, mv.created_at, mf.winner_score
                FROM mutation_variants mv
                JOIN mutation_families mf ON mf.id = mv.family_id
                WHERE {' AND '.join(clauses)}
                ORDER BY mv.created_at DESC, mv.id DESC
                LIMIT 300
            """
            with open_db(self.path) as conn:
                rows = [dict(row) for row in conn.execute(sql, tuple(params)).fetchall()]

            if not rows:
                continue

            sample_count = len(rows)
            if sample_count < min_samples and scope_name != "persona+platform":
                continue

            status_total = 0.0
            winner_total = 0.0
            recency_total = 0.0
            now_ts = time.time()
            for row in rows:
                status = str(row.get("status") or "drafted").strip().lower()
                status_total += status_weight.get(status, 0.25)
                winner_total += self._bounded_score(float(row.get("winner_score") or 0.0))

                created_ts = self._parse_created_at(row.get("created_at"))
                if created_ts is None or days <= 0:
                    recency_total += 0.5
                else:
                    age_days = max(0.0, (now_ts - created_ts) / 86400.0)
                    recency_total += max(0.0, 1.0 - min(age_days / max(float(days), 1.0), 1.0))

            status_avg = status_total / max(sample_count, 1)
            winner_avg = winner_total / max(sample_count, 1)
            recency_avg = recency_total / max(sample_count, 1)

            confidence = self._bounded_score(min(1.0, sample_count / 36.0) * (0.7 + (0.3 * recency_avg)))
            prior_raw = self._bounded_score((0.62 * status_avg) + (0.38 * winner_avg))
            prior_score = self._bounded_score((0.5 * (1.0 - confidence)) + (prior_raw * confidence))

            return {
                "prior_score": round(prior_score, 4),
                "confidence": round(confidence, 4),
                "sample_count": sample_count,
                "scope": scope_name,
                "components": {
                    "status_avg": round(status_avg, 4),
                    "winner_avg": round(winner_avg, 4),
                    "recency_avg": round(recency_avg, 4),
                },
            }

        return {
            "prior_score": 0.5,
            "confidence": 0.0,
            "sample_count": 0,
            "scope": "persona+platform",
            "components": {},
        }

    def _persona_pressure_adjustments(
        self,
        *,
        persona: str,
        platform: str,
        days: int = 45,
        sample_limit: int = 120,
    ) -> tuple[str | None, list[str]]:
        sql = """
            SELECT o.tone_shift, o.spectator_engagement
            FROM outcomes o
            JOIN runs r ON r.id = o.run_id
            WHERE r.persona = ?
              AND r.platform = ?
              AND o.got_reply = 1
              AND r.created_at > datetime('now', ?)
            ORDER BY r.created_at DESC, o.id DESC
            LIMIT ?
        """
        with open_db(self.path) as conn:
            rows = conn.execute(sql, (persona, platform, f"-{int(max(days, 1))} days", int(max(sample_limit, 1)))).fetchall()

        if not rows:
            return None, []

        tone_counts: dict[str, int] = {}
        crowd_total = 0.0
        for row in rows:
            tone = str(row["tone_shift"] or "unknown").strip().lower()
            tone_counts[tone] = tone_counts.get(tone, 0) + 1
            crowd_total += float(row["spectator_engagement"] or 0.0)

        total = max(len(rows), 1)
        dominant_tone, dominant_count = max(tone_counts.items(), key=lambda item: item[1])
        dominant_ratio = dominant_count / total
        avg_crowd = crowd_total / total

        cues: list[str] = []
        if dominant_tone in {"defensive", "guarded"}:
            cues.extend(["premise lock", "forced fork", "no escape clause"])
        elif dominant_tone in {"hostile", "angry", "aggressive"}:
            cues.extend(["cool de-escalation", "single-claim clamp", "low-word dominance"])
        elif dominant_tone in {"dismissive", "smug", "mocking"}:
            cues.extend(["status jab", "confidence puncture", "audience receipt"])
        elif dominant_tone in {"rambling", "digressive", "essay"}:
            cues.extend(["precision question", "narrow corridor", "time-boxed demand"])

        if avg_crowd >= 4.0:
            cues.append("crowd-facing line")

        cue_context = None
        if dominant_ratio >= 0.4 and cues:
            cue_context = f"tone-shift trend: {dominant_tone} ({dominant_count}/{total})"
        return cue_context, list(dict.fromkeys(cues))[:4]

    @staticmethod
    def _apply_persona_pressure_adjustments(
        persona: Any,
        *,
        cue_context: str | None,
        extra_cues: list[str],
    ) -> Any:
        if not cue_context and not extra_cues:
            return persona
        profile = persona.model_copy(deep=True)
        merged = list(dict.fromkeys([*(profile.escalation_cues or []), *extra_cues]))
        profile.escalation_cues = merged[:6]
        return profile

    @staticmethod
    def _derive_mutation_context(seeds: list[MutationSeed]) -> tuple[str | None, list[str], list[str]]:
        if not seeds:
            return None, [], []
        anchors: list[str] = []
        avoid: list[str] = []
        for seed in seeds:
            text = str(seed.text or "").strip()
            if not text:
                continue
            anchors.append(text)
            transform = str(seed.transform or "").lower()
            if transform in {"temperature_shift", "remove_hedge", "hedge_shift"}:
                avoid.append("soft hedge language")
            if transform in {"inject_contrast", "insert_turn", "pivot"}:
                avoid.append("single-claim monotony")
            if transform in {"lengthen", "expand"}:
                avoid.append("one-liners that leave no hook")
        unique_anchors = list(dict.fromkeys(anchors))[:3]
        unique_avoid = list(dict.fromkeys(avoid))[:5]
        snippets = [f"{seed.transform or 'variant'}:{(seed.text or '').strip()[:80]}" for seed in seeds[:3]]
        context = " | ".join(snippets) if snippets else None
        return context, unique_anchors, unique_avoid

    @staticmethod
    def _clamp_unit(value: float) -> float:
        return max(0.0, min(1.0, value))

    @classmethod
    def _extract_axis_scores_from_analysis_json(cls, payload: str | None) -> dict[RhetoricalAxis, float]:
        if not payload:
            return {}
        try:
            doc = json.loads(payload)
        except json.JSONDecodeError:
            return {}

        rows = doc.get("axes") if isinstance(doc, dict) else None
        if not isinstance(rows, list):
            return {}

        out: dict[RhetoricalAxis, float] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            axis_name = row.get("axis")
            score = row.get("score")
            if not isinstance(axis_name, str) or not isinstance(score, (int, float)):
                continue
            try:
                axis = RhetoricalAxis(axis_name)
            except ValueError:
                continue
            out[axis] = cls._clamp_unit(float(score))
        return out

    @classmethod
    def _outcome_quality_score(cls, row: dict[str, Any]) -> float:
        got_reply = 1.0 if row.get("got_reply") else 0.0
        engagement = cls._clamp_unit(float((row.get("spectator_engagement") or 0) / 10.0))
        reply_length = cls._clamp_unit(float((row.get("reply_length") or 0) / 240.0))
        delay_raw = row.get("reply_delay_seconds")
        if isinstance(delay_raw, (int, float)) and delay_raw >= 0:
            speed = 1.0 - cls._clamp_unit(float(delay_raw) / 7200.0)
        else:
            speed = 0.35
        return round(0.6 * got_reply + 0.2 * engagement + 0.1 * speed + 0.1 * reply_length, 4)

    @classmethod
    def _confidence_from_samples(cls, sample_size: int, min_samples: int) -> float:
        if sample_size <= min_samples:
            return 0.0
        span = max(min_samples, 1)
        return cls._clamp_unit((sample_size - min_samples) / span)

    def _build_archetype_weight_profile(
        self,
        *,
        persona: str,
        platform: str,
        objective: str,
        min_samples: int = 8,
        lookback_days: int = 90,
    ) -> dict[str, Any]:
        with open_db(self.path) as conn:
            rows = conn.execute(
                """
                SELECT
                    r.analysis_json,
                    o.got_reply,
                    o.reply_delay_seconds,
                    o.reply_length,
                    o.spectator_engagement
                FROM runs r
                JOIN outcomes o ON o.run_id = r.id
                WHERE r.persona = ?
                  AND r.platform = ?
                  AND r.selected_objective = ?
                  AND r.created_at > datetime('now', ?)
                ORDER BY r.id DESC
                """,
                (persona, platform, objective, f"-{int(lookback_days)} days"),
            ).fetchall()

        sample_size = len(rows)
        if sample_size < min_samples:
            return {
                "enabled": False,
                "persona": persona,
                "platform": platform,
                "objective": objective,
                "sample_size": sample_size,
                "min_samples": min_samples,
                "confidence": 0.0,
                "fallback_reason": "sparse_segment",
                "weights": {},
            }

        axis_sum_all: dict[RhetoricalAxis, float] = {}
        axis_sum_success: dict[RhetoricalAxis, float] = {}
        total_success_weight = 0.0

        for row in rows:
            row_dict = dict(row)
            axis_scores = self._extract_axis_scores_from_analysis_json(row_dict.get("analysis_json"))
            if not axis_scores:
                continue
            for axis, value in axis_scores.items():
                axis_sum_all[axis] = axis_sum_all.get(axis, 0.0) + value

            quality = self._outcome_quality_score(row_dict)
            total_success_weight += quality
            for axis, value in axis_scores.items():
                axis_sum_success[axis] = axis_sum_success.get(axis, 0.0) + (value * quality)

        if not axis_sum_all or total_success_weight <= 0:
            return {
                "enabled": False,
                "persona": persona,
                "platform": platform,
                "objective": objective,
                "sample_size": sample_size,
                "min_samples": min_samples,
                "confidence": 0.0,
                "fallback_reason": "insufficient_signal",
                "weights": {},
            }

        axis_mean_all = {
            axis: axis_sum_all.get(axis, 0.0) / max(sample_size, 1)
            for axis in axis_sum_all
        }
        axis_mean_success = {
            axis: axis_sum_success.get(axis, 0.0) / max(total_success_weight, 1e-6)
            for axis in axis_sum_success
        }

        confidence = self._confidence_from_samples(sample_size, min_samples)
        adjustment_strength = 1.15
        learned_weights: dict[str, dict[str, float]] = {}

        for archetype, base_axes in BASE_ARCHETYPE_AXIS_WEIGHTS.items():
            updated: dict[str, float] = {}
            for axis, base_weight in base_axes.items():
                lift = axis_mean_success.get(axis, axis_mean_all.get(axis, 0.0)) - axis_mean_all.get(axis, 0.0)
                scale = 1.0 + (lift * adjustment_strength)
                learned = float(base_weight) * scale
                if learned >= 0:
                    learned = min(0.85, max(0.0, learned))
                else:
                    learned = max(-0.85, min(0.0, learned))
                updated[axis.value] = round(learned, 4)
            learned_weights[archetype.value] = updated

        return {
            "enabled": confidence > 0,
            "persona": persona,
            "platform": platform,
            "objective": objective,
            "sample_size": sample_size,
            "min_samples": min_samples,
            "confidence": round(confidence, 4),
            "fallback_reason": None if confidence > 0 else "confidence_floor",
            "weights": learned_weights,
        }

    @staticmethod
    def _semantic_inversion_risk(analysis: Any) -> float:
        risk = 0.0
        axes = getattr(analysis, "axes", []) or []
        for axis in axes:
            for reason in getattr(axis, "reasons", []) or []:
                text = str(reason or "").strip().lower()
                if "semantic inversion attenuation=" in text:
                    try:
                        value = float(text.split("semantic inversion attenuation=")[-1].strip())
                        risk = max(risk, min(max(value, 0.0), 1.0))
                    except ValueError:
                        risk = max(risk, 0.6)
                elif "semantic inversion" in text:
                    risk = max(risk, 0.6)
        return round(min(max(risk, 0.0), 1.0), 4)

    @staticmethod
    def _persona_duel_margin(persona_router: Any) -> float:
        if persona_router is None:
            return 1.0
        scores = getattr(persona_router, "persona_scores", {}) or {}
        if not isinstance(scores, dict) or len(scores) < 2:
            return 1.0
        ranked = sorted((float(value or 0.0) for value in scores.values()), reverse=True)
        if len(ranked) < 2:
            return 1.0
        return round(max(0.0, ranked[0] - ranked[1]), 4)

    def _escalation_policy(self, conn: Any | None = None) -> dict[str, Any]:
        policy = self._base_escalation_policy()
        if conn is None:
            return policy
        controls = self._effective_dispatch_controls(conn)
        return self._merge_control_overrides(policy, controls.get("escalation") or {})

    def _daily_hard_escalation_count(self, conn: Any, *, now_ts: float) -> int:
        start_of_day = datetime.fromtimestamp(now_ts, tz=timezone.utc).strftime("%Y-%m-%d") + " 00:00:00"
        rows = conn.execute(
            "SELECT plan_json FROM runs WHERE created_at >= ?",
            (start_of_day,),
        ).fetchall()
        count = 0
        for row in rows:
            try:
                payload = json.loads(row["plan_json"])
            except Exception:
                continue
            if str(payload.get("selected_model_tier") or "") == "hard":
                count += 1
        return count

    def _select_generation_model(
        self,
        conn: Any,
        *,
        analysis: Any,
        plan: DecisionPlan,
        persona_router: Any,
        now_ts: float,
    ) -> dict[str, Any]:
        policy = self._escalation_policy(conn)
        reasons: list[str] = []

        router_confidence = float(getattr(persona_router, "confidence", 1.0) if persona_router is not None else 1.0)
        if router_confidence < float(policy["low_router_confidence_threshold"]):
            reasons.append("low_router_confidence")

        duel_margin = self._persona_duel_margin(persona_router)
        if duel_margin <= float(policy["persona_duel_margin_threshold"]):
            reasons.append("persona_duel_inconclusive")

        inversion_risk = self._semantic_inversion_risk(analysis)
        if inversion_risk >= float(policy["semantic_inversion_risk_threshold"]):
            reasons.append("high_semantic_inversion_risk")

        opportunity_value = float(getattr(getattr(analysis, "opportunity", None), "engagement_value", 0.0) or 0.0)
        if opportunity_value >= float(policy["high_value_opportunity_threshold"]):
            reasons.append("high_value_target")

        requested = len(reasons) > 0
        per_run_cap = int(policy["per_run_escalation_cap"])
        daily_cap = int(policy["daily_escalation_cap"])
        daily_used = self._daily_hard_escalation_count(conn, now_ts=now_ts)

        denied_reason = None
        if requested:
            if per_run_cap < 1:
                denied_reason = "escalation_budget_per_run_exhausted"
            elif daily_used >= daily_cap:
                denied_reason = "escalation_budget_daily_exhausted"

        applied = requested and denied_reason is None
        selected_tier = "hard" if applied else "cheap"
        selected_model = str(policy["hard_model"] if applied else policy["cheap_model"])

        budget_state = {
            "requested": requested,
            "applied": applied,
            "per_run_cap": per_run_cap,
            "per_run_used": 1 if applied else 0,
            "daily_cap": daily_cap,
            "daily_used": daily_used,
            "daily_remaining": max(0, daily_cap - daily_used),
            "denied_reason": denied_reason,
        }

        return {
            "selected_model": selected_model,
            "selected_model_tier": selected_tier,
            "escalation_reasons": reasons,
            "budget_state": budget_state,
            "metrics": {
                "router_confidence": round(router_confidence, 4),
                "persona_duel_margin": duel_margin,
                "semantic_inversion_risk": inversion_risk,
                "opportunity_value": round(opportunity_value, 4),
            },
            "policy": policy,
        }

    def create_run_from_text(
        self,
        text: str,
        persona_name: str = "dry_midwit_savant",
        platform: str = "cli",
        candidate_count: int = 5,
        provider: TextGenerationProvider | None = None,
        heuristic_only: bool = False,
        force_engage: bool = False,
        mutation_source: str = "auto",
    ) -> dict[str, Any]:
        logger.info("create_run_from_text: persona=%s platform=%s heuristic_only=%s", persona_name, platform, heuristic_only)
        bootstrap_analysis = analyze_comment(AnalyzeInput(text=text, platform=platform))
        resolved_persona_name = persona_name
        persona_router = None
        if str(persona_name or "").strip().lower() == "auto":
            objective_hint = (
                bootstrap_analysis.recommended_objectives[0].value
                if bootstrap_analysis.recommended_objectives
                else None
            )
            persona_router = select_persona(
                bootstrap_analysis,
                platform=platform,
                priors=self._persona_router_priors(platform=platform),
                calibration=self.persona_router_calibration(platform=platform, objective=objective_hint),
            )
            resolved_persona_name = persona_router.selected_persona

        bootstrap_plan = build_plan(
            bootstrap_analysis,
            persona=resolved_persona_name,
            persona_router=persona_router,
        )

        archetype_profile = self._build_archetype_weight_profile(
            persona=resolved_persona_name,
            platform=platform,
            objective=bootstrap_plan.selected_objective.value,
        )
        analysis = analyze_comment(
            AnalyzeInput(
                text=text,
                platform=platform,
                archetype_weight_profile=archetype_profile,
            )
        )

        if force_engage and TacticalObjective.DO_NOT_ENGAGE in (analysis.recommended_objectives or []):
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
        if str(persona_name or "").strip().lower() == "auto":
            objective_hint = (
                analysis.recommended_objectives[0].value
                if analysis.recommended_objectives
                else None
            )
            persona_router = select_persona(
                analysis,
                platform=platform,
                priors=self._persona_router_priors(platform=platform),
                calibration=self.persona_router_calibration(platform=platform, objective=objective_hint),
            )
            resolved_persona_name = persona_router.selected_persona

        plan = build_plan(analysis, persona=resolved_persona_name, persona_router=persona_router)
        base_persona = get_persona(resolved_persona_name)
        pressure_context, pressure_cues = self._persona_pressure_adjustments(
            persona=base_persona.name,
            platform=platform,
        )
        persona = self._apply_persona_pressure_adjustments(
            base_persona,
            cue_context=pressure_context,
            extra_cues=pressure_cues,
        )
        mutation_seeds = (
            self._select_mutation_seeds(
                persona=persona.name,
                platform=platform,
                tactic=plan.selected_tactic.value if plan.selected_tactic else None,
                objective=plan.selected_objective.value if plan.selected_objective else None,
                limit=min(candidate_count, 3),
            )
            if mutation_source == "auto"
            else []
        )
        mutation_context, winner_anchors, avoid_patterns = self._derive_mutation_context(mutation_seeds)
        if pressure_context:
            mutation_context = f"{mutation_context} | {pressure_context}" if mutation_context else pressure_context

        with open_db(self.path) as conn:
            escalation_decision = self._select_generation_model(
                conn,
                analysis=analysis,
                plan=plan,
                persona_router=persona_router,
                now_ts=time.time(),
            )

        request = DraftRequest(
            source_text=text,
            plan=plan,
            persona=persona,
            candidate_count=candidate_count,
            mutation_seeds=mutation_seeds,
            mutation_context=mutation_context,
            winner_anchors=winner_anchors,
            avoid_patterns=avoid_patterns,
            target_register=analysis.target_register,
        )

        selected_model = str(escalation_decision.get("selected_model") or "")
        draft = None
        if heuristic_only:
            draft = draft_candidates(request)
        else:
            active_provider = provider
            if active_provider is None:
                active_provider = OpenAICompatibleProvider(model=selected_model or None)
            original_model = getattr(active_provider, "model", None)
            model_swapped = hasattr(active_provider, "model")
            if model_swapped and selected_model:
                setattr(active_provider, "model", selected_model)
            try:
                draft = draft_candidates_with_provider(request, provider=active_provider)
            finally:
                if model_swapped and original_model is not None:
                    setattr(active_provider, "model", original_model)

        requested_persona_name = str(persona_name or "").strip() or resolved_persona_name
        selection_mode = "auto" if requested_persona_name.lower() == "auto" else "forced"
        plan_payload = plan.model_dump(mode="json")
        plan_payload["persona_selection"] = {
            "mode": selection_mode,
            "requested_persona": requested_persona_name,
            "resolved_persona": persona.name,
            "router_used": bool(persona_router is not None),
        }
        plan_payload["selected_model_tier"] = escalation_decision.get("selected_model_tier") or "cheap"
        plan_payload["selected_model"] = selected_model
        plan_payload["escalation_reasons"] = list(escalation_decision.get("escalation_reasons") or [])
        plan_payload["budget_state"] = dict(escalation_decision.get("budget_state") or {})
        plan_payload["escalation_metrics"] = dict(escalation_decision.get("metrics") or {})

        with open_db(self.path) as conn:
            cur = conn.execute(
                """
                INSERT INTO runs (
                    source_text, platform, persona, selected_objective, selected_tactic, exit_state, analysis_json, plan_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    text,
                    platform,
                    persona.name,
                    plan.selected_objective.value,
                    plan.selected_tactic.value if plan.selected_tactic else None,
                    plan.exit_state,
                    to_json(analysis.model_dump(mode="json")),
                    to_json(plan_payload),
                ),
            )
            run_id = int(cur.lastrowid)
            for candidate in candidates_from_draft(run_id, draft):
                conn.execute(
                    """
                    INSERT INTO candidates (
                        run_id, rank_index, text, tactic, objective, estimated_bite_score,
                        estimated_audience_score, critic_penalty, rank_score, critic_notes_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        candidate.run_id,
                        candidate.rank_index,
                        candidate.text,
                        candidate.tactic,
                        candidate.objective,
                        candidate.estimated_bite_score,
                        candidate.estimated_audience_score,
                        candidate.critic_penalty,
                        candidate.rank_score,
                        candidate.critic_notes_json,
                    ),
                )

        logger.info("create_run_from_text: saved run_id=%d persona=%s objective=%s", run_id, persona.name, plan.selected_objective.value)
        return self.get_run(run_id)

    def list_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        with open_db(self.path) as conn:
            rows = conn.execute(
                """
                SELECT id, created_at, platform, persona, selected_objective, selected_tactic, exit_state, source_text
                FROM runs
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_full_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        run_rows = self.list_runs(limit=limit)
        return [self.get_run(run["id"]) for run in run_rows]

    def get_run(self, run_id: int) -> dict[str, Any]:
        with open_db(self.path) as conn:
            run = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
            if not run:
                raise KeyError(f"run {run_id} not found")
            candidates = conn.execute(
                "SELECT * FROM candidates WHERE run_id = ? ORDER BY rank_index ASC",
                (run_id,),
            ).fetchall()
            outcome = conn.execute("SELECT * FROM outcomes WHERE run_id = ?", (run_id,)).fetchone()
            panel_reviews = conn.execute(
                "SELECT * FROM panel_reviews WHERE run_id = ? ORDER BY created_at DESC, id DESC",
                (run_id,),
            ).fetchall()
            emit_outbox = conn.execute(
                "SELECT * FROM emit_outbox WHERE run_id = ? ORDER BY created_at DESC, id DESC",
                (run_id,),
            ).fetchall()
            emit_dispatches = conn.execute(
                "SELECT * FROM emit_dispatches WHERE run_id = ? ORDER BY created_at DESC, id DESC",
                (run_id,),
            ).fetchall()

        run_dict = dict(run)
        run_dict["analysis"] = json.loads(run_dict.pop("analysis_json"))
        run_dict["plan"] = json.loads(run_dict.pop("plan_json"))
        run_dict["candidates"] = [self._decode_candidate(dict(row)) for row in candidates]
        run_dict["outcome"] = dict(outcome) if outcome else None
        run_dict["panel_reviews"] = [dict(row) for row in panel_reviews]
        run_dict["emit_outbox"] = [self._decode_emit_outbox(dict(row)) for row in emit_outbox]
        run_dict["emit_dispatches"] = [self._decode_emit_dispatch(dict(row)) for row in emit_dispatches]
        return run_dict

    def list_intake_targets(
        self,
        *,
        limit: int = 100,
        status: str | None = None,
        platform: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        if platform is not None:
            clauses.append("platform = ?")
            params.append(platform)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"SELECT * FROM intake_targets {where} ORDER BY created_at DESC, id DESC LIMIT ?"
        params.append(limit)
        with open_db(self.path) as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
        return [self._decode_intake_target(dict(row)) for row in rows]

    def get_intake_target(self, target_id: int) -> dict[str, Any]:
        with open_db(self.path) as conn:
            row = conn.execute("SELECT * FROM intake_targets WHERE id = ?", (target_id,)).fetchone()
        if not row:
            raise KeyError(f"intake_target {target_id} not found")
        return self._decode_intake_target(dict(row))

    def upsert_intake_target(self, target: IntakeTargetRecord) -> dict[str, Any]:
        with open_db(self.path) as conn:
            current = conn.execute(
                "SELECT * FROM intake_targets WHERE source_driver = ? AND source_item_id = ?",
                (target.source_driver, target.source_item_id),
            ).fetchone()
            if current:
                current_dict = dict(current)
                protected_statuses = {"promoted", "staged", "approved", "dispatched", "acknowledged", "delivered"}
                next_status = current_dict.get("status") if current_dict.get("status") in protected_statuses else target.status
                conn.execute(
                    """
                    UPDATE intake_targets
                    SET updated_at = CURRENT_TIMESTAMP,
                        platform = ?,
                        thread_id = ?,
                        reply_to_id = ?,
                        author_handle = ?,
                        subject = ?,
                        body = ?,
                        permalink = ?,
                        status = ?,
                        score_json = ?,
                        analysis_json = ?,
                        context_json = ?,
                        metadata_json = ?,
                        promoted_run_id = COALESCE(promoted_run_id, ?),
                        emit_outbox_id = COALESCE(emit_outbox_id, ?)
                    WHERE id = ?
                    """,
                    (
                        target.platform,
                        target.thread_id,
                        target.reply_to_id,
                        target.author_handle,
                        target.subject,
                        target.body,
                        target.permalink,
                        next_status,
                        target.score_json,
                        target.analysis_json,
                        target.context_json,
                        target.metadata_json,
                        target.promoted_run_id,
                        target.emit_outbox_id,
                        int(current_dict["id"]),
                    ),
                )
                target_id = int(current_dict["id"])
            else:
                cur = conn.execute(
                    """
                    INSERT INTO intake_targets (
                        source_driver, source_item_id, platform, thread_id, reply_to_id,
                        author_handle, subject, body, permalink, status, score_json,
                        analysis_json, context_json, metadata_json, promoted_run_id, emit_outbox_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        target.source_driver,
                        target.source_item_id,
                        target.platform,
                        target.thread_id,
                        target.reply_to_id,
                        target.author_handle,
                        target.subject,
                        target.body,
                        target.permalink,
                        target.status,
                        target.score_json,
                        target.analysis_json,
                        target.context_json,
                        target.metadata_json,
                        target.promoted_run_id,
                        target.emit_outbox_id,
                    ),
                )
                target_id = int(cur.lastrowid)
        return self.get_intake_target(target_id)

    def promote_intake_target(
        self,
        target_id: int,
        *,
        persona_name: str = "dry_midwit_savant",
        candidate_count: int = 5,
        provider: TextGenerationProvider | None = None,
        heuristic_only: bool = False,
        stage_emit: bool = False,
        approve_emit: bool = False,
        selection_preset: str | None = "auto",
        selection_strategy: str = "rank",
        tactic: str | None = None,
        objective: str | None = None,
        force: bool = False,
    ) -> dict[str, Any]:
        target = self.get_intake_target(target_id)
        existing_run_id = target.get("promoted_run_id")
        if existing_run_id and not force:
            run = self.get_run(int(existing_run_id))
            emit = None
            emit_outbox_id = target.get("emit_outbox_id")
            if emit_outbox_id:
                emit = next((item for item in run.get("emit_outbox") or [] if int(item.get("id") or 0) == int(emit_outbox_id)), None)
            return {"already_promoted": True, "target": target, "run": run, "emit": emit}

        source_text = compose_source_text(target.get("subject"), target.get("body") or "")
        run = self.create_run_from_text(
            source_text,
            persona_name=persona_name,
            platform=str(target.get("platform") or "reddit"),
            candidate_count=candidate_count,
            provider=provider,
            heuristic_only=heuristic_only,
        )

        staged_emit = None
        next_status = "promoted"
        if stage_emit:
            context = target.get("context") if isinstance(target.get("context"), dict) else None
            reputation = self.get_persona_reputation(run.get("persona") or persona_name, run.get("platform") or str(target.get("platform") or "reddit"))
            envelope = build_reply_envelope(
                run,
                candidate_rank_index=1,
                selection_strategy=selection_strategy,
                selection_preset=selection_preset,
                tactic=tactic,
                objective=objective,
                thread_id=str(target.get("thread_id") or ""),
                reply_to_id=str(target.get("reply_to_id") or target.get("thread_id") or ""),
                author_handle=target.get("author_handle"),
                context=context,
                reputation_data=reputation,
                metadata={
                    "hunt_target_id": int(target["id"]),
                    "hunt_source_driver": target.get("source_driver"),
                    "hunt_score": (target.get("score") or {}).get("score"),
                },
            )
            emit_request = build_emit_request(envelope)
            envelope_meta = envelope.get("metadata") if isinstance(envelope.get("metadata"), dict) else {}
            staged = self.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=int(run["id"]),
                    platform=str(target.get("platform") or run.get("platform") or "reddit"),
                    transport=str(emit_request["transport"]),
                    selection_preset=(envelope_meta.get("selection_preset") if envelope_meta.get("selection_preset") is not None else selection_preset),
                    selection_strategy=str(envelope_meta.get("selection_strategy") or selection_strategy),
                    tactic=envelope.get("tactic"),
                    objective=envelope.get("objective"),
                    status="approved" if approve_emit else "staged",
                    envelope_json=to_json(envelope),
                    emit_request_json=to_json(emit_request),
                    notes=f"promoted from intake_target #{int(target['id'])}",
                )
            )
            staged_emit = (staged.get("emit_outbox") or [None])[0]
            next_status = "approved" if approve_emit else "staged"
            run = staged

        with open_db(self.path) as conn:
            conn.execute(
                """
                UPDATE intake_targets
                SET updated_at = CURRENT_TIMESTAMP,
                    status = ?,
                    promoted_run_id = ?,
                    emit_outbox_id = ?
                WHERE id = ?
                """,
                (
                    next_status,
                    int(run["id"]),
                    int(staged_emit["id"]) if isinstance(staged_emit, dict) and staged_emit.get("id") is not None else None,
                    int(target_id),
                ),
            )
        return {"already_promoted": False, "target": self.get_intake_target(target_id), "run": run, "emit": staged_emit}

    def record_outcome(self, outcome: OutcomeRecord) -> dict[str, Any]:
        logger.info("record_outcome: run_id=%s got_reply=%s result=%s", outcome.run_id, outcome.got_reply, outcome.result_label)
        emit_outbox_id = outcome.emit_outbox_id
        emit_dispatch_id = outcome.emit_dispatch_id

        with open_db(self.path) as conn:
            if emit_outbox_id is None or emit_dispatch_id is None:
                latest_dispatch = conn.execute(
                    """
                    SELECT id, emit_outbox_id
                    FROM emit_dispatches
                    WHERE run_id = ?
                    ORDER BY created_at DESC, id DESC
                    LIMIT 1
                    """,
                    (outcome.run_id,),
                ).fetchone()
                if latest_dispatch:
                    if emit_dispatch_id is None:
                        emit_dispatch_id = int(latest_dispatch["id"])
                    if emit_outbox_id is None:
                        emit_outbox_id = int(latest_dispatch["emit_outbox_id"])

            conn.execute(
                """
                INSERT INTO outcomes (
                    run_id, got_reply, reply_delay_seconds, reply_length, tone_shift,
                    spectator_engagement, result_label, notes, emit_outbox_id, emit_dispatch_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    got_reply=excluded.got_reply,
                    reply_delay_seconds=excluded.reply_delay_seconds,
                    reply_length=excluded.reply_length,
                    tone_shift=excluded.tone_shift,
                    spectator_engagement=excluded.spectator_engagement,
                    result_label=excluded.result_label,
                    notes=excluded.notes,
                    emit_outbox_id=excluded.emit_outbox_id,
                    emit_dispatch_id=excluded.emit_dispatch_id
                """,
                (
                    outcome.run_id,
                    int(outcome.got_reply),
                    outcome.reply_delay_seconds,
                    outcome.reply_length,
                    outcome.tone_shift,
                    outcome.spectator_engagement,
                    outcome.result_label,
                    outcome.notes,
                    emit_outbox_id,
                    emit_dispatch_id,
                ),
            )
        return self.get_run(outcome.run_id)

    def record_panel_review(self, review: PanelReviewRecord) -> dict[str, Any]:
        with open_db(self.path) as conn:
            conn.execute(
                """
                INSERT INTO panel_reviews (
                    run_id, platform, persona, candidate_tactic, candidate_objective,
                    selection_preset, selection_strategy, disposition, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    review.run_id,
                    review.platform,
                    review.persona,
                    review.candidate_tactic,
                    review.candidate_objective,
                    review.selection_preset,
                    review.selection_strategy,
                    review.disposition,
                    review.notes,
                ),
            )
        return self.get_run(review.run_id)

    def list_panel_reviews(self, limit: int = 100) -> list[dict[str, Any]]:
        with open_db(self.path) as conn:
            rows = conn.execute(
                "SELECT * FROM panel_reviews ORDER BY created_at DESC, id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def stage_emit(self, emit: EmitOutboxRecord) -> dict[str, Any]:
        logger.info("stage_emit: run_id=%s platform=%s transport=%s", emit.run_id, emit.platform, emit.transport)
        with open_db(self.path) as conn:
            conn.execute(
                """
                INSERT INTO emit_outbox (
                    run_id, platform, transport, selection_preset, selection_strategy,
                    tactic, objective, status, envelope_json, emit_request_json, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    emit.run_id,
                    emit.platform,
                    emit.transport,
                    emit.selection_preset,
                    emit.selection_strategy,
                    emit.tactic,
                    emit.objective,
                    emit.status,
                    emit.envelope_json,
                    emit.emit_request_json,
                    emit.notes,
                ),
            )
        return self.get_run(emit.run_id)

    def list_emit_outbox(self, limit: int = 100, status: str | None = None) -> list[dict[str, Any]]:
        with open_db(self.path) as conn:
            if status is None:
                rows = conn.execute(
                    "SELECT * FROM emit_outbox ORDER BY created_at DESC, id DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM emit_outbox WHERE status = ? ORDER BY created_at DESC, id DESC LIMIT ?",
                    (status, limit),
                ).fetchall()
        return [self._decode_emit_outbox(dict(row)) for row in rows]

    def get_retry_queue_summary(self, limit: int = 100) -> dict[str, Any]:
        with open_db(self.path) as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM emit_outbox
                WHERE status IN ('failed', 'dead_letter')
                ORDER BY COALESCE(next_retry_at, created_at) ASC, id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        now = time.time()
        due: list[dict[str, Any]] = []
        waiting: list[dict[str, Any]] = []
        dead_letter: list[dict[str, Any]] = []
        for row in rows:
            item = self._decode_emit_outbox(dict(row))
            status = str(item.get("status") or "")
            if status == "dead_letter":
                dead_letter.append(item)
                continue
            if self._is_due(item.get("next_retry_at"), now=now):
                due.append(item)
            else:
                waiting.append(item)
        return {
            "limit": limit,
            "due_count": len(due),
            "waiting_count": len(waiting),
            "dead_letter_count": len(dead_letter),
            "due": due,
            "waiting": waiting,
            "dead_letter": dead_letter,
        }

    def update_emit_outbox(
        self,
        emit_id: int,
        *,
        status: str | None = None,
        notes: str | None = None,
        notes_mode: str = "append",
    ) -> dict[str, Any]:
        with open_db(self.path) as conn:
            current = conn.execute("SELECT run_id, status, notes FROM emit_outbox WHERE id = ?", (emit_id,)).fetchone()
            if not current:
                raise KeyError(f"emit_outbox {emit_id} not found")
            next_status = status or str(current["status"])
            merged_notes = current["notes"]
            if notes is not None:
                if notes_mode == "replace":
                    merged_notes = notes
                elif notes_mode == "append":
                    merged_notes = f"{merged_notes}\n{notes}" if merged_notes and notes else (notes or merged_notes)
                else:
                    raise ValueError(f"unknown notes_mode: {notes_mode}")
            conn.execute(
                "UPDATE emit_outbox SET status = ?, notes = ? WHERE id = ?",
                (next_status, merged_notes, emit_id),
            )
            run_id = int(current["run_id"])
        return self.get_run(run_id)

    def update_emit_outbox_status(self, emit_id: int, status: str, notes: str | None = None) -> dict[str, Any]:
        return self.update_emit_outbox(emit_id, status=status, notes=notes, notes_mode="append")

    def list_emit_dispatches(
        self,
        limit: int = 100,
        emit_outbox_id: int | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if emit_outbox_id is not None:
            clauses.append("emit_outbox_id = ?")
            params.append(emit_outbox_id)
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with open_db(self.path) as conn:
            rows = conn.execute(
                f"SELECT * FROM emit_dispatches {where} ORDER BY created_at DESC, id DESC LIMIT ?",
                (*params, limit),
            ).fetchall()
        return [self._decode_emit_dispatch(dict(row)) for row in rows]

    def dispatch_emit(
        self,
        emit_id: int,
        *,
        driver: str = "auto",
        out_dir: str | Path | None = None,
        notes: str | None = None,
        force: bool = False,
    ) -> dict[str, Any]:
        with open_db(self.path) as conn:
            current = conn.execute("SELECT * FROM emit_outbox WHERE id = ?", (emit_id,)).fetchone()
            if not current:
                raise KeyError(f"emit_outbox {emit_id} not found")
            current_dict = self._decode_emit_outbox(dict(current))
            current_status = str(current_dict.get("status") or "")
            if not force and current_status != "approved":
                raise ValueError(f"emit_outbox {emit_id} must be approved before dispatch (current: {current_status})")

            emit_request_payload = current_dict.get("emit_request") or {}
            dispatch_request = {
                "emit_id": emit_id,
                "run_id": int(current_dict["run_id"]),
                "platform": current_dict.get("platform"),
                "transport": current_dict.get("transport"),
                "envelope": current_dict.get("envelope"),
                "emit_request": emit_request_payload,
                "selection_preset": current_dict.get("selection_preset"),
                "selection_strategy": current_dict.get("selection_strategy"),
                "tactic": current_dict.get("tactic"),
                "objective": current_dict.get("objective"),
                "notes": current_dict.get("notes"),
            }
            dispatch_request_audit = self._redact_sensitive(dispatch_request)
            preferred_driver = ((emit_request_payload.get("metadata") or {}).get("preferred_dispatch_driver") if isinstance(emit_request_payload, dict) else None)
            if str(driver or "") == "auto" and preferred_driver:
                effective_driver = str(preferred_driver)
            else:
                effective_driver = select_dispatch_driver(current_dict.get("platform"), requested=driver)
            if str(driver or "") == "auto":
                request_payload = emit_request_payload.get("request") if isinstance(emit_request_payload, dict) else {}
                if effective_driver == "reddit_api":
                    token = (
                        emit_request_payload.get("oauth_access_token")
                        or os.environ.get("REDDIT_ACCESS_TOKEN")
                    )
                    if not token or not (request_payload or {}).get("thing_id"):
                        effective_driver = "manual_copy"
                elif effective_driver == "x_api":
                    token = (
                        emit_request_payload.get("oauth_access_token")
                        or os.environ.get("X_ACCESS_TOKEN")
                        or os.environ.get("TWITTER_BEARER_TOKEN")
                    )
                    if not token or not (request_payload or {}).get("in_reply_to_tweet_id"):
                        effective_driver = "manual_copy"

            emit_request = emit_request_payload
            control_overrides = self._load_dispatch_control_state(conn)
            request_safety = self._parse_dispatch_safety_policy(emit_request_payload)
            merged_safety = self._merge_control_overrides(request_safety, control_overrides.get("safety") or {})
            safety_policy = self._parse_dispatch_safety_policy({"safety": merged_safety})
            safety_decision = self._evaluate_dispatch_safety(
                safety_policy=safety_policy,
                platform=current_dict.get("platform"),
                driver=effective_driver,
            )
            safety_mode_state = self._safety_mode_state_payload(
                safety_policy=safety_policy,
                platform=current_dict.get("platform"),
            )
            override_source = str(safety_policy.get("override_source") or "none")

            observability_policy = self._parse_observability_policy(emit_request_payload)
            containment_controls = control_overrides.get("containment") if isinstance(control_overrides.get("containment"), dict) else {}
            base_retry_policy = (containment_controls.get("retry_policy") if isinstance(containment_controls, dict) else {}) or {}
            request_retry_policy = emit_request.get("retry_policy") if isinstance(emit_request, dict) else {}
            retry_policy = self._merge_control_overrides(base_retry_policy, request_retry_policy or {})
            max_attempts = max(1, int(retry_policy.get("max_attempts") or 3))
            base_delay_seconds = max(1.0, float(retry_policy.get("base_delay_seconds") or 30.0))
            backoff_multiplier = max(1.0, float(retry_policy.get("backoff_multiplier") or 2.0))
            max_delay_seconds = max(base_delay_seconds, float(retry_policy.get("max_delay_seconds") or 1800.0))
            previous_attempt_count = int(current_dict.get("attempt_count") or 0)
            attempt_count = previous_attempt_count
            outbox_status = current_status
            next_retry_at = current_dict.get("next_retry_at")

            request_breaker_raw = (emit_request_payload.get("circuit_breaker") if isinstance(emit_request_payload, dict) else {}) or {}
            base_breaker_raw = (containment_controls.get("circuit_breaker") if isinstance(containment_controls, dict) else {}) or {}
            merged_breaker_raw = self._merge_control_overrides(request_breaker_raw, base_breaker_raw)
            if "enabled" in merged_breaker_raw and "failure_threshold" not in merged_breaker_raw:
                merged_breaker_raw["failure_threshold"] = 1 if bool(merged_breaker_raw.get("enabled")) else 0
            breaker_policy = self._parse_circuit_breaker_policy({"circuit_breaker": merged_breaker_raw})
            breaker_scope_key = f"{str(current_dict.get('platform') or '')}:{effective_driver}"
            breaker_phase = "closed"
            breaker_state = "closed"
            breaker_reason_code = None
            now_ts = time.time()

            if not bool(safety_decision.get("allowed")):
                dispatch_status = "blocked"
                dispatch_response = {
                    "status": dispatch_status,
                    "attempt_count": attempt_count,
                    "max_attempts": max_attempts,
                    "retries_remaining": max(0, max_attempts - attempt_count),
                    "next_retry_at": next_retry_at,
                    "terminal_status": outbox_status,
                    "reason_code": "safety_blocked",
                    "block_reason": str(safety_decision.get("block_reason") or "safety_blocked"),
                }
            elif bool(breaker_policy.get("enabled")):
                breaker_preflight = self._dispatch_breaker_preflight(
                    conn,
                    scope_key=breaker_scope_key,
                    policy=breaker_policy,
                    now_ts=now_ts,
                )
                breaker_phase = str(breaker_preflight.get("phase") or "closed")
                breaker_state = str((breaker_preflight.get("state") or {}).get("state") or breaker_phase)
                if bool(breaker_preflight.get("blocked")):
                    dispatch_status = "blocked"
                    breaker_reason_code = str(breaker_preflight.get("reason_code") or "circuit_breaker_open")
                    dispatch_response = {
                        "status": dispatch_status,
                        "attempt_count": attempt_count,
                        "max_attempts": max_attempts,
                        "retries_remaining": max(0, max_attempts - attempt_count),
                        "next_retry_at": next_retry_at,
                        "terminal_status": outbox_status,
                        "reason_code": breaker_reason_code,
                        "block_reason": "circuit_breaker_open",
                        "breaker_state": breaker_state,
                        "breaker_scope": breaker_scope_key,
                    }
                else:
                    dispatch_response = self._dispatch_via_driver(
                        effective_driver,
                        emit_id=emit_id,
                        request=dispatch_request,
                        request_for_audit=dispatch_request_audit,
                        out_dir=out_dir,
                    )
                    dispatch_status = str(dispatch_response.get("status") or "dispatched")
            else:
                dispatch_response = self._dispatch_via_driver(
                    effective_driver,
                    emit_id=emit_id,
                    request=dispatch_request,
                    request_for_audit=dispatch_request_audit,
                    out_dir=out_dir,
                )
                dispatch_status = str(dispatch_response.get("status") or "dispatched")

            if dispatch_status == "blocked" and not dispatch_response.get("block_reason"):
                dispatch_response["block_reason"] = str(dispatch_response.get("reason_code") or "blocked")
            dispatch_response["safety_mode_state"] = safety_mode_state
            dispatch_response["override_source"] = override_source

            def _compute_next_retry(attempt: int) -> str | None:
                if attempt >= max_attempts:
                    return None
                raw_delay = base_delay_seconds * (backoff_multiplier ** max(0, attempt - 1))
                delay_seconds = min(max_delay_seconds, raw_delay)
                next_ts = datetime.now(timezone.utc).timestamp() + delay_seconds
                return datetime.fromtimestamp(next_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            if dispatch_status != "blocked":
                attempt_count = min(previous_attempt_count + 1, max_attempts)
                if dispatch_status == "failed":
                    next_retry_at = _compute_next_retry(attempt_count)
                    terminal_status = "dead_letter" if next_retry_at is None else "failed"
                    retries_remaining = max(0, max_attempts - attempt_count)
                    dispatch_response["attempt_count"] = attempt_count
                    dispatch_response["max_attempts"] = max_attempts
                    dispatch_response["retries_remaining"] = retries_remaining
                    dispatch_response["next_retry_at"] = next_retry_at
                    dispatch_response["terminal_status"] = terminal_status
                    dispatch_response["reason_code"] = (
                        "retry_exhausted_dead_letter" if terminal_status == "dead_letter" else "retry_scheduled"
                    )
                    dispatch_status = terminal_status
                    outbox_status = terminal_status
                else:
                    dispatch_response["attempt_count"] = attempt_count
                    dispatch_response["max_attempts"] = max_attempts
                    dispatch_response["retries_remaining"] = max(0, max_attempts - attempt_count)
                    dispatch_response["next_retry_at"] = None
                    dispatch_response["terminal_status"] = dispatch_status
                    dispatch_response["reason_code"] = "dispatch_completed"
                    outbox_status = "dispatched" if dispatch_status == "dispatched" else dispatch_status
                    next_retry_at = None

                if bool(breaker_policy.get("enabled")):
                    breaker_state_row = self._load_dispatch_circuit_breaker(conn, scope_key=breaker_scope_key)
                    breaker_update = self._dispatch_breaker_post_dispatch(
                        conn,
                        state=breaker_state_row,
                        policy=breaker_policy,
                        now_ts=now_ts,
                        dispatch_status=dispatch_status,
                        pre_phase=breaker_phase,
                    )
                    breaker_state = str(breaker_update.get("breaker_state") or breaker_state)
                    if breaker_update.get("reason_code"):
                        breaker_reason_code = str(breaker_update.get("reason_code"))
                    dispatch_response["breaker_state"] = breaker_state
                    dispatch_response["breaker_scope"] = breaker_scope_key
                    dispatch_response["breaker_failure_count_window"] = int(breaker_update.get("failure_count_window") or 0)
                    if breaker_reason_code:
                        dispatch_response["breaker_reason_code"] = breaker_reason_code
            else:
                if breaker_reason_code:
                    dispatch_response["breaker_reason_code"] = breaker_reason_code

            observability_snapshot = self._dispatch_observability_snapshot(
                conn,
                now_ts=now_ts,
                current_status=dispatch_status,
                policy=observability_policy,
            )
            observability_alerts = self._dispatch_observability_alerts(
                snapshot=observability_snapshot,
                now_ts=now_ts,
            )
            dispatch_response["observability_snapshot"] = observability_snapshot
            dispatch_response["observability_alerts"] = observability_alerts

            conn.execute(
                """
                INSERT INTO emit_dispatches (
                    emit_outbox_id, run_id, driver, status, request_json, response_json, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    emit_id,
                    int(current_dict["run_id"]),
                    effective_driver,
                    dispatch_status,
                    to_json(dispatch_request_audit),
                    to_json(dispatch_response),
                    notes,
                ),
            )
            conn.execute(
                "UPDATE emit_outbox SET status = ?, attempt_count = ?, next_retry_at = ?, retry_policy_json = ? WHERE id = ?",
                (outbox_status, attempt_count, next_retry_at, to_json(retry_policy), emit_id),
            )
            run_id = int(current_dict["run_id"])
        run = self.get_run(run_id)
        dispatch = next(
            (item for item in run.get("emit_dispatches") or [] if int(item.get("emit_outbox_id") or 0) == emit_id),
            None,
        )
        emit = next((item for item in run.get("emit_outbox") or [] if int(item.get("id") or 0) == emit_id), None)
        return {"run": run, "emit": emit, "dispatch": dispatch}

    def dispatch_approved(
        self,
        *,
        limit: int = 25,
        driver: str = "auto",
        out_dir: str | Path | None = None,
        notes: str | None = None,
        include_retry_due: bool = False,
    ) -> dict[str, Any]:
        approved = self.list_emit_outbox(limit=limit, status="approved")

        due_failed: list[dict[str, Any]] = []
        if include_retry_due:
            with open_db(self.path) as conn:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM emit_outbox
                    WHERE status = 'failed'
                    ORDER BY COALESCE(next_retry_at, created_at) ASC, id ASC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            now = time.time()
            due_failed = [
                self._decode_emit_outbox(dict(row))
                for row in rows
                if self._is_due(dict(row).get("next_retry_at"), now=now)
            ]

        queue: list[dict[str, Any]] = []
        seen_ids: set[int] = set()
        for item in approved + due_failed:
            emit_id = int(item.get("id") or 0)
            if emit_id and emit_id not in seen_ids:
                queue.append(item)
                seen_ids.add(emit_id)
            if len(queue) >= limit:
                break

        dispatched: list[dict[str, Any]] = []
        for item in queue:
            result = self.dispatch_emit(
                int(item["id"]),
                driver=driver,
                out_dir=out_dir,
                notes=notes,
                force=str(item.get("status") or "") == "failed",
            )
            dispatched.append(
                {
                    "emit": result.get("emit"),
                    "dispatch": result.get("dispatch"),
                    "run_id": result.get("run", {}).get("id") if isinstance(result.get("run"), dict) else None,
                }
            )
        return {
            "requested_limit": limit,
            "approved_found": len(approved),
            "retry_due_found": len(due_failed),
            "queued_count": len(queue),
            "dispatched_count": len(dispatched),
            "dispatched": dispatched,
        }

    def update_emit_dispatch(
        self,
        dispatch_id: int,
        *,
        status: str,
        notes: str | None = None,
        notes_mode: str = "append",
    ) -> dict[str, Any]:
        allowed_statuses = {"dispatched", "acknowledged", "delivered", "failed", "dead_letter", "blocked"}
        next_status = str(status)
        if next_status not in allowed_statuses:
            raise ValueError(f"unknown dispatch status: {next_status}")
        with open_db(self.path) as conn:
            current = conn.execute("SELECT * FROM emit_dispatches WHERE id = ?", (dispatch_id,)).fetchone()
            if not current:
                raise KeyError(f"emit_dispatch {dispatch_id} not found")
            current_dict = self._decode_emit_dispatch(dict(current))
            merged_notes = current_dict.get("notes")
            if notes is not None:
                if notes_mode == "replace":
                    merged_notes = notes
                elif notes_mode == "append":
                    merged_notes = f"{merged_notes}\n{notes}" if merged_notes and notes else (notes or merged_notes)
                else:
                    raise ValueError(f"unknown notes_mode: {notes_mode}")
            response = dict(current_dict.get("response") or {})
            response["status"] = next_status
            conn.execute(
                "UPDATE emit_dispatches SET status = ?, response_json = ?, notes = ? WHERE id = ?",
                (next_status, to_json(response), merged_notes, dispatch_id),
            )
            conn.execute(
                "UPDATE emit_outbox SET status = ? WHERE id = ?",
                (next_status, int(current_dict["emit_outbox_id"])),
            )
            run_id = int(current_dict["run_id"])
        run = self.get_run(run_id)
        dispatch = next((item for item in run.get("emit_dispatches") or [] if int(item.get("id") or 0) == dispatch_id), None)
        emit = next((item for item in run.get("emit_outbox") or [] if int(item.get("id") or 0) == int((dispatch or {}).get("emit_outbox_id") or 0)), None)
        return {"run": run, "emit": emit, "dispatch": dispatch}

    def redrive_dispatch(
        self,
        dispatch_id: int,
        *,
        driver: str | None = None,
        out_dir: str | Path | None = None,
        notes: str | None = None,
    ) -> dict[str, Any]:
        with open_db(self.path) as conn:
            current = conn.execute("SELECT * FROM emit_dispatches WHERE id = ?", (dispatch_id,)).fetchone()
            if not current:
                raise KeyError(f"emit_dispatch {dispatch_id} not found")
            current_dict = self._decode_emit_dispatch(dict(current))
            emit_row = conn.execute("SELECT status FROM emit_outbox WHERE id = ?", (int(current_dict["emit_outbox_id"]),)).fetchone()
            latest_row = conn.execute(
                "SELECT id, status FROM emit_dispatches WHERE emit_outbox_id = ? ORDER BY id DESC LIMIT 1",
                (int(current_dict["emit_outbox_id"]),),
            ).fetchone()
        if str(current_dict.get("status") or "") != "failed":
            raise ValueError(f"emit_dispatch {dispatch_id} must be failed before redrive")
        if not emit_row:
            raise KeyError(f"emit_outbox {current_dict['emit_outbox_id']} not found")
        if str(emit_row["status"] or "") != "failed":
            raise ValueError(f"emit_outbox {current_dict['emit_outbox_id']} must be failed before redrive")
        if not latest_row or int(latest_row["id"]) != int(dispatch_id):
            raise ValueError(f"emit_dispatch {dispatch_id} is not the latest dispatch for emit_outbox {current_dict['emit_outbox_id']}")
        merged_notes = f"redrive of dispatch #{dispatch_id}"
        if notes:
            merged_notes = f"{merged_notes}\n{notes}"
        result = self.dispatch_emit(
            int(current_dict["emit_outbox_id"]),
            driver=driver or str(current_dict.get("driver") or "manual_copy"),
            out_dir=out_dir,
            notes=merged_notes,
            force=True,
        )
        return {"previous_dispatch": current_dict, **result}

    def list_redrivable_failed_dispatches(self, limit: int = 25, min_failed_age_seconds: float = 0.0) -> list[dict[str, Any]]:
        with open_db(self.path) as conn:
            rows = conn.execute(
                """
                SELECT d.*, o.next_retry_at
                FROM emit_dispatches d
                JOIN emit_outbox o ON o.id = d.emit_outbox_id
                JOIN (
                    SELECT emit_outbox_id, MAX(id) AS latest_id
                    FROM emit_dispatches
                    GROUP BY emit_outbox_id
                ) latest ON latest.latest_id = d.id
                WHERE d.status = 'failed' AND o.status = 'failed'
                ORDER BY d.id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        now = time.time()
        redrivable: list[dict[str, Any]] = []
        for row in rows:
            item = self._decode_emit_dispatch(dict(row))
            failed_age_seconds = self._age_seconds(item.get("created_at"), now=now)
            next_retry_at = item.get("next_retry_at")
            retry_window_open = self._is_due(next_retry_at, now=now)
            item["failed_age_seconds"] = round(failed_age_seconds, 4) if failed_age_seconds is not None else None
            item["next_retry_at"] = next_retry_at
            item["retry_window_open"] = retry_window_open
            if not retry_window_open:
                continue
            if failed_age_seconds is None or failed_age_seconds >= max(0.0, float(min_failed_age_seconds)):
                redrivable.append(item)
        return redrivable

    def worker_cycle(
        self,
        *,
        dispatch_limit: int = 25,
        driver: str = "auto",
        out_dir: str | Path | None = None,
        notes: str | None = None,
        include_failed_redrive: bool = False,
        redrive_limit: int = 10,
        min_failed_age_seconds: float = 0.0,
        max_actions_per_hour: int = 0,
        max_actions_per_day: int = 0,
        min_seconds_between_actions: float = 0.0,
        quiet_hours_start: int | None = None,
        quiet_hours_end: int | None = None,
    ) -> dict[str, Any]:
        started_at = time.time()
        with open_db(self.path) as conn:
            controls = self._effective_dispatch_controls(conn)
        governor_config = controls.get("governor") if isinstance(controls.get("governor"), dict) else {}
        effective_max_actions_per_hour = int(governor_config.get("max_actions_per_hour") if governor_config.get("max_actions_per_hour") is not None else max_actions_per_hour)
        effective_max_actions_per_day = int(governor_config.get("max_actions_per_day") if governor_config.get("max_actions_per_day") is not None else max_actions_per_day)
        effective_min_seconds_between_actions = float(governor_config.get("min_seconds_between_actions") if governor_config.get("min_seconds_between_actions") is not None else min_seconds_between_actions)
        effective_quiet_hours_start = governor_config.get("quiet_hours_start") if governor_config.get("quiet_hours_start") is not None else quiet_hours_start
        effective_quiet_hours_end = governor_config.get("quiet_hours_end") if governor_config.get("quiet_hours_end") is not None else quiet_hours_end

        stats = self._action_window_stats(now=started_at)
        governor = self.can_execute_action(
            now_utc=datetime.fromtimestamp(started_at, timezone.utc),
            actions_last_hour=int(stats.get("actions_last_hour") or 0),
            actions_last_day=int(stats.get("actions_last_day") or 0),
            seconds_since_last_action=stats.get("seconds_since_last_action"),
            max_actions_per_hour=effective_max_actions_per_hour,
            max_actions_per_day=effective_max_actions_per_day,
            min_seconds_between_actions=effective_min_seconds_between_actions,
            quiet_hours_start=effective_quiet_hours_start,
            quiet_hours_end=effective_quiet_hours_end,
        )
        if not bool(governor.get("allow")):
            logger.info("worker_cycle: governor blocked execution reason=%s", governor.get("reason"))
            return {
                "dispatch_limit": dispatch_limit,
                "driver": driver,
                "out_dir": str(Path(out_dir).expanduser().resolve()) if out_dir else str(self.default_dispatch_dir()),
                "include_failed_redrive": include_failed_redrive,
                "redrive_limit": redrive_limit,
                "min_failed_age_seconds": round(max(0.0, float(min_failed_age_seconds)), 4),
                "approved_found": 0,
                "dispatched_count": 0,
                "dispatched": [],
                "failed_found": 0,
                "redriven_count": 0,
                "redriven": [],
                "governor": {
                    **governor,
                    "stats": stats,
                    "config": {
                        "max_actions_per_hour": max(0, int(effective_max_actions_per_hour)),
                        "max_actions_per_day": max(0, int(effective_max_actions_per_day)),
                        "min_seconds_between_actions": round(max(0.0, float(effective_min_seconds_between_actions)), 4),
                        "quiet_hours_start": effective_quiet_hours_start,
                        "quiet_hours_end": effective_quiet_hours_end,
                    },
                },
                "duration_seconds": round(time.time() - started_at, 4),
            }

        batch = self.dispatch_approved(limit=dispatch_limit, driver=driver, out_dir=out_dir, notes=notes, include_retry_due=False)
        redriven: list[dict[str, Any]] = []
        failed = self.list_redrivable_failed_dispatches(limit=redrive_limit, min_failed_age_seconds=min_failed_age_seconds) if include_failed_redrive else []
        for item in failed:
            result = self.redrive_dispatch(
                int(item["id"]),
                driver=driver,
                out_dir=out_dir,
                notes=notes,
            )
            previous_dispatch = dict(result.get("previous_dispatch") or {})
            if item.get("failed_age_seconds") is not None:
                previous_dispatch["failed_age_seconds"] = item.get("failed_age_seconds")
            redriven.append(
                {
                    "previous_dispatch": previous_dispatch,
                    "emit": result.get("emit"),
                    "dispatch": result.get("dispatch"),
                    "run_id": result.get("run", {}).get("id") if isinstance(result.get("run"), dict) else None,
                }
            )
        logger.info(
            "worker_cycle: done dispatched=%d redriven=%d approved_found=%d failed_found=%d",
            batch["dispatched_count"],
            len(redriven),
            batch["approved_found"],
            len(failed),
        )
        return {
            "dispatch_limit": dispatch_limit,
            "driver": driver,
            "out_dir": str(Path(out_dir).expanduser().resolve()) if out_dir else str(self.default_dispatch_dir()),
            "include_failed_redrive": include_failed_redrive,
            "redrive_limit": redrive_limit,
            "min_failed_age_seconds": round(max(0.0, float(min_failed_age_seconds)), 4),
            "approved_found": batch["approved_found"],
            "dispatched_count": batch["dispatched_count"],
            "dispatched": batch["dispatched"],
            "failed_found": len(failed),
            "redriven_count": len(redriven),
            "redriven": redriven,
            "governor": {
                **governor,
                "stats": stats,
                "config": {
                    "max_actions_per_hour": max(0, int(effective_max_actions_per_hour)),
                    "max_actions_per_day": max(0, int(effective_max_actions_per_day)),
                    "min_seconds_between_actions": round(max(0.0, float(effective_min_seconds_between_actions)), 4),
                    "quiet_hours_start": effective_quiet_hours_start,
                    "quiet_hours_end": effective_quiet_hours_end,
                },
            },
            "duration_seconds": round(time.time() - started_at, 4),
        }

    @classmethod
    def supported_dispatch_drivers(cls) -> tuple[str, ...]:
        return ("manual_copy", "jsonl_append", "webhook_post", "reddit_api", "x_api")

    @classmethod
    def driver_requirements(cls, driver: str) -> dict[str, Any]:
        """A registry of what each driver needs in the emit request."""
        if driver == "webhook_post":
            return {
                "required": ["webhook_url"],
                "description": "POSTs JSON payload to an external HTTP endpoint. Status 'delivered' on 2xx response.",
                "example": '{"webhook_url": "https://hooks.example.com/..."}'
            }
        if driver == "reddit_api":
            return {
                "required": ["thing_id", "text", "oauth_access_token"],
                "description": "Posts a Reddit comment reply via OAuth API. Uses emit_request.request.thing_id + text.",
                "example": '{"request": {"thing_id": "t1_abc123", "text": "Reply text"}, "oauth_access_token": "..."}'
            }
        if driver == "x_api":
            return {
                "required": ["in_reply_to_tweet_id", "text", "oauth_access_token"],
                "description": "Posts an X reply via v2 API. Uses emit_request.request.in_reply_to_tweet_id + text.",
                "example": '{"request": {"in_reply_to_tweet_id": "123", "text": "Reply text"}, "oauth_access_token": "..."}'
            }
        if driver == "jsonl_append":
            return {
                "required": [],
                "description": "Appends a single JSON line to dispatches.jsonl in the out_dir. Instant 'delivered' status.",
                "example": "{}"
            }
        if driver == "manual_copy":
            return {
                "required": [],
                "description": "Writes a standalone JSON sidecar file for manual transport. Initial status is 'dispatched'.",
                "example": "{}"
            }
        return {"required": [], "description": "Unknown driver.", "example": "{}"}

    def get_execution_metrics(self, days: int = 7) -> dict[str, Any]:
        """High-fidelity stats for Phase 7 cockpit."""
        sql = """
            SELECT 
                status,
                COUNT(*) as count
            FROM emit_dispatches
            WHERE created_at > datetime('now', ?)
            GROUP BY status
        """
        with open_db(self.path) as conn:
            rows = conn.execute(sql, (f"-{days} days",)).fetchall()
            stats = {row["status"]: row["count"] for row in rows}
            
            total = sum(stats.values())
            success_rate = round((stats.get("delivered", 0) / total * 100), 1) if total > 0 else 0
            
            return {
                "total_dispatches": total,
                "status_breakdown": stats,
                "success_rate_percent": success_rate,
                "failed_count": stats.get("failed", 0),
                "delivered_count": stats.get("delivered", 0),
            }

    def get_persona_reputation(self, persona: str, platform: str, days: int = 30) -> dict[str, Any]:
        """Aggregate rhetoric performance for a specific persona-platform pair."""
        sql = """
            SELECT 
                COUNT(*) as total_runs,
                SUM(CASE WHEN o.got_reply = 1 THEN 1 ELSE 0 END) as reply_count,
                AVG(CASE WHEN o.got_reply = 1 THEN o.reply_delay_seconds ELSE NULL END) as avg_reply_delay,
                AVG(o.spectator_engagement) as avg_engagement,
                SUM(CASE WHEN o.emit_dispatch_id IS NOT NULL THEN 1 ELSE 0 END) as linked_outcomes,
                SUM(CASE WHEN d.status IN ('acknowledged', 'delivered') THEN 1 ELSE 0 END) as delivery_verified_count
            FROM runs r
            JOIN outcomes o ON r.id = o.run_id
            LEFT JOIN emit_dispatches d ON d.id = o.emit_dispatch_id
            WHERE r.persona = ? AND r.platform = ? AND r.created_at > datetime('now', ?)
        """
        sql_tactics = """
            SELECT 
                r.selected_tactic as tactic,
                COUNT(*) as count,
                SUM(CASE WHEN o.got_reply = 1 THEN 1 ELSE 0 END) as replies,
                AVG(o.spectator_engagement) as avg_engagement,
                AVG(CASE WHEN o.got_reply = 1 THEN o.reply_delay_seconds ELSE NULL END) as avg_reply_delay,
                SUM(CASE WHEN o.emit_dispatch_id IS NOT NULL THEN 1 ELSE 0 END) as linked_outcomes,
                SUM(CASE WHEN d.status IN ('acknowledged', 'delivered') THEN 1 ELSE 0 END) as delivery_verified_count
            FROM runs r
            JOIN outcomes o ON r.id = o.run_id
            LEFT JOIN emit_dispatches d ON d.id = o.emit_dispatch_id
            WHERE r.persona = ? AND r.platform = ? AND r.created_at > datetime('now', ?)
            GROUP BY r.selected_tactic
        """
        with open_db(self.path) as conn:
            summary = conn.execute(sql, (persona, platform, f"-{days} days")).fetchone()
            tactics = conn.execute(sql_tactics, (persona, platform, f"-{days} days")).fetchall()
            
            tactic_scores = {
                row["tactic"]: {
                    "count": row["count"],
                    "replies": row["replies"],
                    "rate": round(row["replies"] / row["count"], 2) if row["count"] > 0 else 0,
                    "avg_engagement": round(row["avg_engagement"], 2) if row["avg_engagement"] is not None else None,
                    "avg_reply_delay": round(row["avg_reply_delay"], 2) if row["avg_reply_delay"] is not None else None,
                    "linked_outcomes": row["linked_outcomes"] or 0,
                    "delivery_verified_count": row["delivery_verified_count"] or 0,
                    "delivery_confidence": round((row["delivery_verified_count"] or 0) / (row["linked_outcomes"] or 1), 2)
                    if (row["linked_outcomes"] or 0) > 0
                    else 0,
                }
                for row in tactics if row["tactic"]
            }
            
            total_runs = summary["total_runs"] or 0
            reply_count = summary["reply_count"] or 0
            linked_outcomes = summary["linked_outcomes"] or 0
            delivery_verified_count = summary["delivery_verified_count"] or 0

            return {
                "persona": persona,
                "platform": platform,
                "days": days,
                "total_runs": total_runs,
                "reply_count": reply_count,
                "reply_rate": round(reply_count / total_runs, 2) if total_runs > 0 else 0,
                "avg_reply_delay": round(summary["avg_reply_delay"], 1) if summary["avg_reply_delay"] else None,
                "avg_engagement": round(summary["avg_engagement"], 1) if summary["avg_engagement"] else None,
                "linked_outcomes": linked_outcomes,
                "delivery_verified_count": delivery_verified_count,
                "delivery_confidence": round(delivery_verified_count / linked_outcomes, 2) if linked_outcomes > 0 else 0,
                "tactic_performance": tactic_scores
            }

    @staticmethod
    def _variant_hash(text: str) -> str:
        return hashlib.sha256(text.strip().lower().encode("utf-8")).hexdigest()

    def create_mutation_family(self, record: MutationFamilyRecord) -> dict[str, Any]:
        with open_db(self.path) as conn:
            cur = conn.execute(
                """
                INSERT INTO mutation_families (
                    run_id,
                    winner_candidate_id,
                    winner_rank_index,
                    persona,
                    platform,
                    tactic,
                    objective,
                    winner_score,
                    source,
                    strategy,
                    notes,
                    lineage_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id, winner_candidate_id, strategy) DO UPDATE SET
                    winner_rank_index=excluded.winner_rank_index,
                    persona=excluded.persona,
                    platform=excluded.platform,
                    tactic=excluded.tactic,
                    objective=excluded.objective,
                    winner_score=excluded.winner_score,
                    source=excluded.source,
                    notes=excluded.notes,
                    lineage_json=excluded.lineage_json
                """,
                (
                    record.run_id,
                    record.winner_candidate_id,
                    record.winner_rank_index,
                    record.persona,
                    record.platform,
                    record.tactic,
                    record.objective,
                    record.winner_score,
                    record.source,
                    record.strategy,
                    record.notes,
                    record.lineage_json,
                ),
            )
            family_id = int(cur.lastrowid)
            if family_id == 0:
                row = conn.execute(
                    """
                    SELECT id
                    FROM mutation_families
                    WHERE run_id = ?
                      AND ((winner_candidate_id = ?) OR (winner_candidate_id IS NULL AND ? IS NULL))
                      AND strategy = ?
                    """,
                    (record.run_id, record.winner_candidate_id, record.winner_candidate_id, record.strategy),
                ).fetchone()
                if not row:
                    raise KeyError("mutation family upsert failed")
                family_id = int(row["id"])
        return self.get_mutation_family(family_id)

    def get_mutation_family(self, family_id: int) -> dict[str, Any]:
        with open_db(self.path) as conn:
            row = conn.execute("SELECT * FROM mutation_families WHERE id = ?", (family_id,)).fetchone()
        if not row:
            raise KeyError(f"mutation_family {family_id} not found")
        family = dict(row)
        family["lineage"] = json.loads(family.pop("lineage_json"))
        return family

    def list_mutation_variants(
        self,
        *,
        family_id: int | None = None,
        run_id: int | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if family_id is not None:
            clauses.append("mv.family_id = ?")
            params.append(family_id)
        if run_id is not None:
            clauses.append("mv.run_id = ?")
            params.append(run_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"""
            SELECT
                mv.*,
                mf.persona AS family_persona,
                mf.platform AS family_platform,
                mf.tactic AS family_tactic,
                mf.objective AS family_objective,
                mf.winner_score AS family_winner_score
            FROM mutation_variants mv
            JOIN mutation_families mf ON mf.id = mv.family_id
            {where}
            ORDER BY mv.created_at DESC, mv.id DESC
            LIMIT ?
        """
        params.append(limit)
        with open_db(self.path) as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
        variants: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["score"] = json.loads(item.pop("score_json"))
            item["lineage"] = json.loads(item.pop("lineage_json"))
            variants.append(item)
        return variants

    def create_mutation_variants(self, variants: list[MutationVariantRecord]) -> list[dict[str, Any]]:
        if not variants:
            return []
        with open_db(self.path) as conn:
            for record in variants:
                conn.execute(
                    """
                    INSERT INTO mutation_variants (
                        family_id,
                        run_id,
                        parent_candidate_id,
                        transform,
                        variant_text,
                        variant_hash,
                        status,
                        score_json,
                        lineage_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(family_id, variant_hash) DO UPDATE SET
                        transform=excluded.transform,
                        variant_text=excluded.variant_text,
                        status=excluded.status,
                        score_json=excluded.score_json,
                        lineage_json=excluded.lineage_json
                    """,
                    (
                        record.family_id,
                        record.run_id,
                        record.parent_candidate_id,
                        record.transform,
                        record.variant_text,
                        record.variant_hash,
                        record.status,
                        record.score_json,
                        record.lineage_json,
                    ),
                )
        family_ids = {record.family_id for record in variants}
        out: list[dict[str, Any]] = []
        for item_family_id in sorted(family_ids):
            out.extend(self.list_mutation_variants(family_id=item_family_id, limit=250))
        return out

    def _transform_policy_for_winner(
        self,
        *,
        persona: str,
        platform: str,
        tactic: str | None,
        objective: str | None,
        days: int = 60,
        min_samples: int = 2,
    ) -> list[str]:
        clauses = ["mf.persona = ?", "mf.platform = ?", "mv.created_at > datetime('now', ?)"]
        params: list[Any] = [persona, platform, f"-{int(max(days, 1))} days"]
        if tactic:
            clauses.append("mf.tactic = ?")
            params.append(tactic)
        if objective:
            clauses.append("mf.objective = ?")
            params.append(objective)

        sql = f"""
            SELECT mv.transform, mv.status, mf.winner_score
            FROM mutation_variants mv
            JOIN mutation_families mf ON mf.id = mv.family_id
            WHERE {' AND '.join(clauses)}
        """
        with open_db(self.path) as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()

        if not rows:
            return []

        stats: dict[str, dict[str, float]] = {}
        for row in rows:
            transform = str(row["transform"] or "").strip().lower()
            if not transform:
                continue
            bucket = stats.setdefault(transform, {"count": 0.0, "score": 0.0})
            bucket["count"] += 1.0
            status = str(row["status"] or "drafted").strip().lower()
            status_weight = {
                "replied": 1.0,
                "delivered": 0.8,
                "promoted": 0.7,
                "approved": 0.6,
                "selected": 0.5,
                "drafted": 0.25,
                "rejected": -0.25,
                "failed": -0.4,
                "dead": -0.5,
            }.get(status, 0.0)
            winner_score = float(row["winner_score"] or 0.0)
            bucket["score"] += (status_weight * 0.7) + (winner_score * 0.3)

        ranked: list[tuple[str, float, float]] = []
        for transform, bucket in stats.items():
            count = float(bucket["count"])
            if count < float(min_samples):
                continue
            avg = float(bucket["score"]) / max(count, 1.0)
            confidence = min(1.0, count / 8.0)
            ranked.append((transform, avg, confidence))

        ranked.sort(key=lambda item: (item[1] * item[2], item[1], item[2]), reverse=True)
        return [item[0] for item in ranked]

    def _mutate_winner_record(
        self,
        winner: dict[str, Any],
        *,
        variants_per_winner: int,
        strategy: str,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        transform_policy = self._transform_policy_for_winner(
            persona=str(winner.get("persona") or ""),
            platform=str(winner.get("platform") or ""),
            tactic=str(winner.get("candidate_tactic") or "") or None,
            objective=str(winner.get("candidate_objective") or "") or None,
        )

        family = self.create_mutation_family(
            MutationFamilyRecord(
                id=None,
                run_id=int(winner.get("run_id") or 0),
                winner_candidate_id=int(winner.get("candidate_id") or 0) if winner.get("candidate_id") is not None else None,
                winner_rank_index=int(winner.get("candidate_rank_index") or 0) if winner.get("candidate_rank_index") is not None else None,
                persona=str(winner.get("persona") or ""),
                platform=str(winner.get("platform") or ""),
                tactic=str(winner.get("candidate_tactic") or "") or None,
                objective=str(winner.get("candidate_objective") or "") or None,
                winner_score=float(winner.get("winner_score") or 0.0),
                source=str(winner.get("winner_source") or "") or None,
                strategy=str(strategy),
                notes=None,
                lineage_json=to_json(
                    {
                        "winner": {
                            "run_id": winner.get("run_id"),
                            "candidate_id": winner.get("candidate_id"),
                            "candidate_rank_index": winner.get("candidate_rank_index"),
                            "winner_source": winner.get("winner_source"),
                            "winner_score": winner.get("winner_score"),
                            "delivery_status": winner.get("delivery_status"),
                        },
                        "outcome": winner.get("outcome") or {},
                        "tactic_history": winner.get("tactic_history") or {},
                        "reputation": winner.get("reputation") or {},
                        "transform_policy": transform_policy,
                    }
                ),
            )
        )
        raw_variants = generate_controlled_variants(
            winner,
            max_variants=variants_per_winner,
            transform_policy=transform_policy,
        )
        saved = self.create_mutation_variants(
            [
                MutationVariantRecord(
                    id=None,
                    family_id=int(family["id"]),
                    run_id=int(winner.get("run_id") or 0),
                    parent_candidate_id=int(winner.get("candidate_id") or 0) if winner.get("candidate_id") is not None else None,
                    transform=str(item.get("transform") or "unknown"),
                    variant_text=str(item.get("text") or "").strip(),
                    variant_hash=self._variant_hash(str(item.get("text") or "")),
                    status="drafted",
                    score_json=to_json(
                        {
                            "seed_winner_score": winner.get("winner_score"),
                            "seed_rank_score": winner.get("candidate_rank_score"),
                        }
                    ),
                    lineage_json=to_json(item.get("lineage") or {}),
                )
                for item in raw_variants
                if str(item.get("text") or "").strip()
            ]
        )
        return family, saved

    def mutate_run(
        self,
        run_id: int,
        *,
        variants_per_winner: int = 5,
        strategy: str = "controlled_v1",
    ) -> dict[str, Any]:
        run = self.get_run(run_id)
        resolved = self._resolve_winner_candidate(run)
        candidate = resolved.get("candidate") if isinstance(resolved.get("candidate"), dict) else None
        if not candidate:
            raise ValueError(f"run {run_id} has no candidate to mutate")

        outcome = run.get("outcome") if isinstance(run.get("outcome"), dict) else {}
        dispatch = resolved.get("dispatch") if isinstance(resolved.get("dispatch"), dict) else {}
        winner_score = self._score_winner_candidate(candidate, outcome, dispatch, {}, {})
        winner = {
            "run_id": int(run.get("id") or run_id),
            "platform": run.get("platform"),
            "persona": run.get("persona"),
            "winner_source": resolved.get("source") or "run",
            "selection_strategy": resolved.get("selection_strategy"),
            "selection_preset": resolved.get("selection_preset"),
            "candidate_id": candidate.get("id"),
            "candidate_rank_index": candidate.get("rank_index"),
            "candidate_text": candidate.get("text"),
            "candidate_tactic": candidate.get("tactic"),
            "candidate_objective": candidate.get("objective"),
            "candidate_rank_score": candidate.get("rank_score"),
            "estimated_bite_score": candidate.get("estimated_bite_score"),
            "estimated_audience_score": candidate.get("estimated_audience_score"),
            "critic_penalty": candidate.get("critic_penalty"),
            "delivery_status": dispatch.get("status"),
            "winner_score": winner_score,
            "outcome": outcome or {},
            "candidate": candidate,
            "dispatch": dispatch,
            "reputation": {},
            "tactic_history": {},
        }
        family, variants = self._mutate_winner_record(
            winner,
            variants_per_winner=variants_per_winner,
            strategy=strategy,
        )
        return {
            "ok": True,
            "strategy": strategy,
            "run_id": run_id,
            "winner": winner,
            "family": family,
            "variant_count": len(variants),
            "variants": variants,
        }

    def mutation_report(
        self,
        *,
        limit: int = 250,
        persona: str | None = None,
        platform: str | None = None,
        tactic: str | None = None,
        objective: str | None = None,
        status: str | None = None,
    ) -> dict[str, Any]:
        clauses: list[str] = []
        params: list[Any] = []
        if persona is not None:
            clauses.append("mf.persona = ?")
            params.append(persona)
        if platform is not None:
            clauses.append("mf.platform = ?")
            params.append(platform)
        if tactic is not None:
            clauses.append("mf.tactic = ?")
            params.append(tactic)
        if objective is not None:
            clauses.append("mf.objective = ?")
            params.append(objective)
        if status is not None:
            clauses.append("mv.status = ?")
            params.append(status)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"""
            SELECT
                mv.id,
                mv.family_id,
                mv.run_id,
                mv.parent_candidate_id,
                mv.transform,
                mv.variant_text,
                mv.variant_hash,
                mv.status,
                mv.score_json,
                mv.lineage_json,
                mv.created_at,
                mf.persona AS family_persona,
                mf.platform AS family_platform,
                mf.tactic AS family_tactic,
                mf.objective AS family_objective,
                mf.winner_score AS family_winner_score
            FROM mutation_variants mv
            JOIN mutation_families mf ON mf.id = mv.family_id
            {where}
            ORDER BY mv.created_at DESC, mv.id DESC
            LIMIT ?
        """
        params.append(limit)
        with open_db(self.path) as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()

        variants: list[dict[str, Any]] = []
        by_transform: dict[str, int] = {}
        by_status: dict[str, int] = {}
        for row in rows:
            item = dict(row)
            item["score"] = json.loads(item.pop("score_json"))
            item["lineage"] = json.loads(item.pop("lineage_json"))
            variants.append(item)
            transform = str(item.get("transform") or "unknown")
            by_transform[transform] = by_transform.get(transform, 0) + 1
            status_key = str(item.get("status") or "unknown")
            by_status[status_key] = by_status.get(status_key, 0) + 1

        return {
            "ok": True,
            "limit": limit,
            "filters": {
                "persona": persona,
                "platform": platform,
                "tactic": tactic,
                "objective": objective,
                "status": status,
            },
            "total": len(variants),
            "by_transform": by_transform,
            "by_status": by_status,
            "variants": variants,
        }

    def mutate_top_winners(
        self,
        *,
        winner_limit: int = 5,
        variants_per_winner: int = 5,
        persona: str | None = None,
        platform: str | None = None,
        tactic: str | None = None,
        objective: str | None = None,
        days: int = 30,
        require_reply: bool = True,
        strategy: str = "controlled_v1",
    ) -> dict[str, Any]:
        winners = self.extract_top_winners(
            limit=winner_limit,
            persona=persona,
            platform=platform,
            tactic=tactic,
            objective=objective,
            days=days,
            require_reply=require_reply,
        )

        created_families: list[dict[str, Any]] = []
        created_variants: list[dict[str, Any]] = []
        for winner in winners:
            family, saved = self._mutate_winner_record(
                winner,
                variants_per_winner=variants_per_winner,
                strategy=strategy,
            )
            created_families.append(family)
            created_variants.extend(saved)

        return {
            "ok": True,
            "strategy": strategy,
            "winner_count": len(winners),
            "family_count": len(created_families),
            "variant_count": len(created_variants),
            "winners": winners,
            "families": created_families,
            "variants": created_variants,
        }

    def extract_top_winners(
        self,
        limit: int = 25,
        *,
        persona: str | None = None,
        platform: str | None = None,
        tactic: str | None = None,
        objective: str | None = None,
        days: int = 30,
        require_reply: bool = True,
    ) -> list[dict[str, Any]]:
        """Resolve the strongest historically-linked candidate winners for mutation seeding."""
        clauses = ["r.created_at > datetime('now', ?)"]
        params: list[Any] = [f"-{days} days"]
        if persona is not None:
            clauses.append("r.persona = ?")
            params.append(persona)
        if platform is not None:
            clauses.append("r.platform = ?")
            params.append(platform)
        if require_reply:
            clauses.append("o.got_reply = 1")

        sql = f"""
            SELECT r.id
            FROM runs r
            JOIN outcomes o ON o.run_id = r.id
            WHERE {' AND '.join(clauses)}
            ORDER BY r.created_at DESC, r.id DESC
        """
        with open_db(self.path) as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()

        reputation_cache: dict[tuple[str, str], dict[str, Any]] = {}
        winners: list[dict[str, Any]] = []
        for row in rows:
            run = self.get_run(int(row["id"]))
            outcome = run.get("outcome")
            if not outcome:
                continue
            resolved = self._resolve_winner_candidate(run)
            candidate = resolved.get("candidate")
            if not isinstance(candidate, dict):
                continue
            if tactic is not None and candidate.get("tactic") != tactic:
                continue
            if objective is not None and candidate.get("objective") != objective:
                continue

            persona_key = (str(run.get("persona") or ""), str(run.get("platform") or ""))
            if persona_key not in reputation_cache:
                reputation_cache[persona_key] = self.get_persona_reputation(persona_key[0], persona_key[1], days=days)
            reputation = reputation_cache[persona_key]
            tactic_history = {}
            tactic_performance = reputation.get("tactic_performance") or {}
            if isinstance(tactic_performance, dict) and candidate.get("tactic") in tactic_performance:
                tactic_history = tactic_performance.get(candidate.get("tactic")) or {}

            dispatch = resolved.get("dispatch") if isinstance(resolved.get("dispatch"), dict) else None
            emit = resolved.get("emit") if isinstance(resolved.get("emit"), dict) else None
            winner_score = self._score_winner_candidate(candidate, outcome, dispatch, tactic_history, reputation)
            delivery_status = str((dispatch or {}).get("status") or "") or None
            delivery_verified = delivery_status in {"acknowledged", "delivered"}
            winners.append(
                {
                    "run_id": int(run["id"]),
                    "created_at": run.get("created_at"),
                    "platform": run.get("platform"),
                    "persona": run.get("persona"),
                    "source_text": run.get("source_text"),
                    "winner_source": resolved.get("source"),
                    "selection_strategy": resolved.get("selection_strategy"),
                    "selection_preset": resolved.get("selection_preset"),
                    "candidate_id": candidate.get("id"),
                    "candidate_rank_index": candidate.get("rank_index"),
                    "candidate_text": candidate.get("text"),
                    "candidate_tactic": candidate.get("tactic"),
                    "candidate_objective": candidate.get("objective"),
                    "candidate_rank_score": candidate.get("rank_score"),
                    "estimated_bite_score": candidate.get("estimated_bite_score"),
                    "estimated_audience_score": candidate.get("estimated_audience_score"),
                    "critic_penalty": candidate.get("critic_penalty"),
                    "emit_outbox_id": (emit or {}).get("id"),
                    "emit_dispatch_id": (dispatch or {}).get("id"),
                    "delivery_status": delivery_status,
                    "delivery_verified": delivery_verified,
                    "winner_score": winner_score,
                    "outcome": outcome,
                    "candidate": candidate,
                    "emit": emit,
                    "dispatch": dispatch,
                    "reputation": {
                        "reply_rate": reputation.get("reply_rate"),
                        "avg_reply_delay": reputation.get("avg_reply_delay"),
                        "avg_engagement": reputation.get("avg_engagement"),
                        "delivery_confidence": reputation.get("delivery_confidence"),
                        "total_runs": reputation.get("total_runs"),
                    },
                    "tactic_history": tactic_history,
                }
            )

        winners.sort(
            key=lambda item: (
                -float(item.get("winner_score") or 0.0),
                -int((item.get("outcome") or {}).get("spectator_engagement") or 0),
                int((item.get("outcome") or {}).get("reply_delay_seconds") or 999999999),
                -(1 if item.get("delivery_verified") else 0),
                -float(item.get("candidate_rank_score") or 0.0),
                int(item.get("candidate_rank_index") or 999999999),
                -int(item.get("run_id") or 0),
            )
        )
        return winners[:limit]

    @staticmethod
    def _pick_best_candidate(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not candidates:
            return None
        return max(
            candidates,
            key=lambda item: (
                float(item.get("rank_score") or 0.0),
                float(item.get("estimated_bite_score") or 0.0),
                float(item.get("estimated_audience_score") or 0.0),
                -float(item.get("critic_penalty") or 0.0),
                -int(item.get("rank_index") or 999999999),
            ),
        )

    @classmethod
    def _resolve_winner_candidate(cls, run: dict[str, Any]) -> dict[str, Any]:
        candidates = list(run.get("candidates") or [])
        outcome = run.get("outcome") or {}
        emit_records = list(run.get("emit_outbox") or [])
        dispatch_records = list(run.get("emit_dispatches") or [])

        emit = next(
            (item for item in emit_records if int(item.get("id") or 0) == int(outcome.get("emit_outbox_id") or 0)),
            None,
        )
        dispatch = next(
            (item for item in dispatch_records if int(item.get("id") or 0) == int(outcome.get("emit_dispatch_id") or 0)),
            None,
        )
        if dispatch is None and emit is not None:
            dispatch = next(
                (item for item in dispatch_records if int(item.get("emit_outbox_id") or 0) == int(emit.get("id") or 0)),
                None,
            )
        if emit is None and outcome.get("emit_outbox_id") is None and emit_records:
            emit = emit_records[0]
        if dispatch is None and outcome.get("emit_dispatch_id") is None and dispatch_records:
            dispatch = dispatch_records[0]

        envelope = (emit or {}).get("envelope") if isinstance((emit or {}).get("envelope"), dict) else {}
        metadata = envelope.get("metadata") if isinstance(envelope.get("metadata"), dict) else {}
        selection_strategy = metadata.get("selection_strategy") or (emit or {}).get("selection_strategy")
        selection_preset = metadata.get("selection_preset") or (emit or {}).get("selection_preset")

        candidate = None
        source = "unresolved"
        rank_index = envelope.get("candidate_rank_index") if isinstance(envelope, dict) else None
        if rank_index is not None:
            candidate = next(
                (item for item in candidates if int(item.get("rank_index") or 0) == int(rank_index)),
                None,
            )
            if candidate is not None:
                source = "linked_emit_rank_index"

        if candidate is None and emit is not None:
            candidate_tactic = metadata.get("candidate_tactic") or (emit or {}).get("tactic")
            candidate_objective = metadata.get("candidate_objective") or (emit or {}).get("objective")
            if candidate_tactic is not None or candidate_objective is not None:
                matches = [
                    item
                    for item in candidates
                    if (candidate_tactic is None or item.get("tactic") == candidate_tactic)
                    and (candidate_objective is None or item.get("objective") == candidate_objective)
                ]
                candidate = cls._pick_best_candidate(matches)
                if candidate is not None:
                    source = "linked_emit_filter"

        if candidate is None:
            candidate = next((item for item in candidates if int(item.get("rank_index") or 0) == 1), None)
            if candidate is not None:
                source = "rank_fallback"

        if candidate is None:
            candidate = cls._pick_best_candidate(candidates)
            if candidate is not None:
                source = "score_fallback"

        return {
            "candidate": candidate,
            "emit": emit,
            "dispatch": dispatch,
            "source": source,
            "selection_strategy": selection_strategy,
            "selection_preset": selection_preset,
        }

    @staticmethod
    def _score_winner_candidate(
        candidate: dict[str, Any],
        outcome: dict[str, Any] | None,
        dispatch: dict[str, Any] | None,
        tactic_history: dict[str, Any] | None,
        reputation: dict[str, Any] | None,
    ) -> float:
        outcome = outcome or {}
        dispatch = dispatch or {}
        tactic_history = tactic_history or {}
        reputation = reputation or {}

        got_reply = 1.0 if outcome.get("got_reply") else 0.0
        engagement = min(float(outcome.get("spectator_engagement") or 0.0) / 10.0, 1.0)
        reply_length = min(float(outcome.get("reply_length") or 0.0) / 280.0, 1.0)

        speed = 0.0
        reply_delay_seconds = outcome.get("reply_delay_seconds")
        if reply_delay_seconds is not None:
            delay_value = float(reply_delay_seconds)
            speed = max(min((900.0 - delay_value) / 900.0, 1.0), -0.35)

        delivery_status = str(dispatch.get("status") or "")
        if delivery_status in {"acknowledged", "delivered"}:
            delivery = 1.0
        elif delivery_status == "dispatched":
            delivery = 0.5
        else:
            delivery = 0.0

        rank_score = float(candidate.get("rank_score") or 0.0)
        bite = float(candidate.get("estimated_bite_score") or 0.0)
        audience = float(candidate.get("estimated_audience_score") or 0.0)
        penalty = float(candidate.get("critic_penalty") or 0.0)

        hist_rate = float(tactic_history.get("rate") or reputation.get("reply_rate") or 0.0)
        hist_delivery = float(tactic_history.get("delivery_confidence") or reputation.get("delivery_confidence") or 0.0)
        hist_engagement = min(float(tactic_history.get("avg_engagement") or reputation.get("avg_engagement") or 0.0) / 10.0, 1.0)
        hist_speed = 0.0
        hist_reply_delay = tactic_history.get("avg_reply_delay") or reputation.get("avg_reply_delay")
        if hist_reply_delay is not None:
            hist_speed = max(min((900.0 - float(hist_reply_delay)) / 900.0, 1.0), -0.35)

        outcome_score = got_reply * 1.2 + delivery * 0.25 + engagement * 0.3 + speed * 0.15 + reply_length * 0.05
        candidate_score = rank_score * 0.3 + bite * 0.08 + audience * 0.05 - penalty * 0.08
        history_score = hist_rate * 0.15 + hist_delivery * 0.08 + hist_engagement * 0.05 + hist_speed * 0.02
        return round(outcome_score + candidate_score + history_score, 4)

    @classmethod
    def _dispatch_via_driver(
        cls,
        driver: str,
        *,
        emit_id: int,
        request: dict[str, Any],
        request_for_audit: dict[str, Any] | None = None,
        out_dir: str | Path | None = None,
    ) -> dict[str, Any]:
        if driver not in cls.supported_dispatch_drivers():
            raise ValueError(f"unknown dispatch driver: {driver}")
        target_dir = Path(out_dir).expanduser().resolve() if out_dir else cls.default_dispatch_dir()
        target_dir.mkdir(parents=True, exist_ok=True)
        stem = cls._slugify_transport(str(request.get("transport") or "emit"))
        audit_request = request_for_audit if isinstance(request_for_audit, dict) else cls._redact_sensitive(request)
        
        if driver == "webhook_post":
            # Real delivery driver (HTTP POST)
            webhook_url = (
                request.get("webhook_url")
                or request.get("emit_request", {}).get("webhook_url")
                or request.get("request", {}).get("webhook_url")
                or request.get("envelope", {}).get("webhook_url")
            )
            if not webhook_url:
                error_msg = f"webhook_post driver requires 'webhook_url' in emit request (emit_id={emit_id})"
                return {"status": "failed", "driver": driver, "error": error_msg}
            
            payload = {
                "driver": driver,
                "emit_id": emit_id,
                "payload": audit_request,
                "sha256": hashlib.sha256(json.dumps(audit_request, ensure_ascii=False).encode("utf-8")).hexdigest(),
            }
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            req = urllib.request.Request(
                webhook_url, 
                data=data, 
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            try:
                with urllib.request.urlopen(req, timeout=10) as response:
                    resp_body = response.read().decode("utf-8")
                    return {
                        "status": "delivered",
                        "driver": driver,
                        "http_code": response.status,
                        "response_body": resp_body[:2000], # Cap for DB safety
                        "delivered_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        "sha256": payload["sha256"],
                    }
            except Exception as e:
                return {
                    "status": "failed",
                    "driver": driver,
                    "error": str(e),
                    "error_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    "sha256": payload["sha256"],
                }

        if driver == "reddit_api":
            emit_request = request.get("emit_request") if isinstance(request.get("emit_request"), dict) else {}
            body = emit_request.get("request") if isinstance(emit_request.get("request"), dict) else {}
            thing_id = body.get("thing_id")
            text = body.get("text")
            access_token = (
                emit_request.get("oauth_access_token")
                or emit_request.get("access_token")
                or request.get("oauth_access_token")
                or request.get("access_token")
                or os.environ.get("REDDIT_ACCESS_TOKEN")
            )
            user_agent = (
                emit_request.get("user_agent")
                or request.get("user_agent")
                or os.environ.get("REDDIT_USER_AGENT")
                or "bait-engine-v2/1.0"
            )
            if not thing_id or not text:
                return {
                    "status": "failed",
                    "driver": driver,
                    "error": f"reddit_api driver requires request.thing_id and request.text (emit_id={emit_id})",
                }
            if not access_token:
                return {
                    "status": "failed",
                    "driver": driver,
                    "error": f"reddit_api driver requires oauth_access_token (emit_id={emit_id})",
                }
            payload = {"thing_id": thing_id, "text": text, "api_type": "json"}
            encoded = urllib.parse.urlencode(payload).encode("utf-8")
            req = urllib.request.Request(
                "https://oauth.reddit.com/api/comment",
                data=encoded,
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "User-Agent": user_agent,
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=15) as response:
                    raw = response.read().decode("utf-8")
                    parsed = json.loads(raw) if raw else {}
                    thing = (((parsed.get("json") or {}).get("data") or {}).get("things") or [{}])[0] if isinstance(parsed, dict) else {}
                    thing_data = thing.get("data") if isinstance(thing, dict) else {}
                    return {
                        "status": "delivered",
                        "driver": driver,
                        "http_code": response.status,
                        "reddit_thing_id": thing_data.get("name") or thing_data.get("id"),
                        "response_body": raw[:2000],
                        "delivered_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        "sha256": hashlib.sha256(json.dumps(audit_request, ensure_ascii=False).encode("utf-8")).hexdigest(),
                    }
            except Exception as e:
                return {
                    "status": "failed",
                    "driver": driver,
                    "error": str(e),
                    "error_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    "sha256": hashlib.sha256(json.dumps(audit_request, ensure_ascii=False).encode("utf-8")).hexdigest(),
                }

        if driver == "x_api":
            emit_request = request.get("emit_request") if isinstance(request.get("emit_request"), dict) else {}
            body = emit_request.get("request") if isinstance(emit_request.get("request"), dict) else {}
            in_reply_to_tweet_id = body.get("in_reply_to_tweet_id")
            text = body.get("text")
            access_token = (
                emit_request.get("oauth_access_token")
                or emit_request.get("access_token")
                or request.get("oauth_access_token")
                or request.get("access_token")
                or os.environ.get("X_ACCESS_TOKEN")
                or os.environ.get("TWITTER_BEARER_TOKEN")
            )
            if not in_reply_to_tweet_id or not text:
                return {
                    "status": "failed",
                    "driver": driver,
                    "error": f"x_api driver requires request.in_reply_to_tweet_id and request.text (emit_id={emit_id})",
                }
            if not access_token:
                return {
                    "status": "failed",
                    "driver": driver,
                    "error": f"x_api driver requires oauth_access_token or X_ACCESS_TOKEN (emit_id={emit_id})",
                }
            payload = {
                "text": text,
                "reply": {"in_reply_to_tweet_id": str(in_reply_to_tweet_id)},
            }
            encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            req = urllib.request.Request(
                "https://api.x.com/2/tweets",
                data=encoded,
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=15) as response:
                    raw = response.read().decode("utf-8")
                    parsed = json.loads(raw) if raw else {}
                    created_id = (parsed.get("data") or {}).get("id") if isinstance(parsed, dict) else None
                    return {
                        "status": "delivered",
                        "driver": driver,
                        "http_code": response.status,
                        "tweet_id": created_id,
                        "response_body": raw[:2000],
                        "delivered_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        "sha256": hashlib.sha256(json.dumps(audit_request, ensure_ascii=False).encode("utf-8")).hexdigest(),
                    }
            except Exception as e:
                return {
                    "status": "failed",
                    "driver": driver,
                    "error": str(e),
                    "error_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    "sha256": hashlib.sha256(json.dumps(audit_request, ensure_ascii=False).encode("utf-8")).hexdigest(),
                }

        if driver == "jsonl_append":
            # Real delivery driver (local append)
            target_path = target_dir / "dispatches.jsonl"
            payload = {
                "driver": driver,
                "status": "delivered",
                "delivered_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "emit_id": emit_id,
                "payload": audit_request,
                "sha256": hashlib.sha256(json.dumps(audit_request, ensure_ascii=False).encode("utf-8")).hexdigest(),
            }
            with target_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")
            return {
                "status": "delivered", 
                "driver": driver, 
                "artifact_path": str(target_path), 
                "sha256": payload["sha256"],
                "delivered_at": payload["delivered_at"],
            }

        # Default (manual_copy) — creates a standalone JSON sidecar
        target_path = target_dir / f"emit-{emit_id:04d}-{stem}.json"
        payload = {
            "driver": driver,
            "status": "dispatched",
            "emit_id": emit_id,
            "artifact_path": str(target_path),
            "payload": audit_request,
            "sha256": hashlib.sha256(json.dumps(audit_request, ensure_ascii=False).encode("utf-8")).hexdigest(),
        }
        target_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        return {"status": "dispatched", "driver": driver, "artifact_path": str(target_path), "sha256": payload["sha256"]}

    @staticmethod
    def _slugify_transport(value: str) -> str:
        cleaned = re.sub(r"[^a-zA-Z0-9]+", "-", value).strip("-").lower()
        return cleaned or "emit"

    @staticmethod
    def _age_seconds(created_at: Any, *, now: float | None = None) -> float | None:
        if not created_at:
            return None
        try:
            created = datetime.strptime(str(created_at), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError:
            return None
        current = now if now is not None else time.time()
        return max(0.0, current - created.timestamp())

    @staticmethod
    def _is_due(timestamp_value: Any, *, now: float | None = None) -> bool:
        if not timestamp_value:
            return True
        try:
            ts = datetime.strptime(str(timestamp_value), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            return True
        current = now if now is not None else time.time()
        return ts <= current

    @staticmethod
    def can_execute_action(
        *,
        now_utc: datetime | None = None,
        actions_last_hour: int = 0,
        actions_last_day: int = 0,
        seconds_since_last_action: float | None = None,
        max_actions_per_hour: int = 0,
        max_actions_per_day: int = 0,
        min_seconds_between_actions: float = 0.0,
        quiet_hours_start: int | None = None,
        quiet_hours_end: int | None = None,
    ) -> dict[str, Any]:
        current = now_utc or datetime.now(timezone.utc)
        hourly_cap = max(0, int(max_actions_per_hour))
        daily_cap = max(0, int(max_actions_per_day))
        min_gap = max(0.0, float(min_seconds_between_actions))

        if hourly_cap and int(actions_last_hour) >= hourly_cap:
            return {"allow": False, "reason": "hourly_cap", "segment_confidence": 1.0}
        if daily_cap and int(actions_last_day) >= daily_cap:
            return {"allow": False, "reason": "daily_cap", "segment_confidence": 1.0}
        if min_gap and seconds_since_last_action is not None and float(seconds_since_last_action) < min_gap:
            return {"allow": False, "reason": "cooldown", "segment_confidence": 1.0}

        if quiet_hours_start is not None and quiet_hours_end is not None:
            start = int(quiet_hours_start) % 24
            end = int(quiet_hours_end) % 24
            hour = int(current.hour)
            if start == end:
                quiet = True
            elif start < end:
                quiet = start <= hour < end
            else:
                quiet = hour >= start or hour < end
            if quiet:
                return {"allow": False, "reason": "quiet_hours", "segment_confidence": 1.0}

        return {"allow": True, "reason": "under_limits", "segment_confidence": 1.0}

    def _action_window_stats(self, *, now: float | None = None) -> dict[str, Any]:
        current = now if now is not None else time.time()
        with open_db(self.path) as conn:
            rows = conn.execute("SELECT created_at FROM emit_dispatches ORDER BY created_at DESC LIMIT 5000").fetchall()
        timestamps = [
            self._parse_created_at(dict(row).get("created_at"))
            for row in rows
        ]
        valid = [float(ts) for ts in timestamps if ts is not None]
        last_action_ts = max(valid) if valid else None
        return {
            "actions_last_hour": sum(1 for ts in valid if ts >= current - 3600),
            "actions_last_day": sum(1 for ts in valid if ts >= current - 86400),
            "seconds_since_last_action": (current - last_action_ts) if last_action_ts is not None else None,
            "last_action_at": datetime.fromtimestamp(last_action_ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S") if last_action_ts is not None else None,
        }

    def rebuild_request(
        self,
        run_id: int,
        candidate_count: int | None = None,
        mutation_source: str = "auto",
    ) -> DraftRequest:
        run = self.get_run(run_id)
        plan = DecisionPlan.model_validate(run["plan"])
        base_persona = get_persona(run.get("persona"))
        pressure_context, pressure_cues = self._persona_pressure_adjustments(
            persona=base_persona.name,
            platform=str(run.get("platform") or ""),
        )
        persona = self._apply_persona_pressure_adjustments(
            base_persona,
            cue_context=pressure_context,
            extra_cues=pressure_cues,
        )
        resolved_candidate_count = candidate_count or len(run.get("candidates") or []) or 5
        mutation_seeds = (
            self._select_mutation_seeds(
                persona=persona.name,
                platform=str(run.get("platform") or ""),
                tactic=plan.selected_tactic.value if plan.selected_tactic else None,
                objective=plan.selected_objective.value if plan.selected_objective else None,
                limit=min(resolved_candidate_count, 3),
            )
            if mutation_source == "auto"
            else []
        )
        mutation_context, winner_anchors, avoid_patterns = self._derive_mutation_context(mutation_seeds)
        if pressure_context:
            mutation_context = f"{mutation_context} | {pressure_context}" if mutation_context else pressure_context
        stored_register = float((run.get("analysis") or {}).get("target_register") or 0.5)
        return DraftRequest(
            source_text=run["source_text"],
            plan=plan,
            persona=persona,
            candidate_count=resolved_candidate_count,
            mutation_seeds=mutation_seeds,
            mutation_context=mutation_context,
            winner_anchors=winner_anchors,
            avoid_patterns=avoid_patterns,
            target_register=stored_register,
        )

    @staticmethod
    def _decode_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
        candidate["critic_notes"] = json.loads(candidate.pop("critic_notes_json"))
        return candidate

    @staticmethod
    def _decode_emit_outbox(record: dict[str, Any]) -> dict[str, Any]:
        record["envelope"] = json.loads(record.pop("envelope_json"))
        record["emit_request"] = json.loads(record.pop("emit_request_json"))
        return record

    @staticmethod
    def _decode_emit_dispatch(record: dict[str, Any]) -> dict[str, Any]:
        record["request"] = json.loads(record.pop("request_json"))
        record["response"] = json.loads(record.pop("response_json"))
        return record

    @staticmethod
    def _decode_intake_target(record: dict[str, Any]) -> dict[str, Any]:
        record["score"] = json.loads(record.pop("score_json"))
        record["analysis"] = json.loads(record.pop("analysis_json"))
        record["context"] = json.loads(record.pop("context_json"))
        record["metadata"] = json.loads(record.pop("metadata_json"))
        return record
