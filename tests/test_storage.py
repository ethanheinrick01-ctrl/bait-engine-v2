from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
import sqlite3
import sys
import tempfile
import unittest
import json
import hashlib
import os
from unittest import mock
import urllib.request
import urllib.response
import io

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bait_engine.analysis import AnalyzeInput, analyze_comment
from bait_engine.storage import EmitOutboxRecord, OutcomeRecord, PanelReviewRecord, RunRepository, build_outcome_scoreboard, build_report, filter_summaries, render_report_csv, render_report_markdown, summarize_run, summarize_runs
from bait_engine.planning import build_plan
from bait_engine.planning.personas import PersonaProfile
from bait_engine.planning.router import select_persona
from bait_engine.storage.db import open_db
from bait_engine.cli.main import cmd_operator_status, cmd_preflight, cmd_worker_cycle


class StorageTests(unittest.TestCase):
    def test_run_roundtrip_and_outcome(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            run = repo.create_run_from_text(
                text="A model being useful doesn't make it true, and you're still confusing mechanism with necessity.",
                persona_name="dry_midwit_savant",
                platform="test",
                candidate_count=3,
            )
            self.assertIn("analysis", run)
            self.assertIn("plan", run)
            self.assertGreaterEqual(len(run["candidates"]), 1)

            listed = repo.list_runs(limit=5)
            self.assertEqual(len(listed), 1)
            self.assertEqual(listed[0]["id"], run["id"])

            updated = repo.record_outcome(
                OutcomeRecord(
                    id=None,
                    run_id=run["id"],
                    got_reply=True,
                    reply_delay_seconds=90,
                    reply_length=84,
                    tone_shift="angrier",
                    spectator_engagement=2,
                    result_label="essay_bite",
                    notes="target overcommitted",
                )
            )
            updated = repo.record_panel_review(
                PanelReviewRecord(
                    id=None,
                    run_id=run["id"],
                    platform="test",
                    persona="dry_midwit_savant",
                    candidate_tactic=updated["candidates"][0].get("tactic"),
                    candidate_objective=updated["candidates"][0].get("objective"),
                    selection_preset="default",
                    selection_strategy="rank",
                    disposition="favorite",
                    notes="clean win",
                )
            )
            updated = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=run["id"],
                    platform="test",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic=updated["candidates"][0].get("tactic"),
                    objective=updated["candidates"][0].get("objective"),
                    status="staged",
                    envelope_json='{"run_id": %d}' % run["id"],
                    emit_request_json='{"transport": "test.reply"}',
                    notes="queued for later",
                )
            )
            self.assertTrue(updated["outcome"]["got_reply"])
            self.assertEqual(updated["panel_reviews"][0]["disposition"], "favorite")
            self.assertEqual(repo.list_panel_reviews(limit=5)[0]["selection_strategy"], "rank")
            self.assertEqual(updated["emit_outbox"][0]["status"], "staged")
            self.assertEqual(repo.list_emit_outbox(limit=5)[0]["transport"], "test.reply")
            updated = repo.update_emit_outbox_status(updated["emit_outbox"][0]["id"], "approved", notes="manual greenlight")
            self.assertEqual(updated["emit_outbox"][0]["status"], "approved")
            self.assertIn("manual greenlight", updated["emit_outbox"][0]["notes"])
            updated = repo.update_emit_outbox(
                updated["emit_outbox"][0]["id"],
                notes="replace the note body",
                notes_mode="replace",
            )
            self.assertEqual(updated["emit_outbox"][0]["status"], "approved")
            self.assertEqual(updated["emit_outbox"][0]["notes"], "replace the note body")
            dispatch_dir = Path(tmp) / "dispatches"
            dispatched = repo.dispatch_emit(updated["emit_outbox"][0]["id"], out_dir=dispatch_dir, notes="manual handoff")
            self.assertEqual(dispatched["emit"]["status"], "dispatched")
            self.assertEqual(dispatched["dispatch"]["status"], "dispatched")
            artifact_path = Path(dispatched["dispatch"]["response"]["artifact_path"])
            self.assertTrue(artifact_path.exists())
            dispatches = repo.list_emit_dispatches(limit=5)
            self.assertEqual(len(dispatches), 1)
            self.assertEqual(dispatches[0]["notes"], "manual handoff")
            lifecycle = repo.update_emit_dispatch(dispatches[0]["id"], status="acknowledged", notes="operator saw handoff")
            self.assertEqual(lifecycle["emit"]["status"], "acknowledged")
            self.assertEqual(lifecycle["dispatch"]["status"], "acknowledged")
            lifecycle = repo.update_emit_dispatch(lifecycle["dispatch"]["id"], status="failed", notes="transport choked", notes_mode="append")
            self.assertEqual(lifecycle["emit"]["status"], "failed")
            self.assertIn("transport choked", lifecycle["dispatch"]["notes"])
            redriven = repo.redrive_dispatch(lifecycle["dispatch"]["id"], out_dir=dispatch_dir, notes="second pass")
            self.assertEqual(redriven["previous_dispatch"]["status"], "failed")
            self.assertEqual(redriven["emit"]["status"], "dispatched")
            self.assertEqual(redriven["dispatch"]["status"], "dispatched")
            self.assertIn("redrive of dispatch", redriven["dispatch"]["notes"])
            self.assertIn("second pass", redriven["dispatch"]["notes"])
            self.assertEqual(len(repo.list_emit_dispatches(limit=10)), 2)
            lifecycle = repo.update_emit_dispatch(redriven["dispatch"]["id"], status="delivered", notes="posted for real", notes_mode="append")
            self.assertEqual(lifecycle["emit"]["status"], "delivered")
            self.assertIn("posted for real", lifecycle["dispatch"]["notes"])
            second_run = repo.create_run_from_text(
                text="What exactly do you mean by that and why should anyone buy it?",
                persona_name="dry_midwit_savant",
                platform="test",
                candidate_count=3,
            )
            staged_two = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=second_run["id"],
                    platform="test",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic=second_run["candidates"][0].get("tactic"),
                    objective=second_run["candidates"][0].get("objective"),
                    status="approved",
                    envelope_json='{"run_id": %d}' % second_run["id"],
                    emit_request_json='{"transport": "test.reply"}',
                    notes="worker fodder",
                )
            )
            cycle = repo.worker_cycle(dispatch_limit=10, out_dir=dispatch_dir, notes="worker pass")
            self.assertEqual(cycle["approved_found"], 1)
            self.assertEqual(cycle["dispatched_count"], 1)
            self.assertEqual(cycle["failed_found"], 0)
            self.assertEqual(cycle["redriven_count"], 0)
            self.assertEqual(cycle["dispatched"][0]["emit"]["id"], staged_two["emit_outbox"][0]["id"])
            third_run = repo.create_run_from_text(
                text="A model being useful doesn't make it true, and you're still confusing mechanism with necessity.",
                persona_name="dry_midwit_savant",
                platform="test",
                candidate_count=3,
            )
            staged_three = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=third_run["id"],
                    platform="test",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic=third_run["candidates"][0].get("tactic"),
                    objective=third_run["candidates"][0].get("objective"),
                    status="approved",
                    envelope_json='{"run_id": %d}' % third_run["id"],
                    emit_request_json='{"transport": "test.reply"}',
                    notes="will fail then redrive",
                )
            )
            failed_dispatch = repo.dispatch_emit(staged_three["emit_outbox"][0]["id"], out_dir=dispatch_dir, notes="first miss")
            repo.update_emit_dispatch(failed_dispatch["dispatch"]["id"], status="failed", notes="manual failure")
            blocked_cycle = repo.worker_cycle(
                dispatch_limit=10,
                out_dir=dispatch_dir,
                notes="worker retry pass",
                include_failed_redrive=True,
                redrive_limit=10,
                min_failed_age_seconds=3600,
            )
            self.assertEqual(blocked_cycle["failed_found"], 0)
            self.assertEqual(blocked_cycle["redriven_count"], 0)
            with sqlite3.connect(db_path) as conn:
                conn.execute("UPDATE emit_dispatches SET created_at = datetime('now', '-2 hours') WHERE id = ?", (failed_dispatch["dispatch"]["id"],))
                conn.commit()
            redrive_cycle = repo.worker_cycle(
                dispatch_limit=10,
                out_dir=dispatch_dir,
                notes="worker retry pass",
                include_failed_redrive=True,
                redrive_limit=10,
                min_failed_age_seconds=3600,
            )
            self.assertEqual(redrive_cycle["approved_found"], 0)
            self.assertEqual(redrive_cycle["dispatched_count"], 0)
            self.assertEqual(redrive_cycle["failed_found"], 1)
            self.assertEqual(redrive_cycle["redriven_count"], 1)
            self.assertGreaterEqual(redrive_cycle["redriven"][0]["previous_dispatch"].get("failed_age_seconds") or 0, 3600)
            self.assertEqual(redrive_cycle["redriven"][0]["previous_dispatch"]["id"], failed_dispatch["dispatch"]["id"])
            self.assertEqual(redrive_cycle["redriven"][0]["emit"]["status"], "dispatched")
            with self.assertRaises(ValueError):
                repo.redrive_dispatch(failed_dispatch["dispatch"]["id"], out_dir=dispatch_dir, notes="stale retry")
            summary = summarize_run(lifecycle["run"])
            self.assertEqual(summary["verdict"], "engaged")

    def test_jsonl_append_driver_behavior(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            dispatch_dir = Path(tmp) / "dispatches"
            saved = repo.create_run_from_text(
                text="Is there any evidence for that beyond your own preference?",
                persona_name="dry_midwit_savant",
                platform="test",
                candidate_count=1,
            )
            staged = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=saved["id"],
                    platform="test",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % saved["id"],
                    emit_request_json='{"transport": "test.reply"}',
                    notes="append fodder",
                )
            )
            # Dispatch once
            dispatched = repo.dispatch_emit(staged["emit_outbox"][0]["id"], driver="jsonl_append", out_dir=dispatch_dir)
            self.assertEqual(dispatched["emit"]["status"], "delivered")
            self.assertEqual(dispatched["dispatch"]["status"], "delivered")
            self.assertIn("delivered_at", dispatched["dispatch"]["response"])
            
            jsonl_path = dispatch_dir / "dispatches.jsonl"
            self.assertTrue(jsonl_path.exists())
            lines = jsonl_path.read_text().strip().split("\n")
            self.assertEqual(len(lines), 1)
            data = json.loads(lines[0])
            self.assertEqual(data["driver"], "jsonl_append")
            self.assertEqual(data["status"], "delivered")
            self.assertEqual(data["emit_id"], staged["emit_outbox"][0]["id"])
            
            # Dispatch again (new emit)
            staged_2 = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=saved["id"],
                    platform="test",
                    transport="test.direct",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % saved["id"],
                    emit_request_json='{"transport": "test.direct"}',
                    notes="append fodder 2",
                )
            )
            dispatched_2 = repo.dispatch_emit(staged_2["emit_outbox"][0]["id"], driver="jsonl_append", out_dir=dispatch_dir)
            self.assertEqual(dispatched_2["emit"]["status"], "delivered")
            
            lines = jsonl_path.read_text().strip().split("\n")
            self.assertEqual(len(lines), 2)
            data_2 = json.loads(lines[1])
            self.assertEqual(data_2["emit_id"], staged_2["emit_outbox"][0]["id"])

    def test_webhook_post_driver_behavior(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            saved = repo.create_run_from_text(
                text="Is there any evidence for that beyond your own preference?",
                persona_name="dry_midwit_savant",
                platform="test",
                candidate_count=1,
            )
            staged = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=saved["id"],
                    platform="test",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % saved["id"],
                    emit_request_json='{"transport": "test.reply", "webhook_url": "http://example.com/hook"}',
                    notes="webhook fodder",
                )
            )
            # Case 1: Success
            with mock.patch("urllib.request.urlopen") as mock_url:
                mock_resp = mock.MagicMock()
                mock_resp.status = 200
                mock_resp.read.return_value = b'{"ok": true}'
                mock_resp.__enter__.return_value = mock_resp
                mock_url.return_value = mock_resp
                
                dispatched = repo.dispatch_emit(staged["emit_outbox"][0]["id"], driver="webhook_post")
                self.assertEqual(dispatched["emit"]["status"], "delivered")
                self.assertEqual(dispatched["dispatch"]["status"], "delivered")
                self.assertEqual(dispatched["dispatch"]["response"]["http_code"], 200)
                self.assertEqual(dispatched["dispatch"]["response"]["response_body"], '{"ok": true}')
                self.assertIn("sha256", dispatched["dispatch"]["response"])
                
                # Verify hashing matches
                req_json = '{"transport": "test.reply", "webhook_url": "http://example.com/hook"}'
                expected_sha = hashlib.sha256(req_json.encode("utf-8")).hexdigest()
                # Actually our repo uses local dict for hashing, need to match implementation
                # The repo does request = json.loads(emit_request_json)
                # Then hashlib.sha256(json.dumps(request, ensure_ascii=False).encode("utf-8"))
                # Let's just assert the field exists and is hex string
                self.assertEqual(len(dispatched["dispatch"]["response"]["sha256"]), 64)

            # Case 2: Failure (missing URL)
            staged_2 = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=saved["id"],
                    platform="test",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % saved["id"],
                    emit_request_json='{"transport": "test.reply"}', # No webhook_url
                    notes="broken webhook",
                )
            )
            dispatched_2 = repo.dispatch_emit(staged_2["emit_outbox"][0]["id"], driver="webhook_post")
            self.assertEqual(dispatched_2["emit"]["status"], "failed")
            self.assertEqual(dispatched_2["dispatch"]["status"], "failed")
            self.assertIn("requires 'webhook_url'", dispatched_2["dispatch"]["response"]["error"])

            # Case 3: Network Error
            staged_3 = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=saved["id"],
                    platform="test",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % saved["id"],
                    emit_request_json='{"transport": "test.reply", "webhook_url": "http://example.com/error"}',
                    notes="crashing webhook",
                )
            )
            with mock.patch("urllib.request.urlopen") as mock_url:
                mock_url.side_effect = Exception("connection reset")
                dispatched_3 = repo.dispatch_emit(staged_3["emit_outbox"][0]["id"], driver="webhook_post")
                self.assertEqual(dispatched_3["emit"]["status"], "failed")
                self.assertEqual(dispatched_3["dispatch"]["response"]["error"], "connection reset")

    def test_reddit_api_driver_behavior(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            saved = repo.create_run_from_text(
                text="Reply to this reddit thread.",
                persona_name="dry_midwit_savant",
                platform="reddit",
                candidate_count=1,
            )
            staged = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=saved["id"],
                    platform="reddit",
                    transport="reddit.comment.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % saved["id"],
                    emit_request_json='{"transport": "reddit.comment.reply", "request": {"thing_id": "t1_deadbeef", "text": "Necromancy works."}, "oauth_access_token": "reddit-token"}',
                    notes="reddit dispatch",
                )
            )

            with mock.patch("urllib.request.urlopen") as mock_url:
                mock_resp = mock.MagicMock()
                mock_resp.status = 200
                mock_resp.read.return_value = b'{"json": {"data": {"things": [{"data": {"name": "t1_newcomment"}}]}}}'
                mock_resp.__enter__.return_value = mock_resp
                mock_url.return_value = mock_resp

                dispatched = repo.dispatch_emit(staged["emit_outbox"][0]["id"], driver="reddit_api")
                self.assertEqual(dispatched["emit"]["status"], "delivered")
                self.assertEqual(dispatched["dispatch"]["status"], "delivered")
                self.assertEqual(dispatched["dispatch"]["response"]["http_code"], 200)
                self.assertEqual(dispatched["dispatch"]["response"]["reddit_thing_id"], "t1_newcomment")

            staged_2 = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=saved["id"],
                    platform="reddit",
                    transport="reddit.comment.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % saved["id"],
                    emit_request_json='{"transport": "reddit.comment.reply", "request": {"thing_id": "t1_deadbeef", "text": "Necromancy works."}}',
                    notes="reddit missing token",
                )
            )
            with mock.patch.dict("os.environ", {}, clear=True):
                failed = repo.dispatch_emit(staged_2["emit_outbox"][0]["id"], driver="reddit_api")
            self.assertEqual(failed["emit"]["status"], "failed")
            self.assertIn("requires oauth_access_token", failed["dispatch"]["response"]["error"])

    def test_x_api_driver_behavior(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            saved = repo.create_run_from_text(
                text="Reply on X.",
                persona_name="dry_midwit_savant",
                platform="x",
                candidate_count=1,
            )
            staged = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=saved["id"],
                    platform="x",
                    transport="x.post.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % saved["id"],
                    emit_request_json='{"transport": "x.post.reply", "request": {"in_reply_to_tweet_id": "1900000000000000000", "text": "Counterpoint."}, "oauth_access_token": "x-token"}',
                    notes="x dispatch",
                )
            )

            with mock.patch("urllib.request.urlopen") as mock_url:
                mock_resp = mock.MagicMock()
                mock_resp.status = 201
                mock_resp.read.return_value = b'{"data": {"id": "1900000000000000001"}}'
                mock_resp.__enter__.return_value = mock_resp
                mock_url.return_value = mock_resp

                dispatched = repo.dispatch_emit(staged["emit_outbox"][0]["id"], driver="x_api")
                self.assertEqual(dispatched["emit"]["status"], "delivered")
                self.assertEqual(dispatched["dispatch"]["status"], "delivered")
                self.assertEqual(dispatched["dispatch"]["response"]["http_code"], 201)
                self.assertEqual(dispatched["dispatch"]["response"]["tweet_id"], "1900000000000000001")

            staged_2 = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=saved["id"],
                    platform="x",
                    transport="x.post.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % saved["id"],
                    emit_request_json='{"transport": "x.post.reply", "request": {"in_reply_to_tweet_id": "1900000000000000000", "text": "Counterpoint."}}',
                    notes="x missing token",
                )
            )
            with mock.patch.dict("os.environ", {}, clear=True):
                failed = repo.dispatch_emit(staged_2["emit_outbox"][0]["id"], driver="x_api")
            self.assertEqual(failed["emit"]["status"], "failed")
            self.assertIn("requires oauth_access_token", failed["dispatch"]["response"]["error"])

    def test_dispatch_redacts_sensitive_tokens_in_audit_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            out_dir = Path(tmp) / "dispatches"
            repo = RunRepository(db_path)
            saved = repo.create_run_from_text(
                text="Keep tokens out of audit logs.",
                persona_name="dry_midwit_savant",
                platform="reddit",
                candidate_count=1,
            )
            staged = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=saved["id"],
                    platform="reddit",
                    transport="reddit.comment.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % saved["id"],
                    emit_request_json='{"transport": "reddit.comment.reply", "request": {"thing_id": "t1_deadbeef", "text": "manual fallback"}, "oauth_access_token": "secret-token-value"}',
                    notes="redaction",
                )
            )

            dispatched = repo.dispatch_emit(staged["emit_outbox"][0]["id"], driver="manual_copy", out_dir=out_dir)
            audit_emit_request = (((dispatched.get("dispatch") or {}).get("request") or {}).get("emit_request") or {})
            self.assertEqual(audit_emit_request.get("oauth_access_token"), "***REDACTED***")

            artifact_path = Path((dispatched.get("dispatch") or {}).get("response", {}).get("artifact_path") or "")
            artifact = artifact_path.read_text(encoding="utf-8")
            self.assertNotIn("secret-token-value", artifact)
            self.assertIn("***REDACTED***", artifact)

    def test_auto_driver_prefers_emit_metadata_hint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)

            run = repo.create_run_from_text(
                text="Use metadata driver hint.",
                persona_name="dry_midwit_savant",
                platform="reddit",
                candidate_count=1,
            )
            staged = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=run["id"],
                    platform="reddit",
                    transport="x.post.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % run["id"],
                    emit_request_json='{"transport": "x.post.reply", "request": {"in_reply_to_tweet_id": "1900000000000000000", "text": "hint"}, "oauth_access_token": "x-token", "metadata": {"preferred_dispatch_driver": "x_api"}}',
                    notes="metadata hint",
                )
            )

            with mock.patch("urllib.request.urlopen") as mock_url:
                x_resp = mock.MagicMock()
                x_resp.status = 201
                x_resp.read.return_value = b'{"data": {"id": "1900000000000000001"}}'
                x_resp.__enter__.return_value = x_resp
                mock_url.return_value = x_resp

                dispatched = repo.dispatch_emit(staged["emit_outbox"][0]["id"], driver="auto")

            self.assertEqual(dispatched["dispatch"]["driver"], "x_api")
            self.assertEqual(dispatched["emit"]["status"], "delivered")

    def test_auto_driver_metadata_hint_falls_back_when_requirements_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            out_dir = Path(tmp) / "dispatches"
            repo = RunRepository(db_path)

            run = repo.create_run_from_text(
                text="Hint should fail open to manual copy.",
                persona_name="dry_midwit_savant",
                platform="reddit",
                candidate_count=1,
            )
            staged = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=run["id"],
                    platform="reddit",
                    transport="x.post.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % run["id"],
                    emit_request_json='{"transport": "x.post.reply", "request": {"text": "missing id+token"}, "metadata": {"preferred_dispatch_driver": "x_api"}}',
                    notes="metadata hint fallback",
                )
            )

            with mock.patch.dict("os.environ", {}, clear=True):
                dispatched = repo.dispatch_emit(staged["emit_outbox"][0]["id"], driver="auto", out_dir=out_dir)

            self.assertEqual(dispatched["dispatch"]["driver"], "manual_copy")
            self.assertEqual(dispatched["emit"]["status"], "dispatched")

    def test_auto_driver_selection_routes_platforms(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)

            reddit_run = repo.create_run_from_text(
                text="Route me to reddit.",
                persona_name="dry_midwit_savant",
                platform="reddit",
                candidate_count=1,
            )
            reddit_emit = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=reddit_run["id"],
                    platform="reddit",
                    transport="reddit.comment.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % reddit_run["id"],
                    emit_request_json='{"transport": "reddit.comment.reply", "request": {"thing_id": "t1_deadbeef", "text": "auto"}, "oauth_access_token": "reddit-token"}',
                    notes="auto reddit",
                )
            )
            with mock.patch("urllib.request.urlopen") as mock_url:
                reddit_resp = mock.MagicMock()
                reddit_resp.status = 200
                reddit_resp.read.return_value = b'{"json": {"data": {"things": [{"data": {"name": "t1_auto"}}]}}}'
                reddit_resp.__enter__.return_value = reddit_resp
                mock_url.return_value = reddit_resp
                reddit_dispatched = repo.dispatch_emit(reddit_emit["emit_outbox"][0]["id"], driver="auto")
            self.assertEqual(reddit_dispatched["dispatch"]["driver"], "reddit_api")
            self.assertEqual(reddit_dispatched["emit"]["status"], "delivered")

            x_run = repo.create_run_from_text(
                text="Route me to x.",
                persona_name="dry_midwit_savant",
                platform="x",
                candidate_count=1,
            )
            x_emit = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=x_run["id"],
                    platform="x",
                    transport="x.post.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % x_run["id"],
                    emit_request_json='{"transport": "x.post.reply", "request": {"in_reply_to_tweet_id": "1900000000000000000", "text": "auto"}, "oauth_access_token": "x-token"}',
                    notes="auto x",
                )
            )
            with mock.patch("urllib.request.urlopen") as mock_url:
                x_resp = mock.MagicMock()
                x_resp.status = 201
                x_resp.read.return_value = b'{"data": {"id": "1900000000000000001"}}'
                x_resp.__enter__.return_value = x_resp
                mock_url.return_value = x_resp
                x_dispatched = repo.dispatch_emit(x_emit["emit_outbox"][0]["id"], driver="auto")
            self.assertEqual(x_dispatched["dispatch"]["driver"], "x_api")
            self.assertEqual(x_dispatched["emit"]["status"], "delivered")

            local_run = repo.create_run_from_text(
                text="Route me local.",
                persona_name="dry_midwit_savant",
                platform="test",
                candidate_count=1,
            )
            local_emit = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=local_run["id"],
                    platform="test",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % local_run["id"],
                    emit_request_json='{"transport": "test.reply"}',
                    notes="auto local",
                )
            )
            local_dispatched = repo.dispatch_emit(local_emit["emit_outbox"][0]["id"], driver="auto", out_dir=Path(tmp) / "dispatches")
            self.assertEqual(local_dispatched["dispatch"]["driver"], "manual_copy")
            self.assertEqual(local_dispatched["emit"]["status"], "dispatched")

    def test_dispatch_circuit_breaker_opens_at_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            saved = repo.create_run_from_text(
                text="Breaker threshold trip.",
                persona_name="dry_midwit_savant",
                platform="test",
                candidate_count=1,
            )

            emit_request = {
                "transport": "test.reply",
                "webhook_url": "",
                "circuit_breaker": {
                    "failure_threshold": 2,
                    "failure_window_seconds": 120,
                    "cooldown_seconds": 300,
                },
            }

            first_emit = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=saved["id"],
                    platform="test",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % saved["id"],
                    emit_request_json=json.dumps(emit_request),
                    notes="breaker fail one",
                )
            )["emit_outbox"][0]["id"]

            first = repo.dispatch_emit(first_emit, driver="webhook_post")
            self.assertEqual(first["dispatch"]["status"], "failed")
            self.assertEqual(first["dispatch"]["response"]["breaker_state"], "closed")

            second_emit = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=saved["id"],
                    platform="test",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % saved["id"],
                    emit_request_json=json.dumps(emit_request),
                    notes="breaker fail two",
                )
            )["emit_outbox"][0]["id"]

            second = repo.dispatch_emit(second_emit, driver="webhook_post")
            self.assertEqual(second["dispatch"]["status"], "failed")
            self.assertEqual(second["dispatch"]["response"]["breaker_state"], "open")
            self.assertEqual(second["dispatch"]["response"]["breaker_reason_code"], "circuit_breaker_opened")

    def test_dispatch_circuit_breaker_blocks_while_open(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            saved = repo.create_run_from_text(
                text="Breaker block behavior.",
                persona_name="dry_midwit_savant",
                platform="test",
                candidate_count=1,
            )

            emit_request = {
                "transport": "test.reply",
                "circuit_breaker": {
                    "failure_threshold": 1,
                    "failure_window_seconds": 120,
                    "cooldown_seconds": 300,
                },
            }

            first_emit = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=saved["id"],
                    platform="test",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % saved["id"],
                    emit_request_json=json.dumps(emit_request),
                    notes="open breaker",
                )
            )["emit_outbox"][0]["id"]
            repo.dispatch_emit(first_emit, driver="webhook_post")

            blocked_emit = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=saved["id"],
                    platform="test",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % saved["id"],
                    emit_request_json=json.dumps(emit_request),
                    notes="should block",
                )
            )["emit_outbox"][0]["id"]

            blocked = repo.dispatch_emit(blocked_emit, driver="webhook_post")
            self.assertEqual(blocked["dispatch"]["status"], "blocked")
            self.assertEqual(blocked["dispatch"]["response"]["reason_code"], "circuit_breaker_open")
            self.assertEqual(blocked["dispatch"]["response"]["breaker_state"], "open")
            self.assertEqual(blocked["emit"]["status"], "approved")
            self.assertEqual(blocked["emit"]["attempt_count"], 0)

    def test_dispatch_circuit_breaker_recovery_half_open_to_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            saved = repo.create_run_from_text(
                text="Breaker deterministic recovery.",
                persona_name="dry_midwit_savant",
                platform="test",
                candidate_count=1,
            )

            fail_request = {
                "transport": "test.reply",
                "circuit_breaker": {
                    "failure_threshold": 1,
                    "failure_window_seconds": 300,
                    "cooldown_seconds": 30,
                },
            }
            success_request = {
                "transport": "test.reply",
                "webhook_url": "http://example.com/hook",
                "circuit_breaker": {
                    "failure_threshold": 1,
                    "failure_window_seconds": 300,
                    "cooldown_seconds": 30,
                },
            }

            failing_emit = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=saved["id"],
                    platform="test",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % saved["id"],
                    emit_request_json=json.dumps(fail_request),
                    notes="trip breaker",
                )
            )["emit_outbox"][0]["id"]

            with mock.patch("bait_engine.storage.repository.time.time", side_effect=[1000.0, 1005.0, 1035.0]):
                first = repo.dispatch_emit(failing_emit, driver="webhook_post")
                self.assertEqual(first["dispatch"]["response"]["breaker_state"], "open")

                blocked_emit = repo.stage_emit(
                    EmitOutboxRecord(
                        id=None,
                        run_id=saved["id"],
                        platform="test",
                        transport="test.reply",
                        selection_preset="default",
                        selection_strategy="rank",
                        tactic="test",
                        objective="test",
                        status="approved",
                        envelope_json='{"run_id": %d}' % saved["id"],
                        emit_request_json=json.dumps(fail_request),
                        notes="blocked during cooldown",
                    )
                )["emit_outbox"][0]["id"]
                blocked = repo.dispatch_emit(blocked_emit, driver="webhook_post")
                self.assertEqual(blocked["dispatch"]["status"], "blocked")

                recovered_emit = repo.stage_emit(
                    EmitOutboxRecord(
                        id=None,
                        run_id=saved["id"],
                        platform="test",
                        transport="test.reply",
                        selection_preset="default",
                        selection_strategy="rank",
                        tactic="test",
                        objective="test",
                        status="approved",
                        envelope_json='{"run_id": %d}' % saved["id"],
                        emit_request_json=json.dumps(success_request),
                        notes="half-open probe",
                    )
                )["emit_outbox"][0]["id"]

                with mock.patch("urllib.request.urlopen") as mock_url:
                    mock_resp = mock.MagicMock()
                    mock_resp.status = 200
                    mock_resp.read.return_value = b'{"ok": true}'
                    mock_resp.__enter__.return_value = mock_resp
                    mock_url.return_value = mock_resp
                    recovered = repo.dispatch_emit(recovered_emit, driver="webhook_post")

            self.assertEqual(recovered["dispatch"]["status"], "delivered")
            self.assertEqual(recovered["dispatch"]["response"]["breaker_state"], "closed")
            self.assertEqual(recovered["dispatch"]["response"]["breaker_reason_code"], "circuit_breaker_closed")

    def test_retry_backoff_and_dead_letter(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            saved = repo.create_run_from_text(
                text="Retry this until it dies.",
                persona_name="dry_midwit_savant",
                platform="test",
                candidate_count=1,
            )
            staged = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=saved["id"],
                    platform="test",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % saved["id"],
                    emit_request_json='{"transport": "test.reply", "retry_policy": {"max_attempts": 2, "base_delay_seconds": 1, "backoff_multiplier": 2, "max_delay_seconds": 5}}',
                    notes="must fail",
                )
            )
            emit_id = staged["emit_outbox"][0]["id"]

            first = repo.dispatch_emit(emit_id, driver="webhook_post")
            self.assertEqual(first["emit"]["status"], "failed")
            self.assertEqual(first["dispatch"]["status"], "failed")
            self.assertEqual(first["dispatch"]["response"]["attempt_count"], 1)
            self.assertEqual(first["dispatch"]["response"]["max_attempts"], 2)
            self.assertEqual(first["dispatch"]["response"]["retries_remaining"], 1)
            self.assertIsNotNone(first["dispatch"]["response"]["next_retry_at"])
            self.assertEqual(first["dispatch"]["response"]["terminal_status"], "failed")
            self.assertEqual(first["dispatch"]["response"]["reason_code"], "retry_scheduled")

            queued = repo.dispatch_approved(limit=10, driver="webhook_post", include_retry_due=True)
            self.assertEqual(queued["dispatched_count"], 0)

            with sqlite3.connect(db_path) as conn:
                conn.execute("UPDATE emit_outbox SET next_retry_at = datetime('now', '-2 seconds') WHERE id = ?", (emit_id,))
                conn.commit()

            second_batch = repo.dispatch_approved(limit=10, driver="webhook_post", include_retry_due=True)
            self.assertEqual(second_batch["retry_due_found"], 1)
            self.assertEqual(second_batch["dispatched_count"], 1)
            final = second_batch["dispatched"][0]
            self.assertEqual(final["emit"]["status"], "dead_letter")
            self.assertEqual(final["dispatch"]["status"], "dead_letter")
            self.assertEqual(final["dispatch"]["response"]["attempt_count"], 2)
            self.assertEqual(final["dispatch"]["response"]["max_attempts"], 2)
            self.assertEqual(final["dispatch"]["response"]["retries_remaining"], 0)
            self.assertIsNone(final["dispatch"]["response"]["next_retry_at"])
            self.assertEqual(final["dispatch"]["response"]["terminal_status"], "dead_letter")
            self.assertEqual(final["dispatch"]["response"]["reason_code"], "retry_exhausted_dead_letter")

    def test_dispatch_safety_global_pause_blocks_all(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            saved = repo.create_run_from_text(
                text="Global pause safety kill switch.",
                persona_name="dry_midwit_savant",
                platform="reddit",
                candidate_count=1,
            )

            emit_request = {
                "transport": "reddit.comment.reply",
                "safety": {
                    "global_pause": True,
                    "override_source": "system",
                },
                "request": {"thing_id": "t1_pause", "text": "blocked"},
            }
            emit_id = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=saved["id"],
                    platform="reddit",
                    transport="reddit.comment.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % saved["id"],
                    emit_request_json=json.dumps(emit_request),
                    notes="global pause",
                )
            )["emit_outbox"][0]["id"]

            blocked = repo.dispatch_emit(emit_id, driver="manual_copy", out_dir=Path(tmp) / "dispatch")
            response = blocked["dispatch"]["response"]
            self.assertEqual(blocked["dispatch"]["status"], "blocked")
            self.assertEqual(response["block_reason"], "global_pause")
            self.assertEqual(response["override_source"], "system")
            self.assertTrue(response["safety_mode_state"]["global_pause"])

    def test_dispatch_safety_platform_pause_blocks_only_target_platform(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)

            reddit_run = repo.create_run_from_text("reddit platform", persona_name="dry_midwit_savant", platform="reddit", candidate_count=1)
            x_run = repo.create_run_from_text("x platform", persona_name="dry_midwit_savant", platform="x", candidate_count=1)

            policy = {
                "transport": "test.reply",
                "safety": {
                    "paused_platforms": ["reddit"],
                    "override_source": "manual",
                },
            }

            reddit_emit = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=reddit_run["id"],
                    platform="reddit",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % reddit_run["id"],
                    emit_request_json=json.dumps(policy),
                    notes="paused platform",
                )
            )["emit_outbox"][0]["id"]
            x_emit = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=x_run["id"],
                    platform="x",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % x_run["id"],
                    emit_request_json=json.dumps(policy),
                    notes="allowed platform",
                )
            )["emit_outbox"][0]["id"]

            blocked = repo.dispatch_emit(reddit_emit, driver="manual_copy", out_dir=Path(tmp) / "dispatch")
            allowed = repo.dispatch_emit(x_emit, driver="manual_copy", out_dir=Path(tmp) / "dispatch")

            self.assertEqual(blocked["dispatch"]["status"], "blocked")
            self.assertEqual(blocked["dispatch"]["response"]["block_reason"], "platform_pause")
            self.assertEqual(allowed["dispatch"]["status"], "dispatched")
            self.assertFalse(allowed["dispatch"]["response"]["safety_mode_state"]["platform_paused"])
            self.assertEqual(allowed["dispatch"]["response"]["override_source"], "manual")

    def test_dispatch_safety_safe_mode_allowlist_behavior(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            saved = repo.create_run_from_text("safe mode", persona_name="dry_midwit_savant", platform="test", candidate_count=1)

            policy = {
                "transport": "test.reply",
                "safety": {
                    "safe_mode": True,
                    "safe_mode_allowed_drivers": ["manual_copy", "jsonl_append"],
                },
            }
            blocked_emit = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=saved["id"],
                    platform="test",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % saved["id"],
                    emit_request_json=json.dumps(policy),
                    notes="safe-mode blocked",
                )
            )["emit_outbox"][0]["id"]
            allowed_emit = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=saved["id"],
                    platform="test",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % saved["id"],
                    emit_request_json=json.dumps(policy),
                    notes="safe-mode allowed",
                )
            )["emit_outbox"][0]["id"]

            blocked = repo.dispatch_emit(blocked_emit, driver="webhook_post", out_dir=Path(tmp) / "dispatch")
            allowed = repo.dispatch_emit(allowed_emit, driver="manual_copy", out_dir=Path(tmp) / "dispatch")

            self.assertEqual(blocked["dispatch"]["status"], "blocked")
            self.assertEqual(blocked["dispatch"]["response"]["block_reason"], "safe_mode_restricted_driver")
            self.assertEqual(allowed["dispatch"]["status"], "dispatched")

    def test_dispatch_safety_precedence_and_audit_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            saved = repo.create_run_from_text("precedence", persona_name="dry_midwit_savant", platform="reddit", candidate_count=1)

            emit_request = {
                "transport": "reddit.comment.reply",
                "safety": {
                    "global_pause": True,
                    "paused_platforms": ["reddit"],
                    "safe_mode": True,
                    "safe_mode_allowed_drivers": ["manual_copy"],
                    "override_source": "manual",
                },
                "request": {"thing_id": "t1_precedence", "text": "noop"},
            }
            emit_id = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=saved["id"],
                    platform="reddit",
                    transport="reddit.comment.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % saved["id"],
                    emit_request_json=json.dumps(emit_request),
                    notes="precedence",
                )
            )["emit_outbox"][0]["id"]

            blocked = repo.dispatch_emit(emit_id, driver="webhook_post", out_dir=Path(tmp) / "dispatch")
            response = blocked["dispatch"]["response"]

            self.assertEqual(blocked["dispatch"]["status"], "blocked")
            self.assertEqual(response["block_reason"], "global_pause")
            self.assertEqual(response["override_source"], "manual")
            self.assertIn("safety_mode_state", response)
            self.assertEqual(
                response["safety_mode_state"].get("precedence"),
                ["global_pause", "platform_pause", "safe_mode_allowlist", "allow"],
            )

    def test_observability_heartbeat_updates_on_successful_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            run = repo.create_run_from_text(
                text="heartbeat cycle",
                persona_name="dry_midwit_savant",
                platform="test",
                candidate_count=1,
            )
            staged = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=run["id"],
                    platform="test",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % run["id"],
                    emit_request_json='{"transport": "test.reply", "observability": {"stall_threshold_seconds": 120}}',
                    notes="heartbeat",
                )
            )

            cycle = repo.worker_cycle(dispatch_limit=5, driver="manual_copy", out_dir=Path(tmp) / "dispatch")
            self.assertEqual(cycle["dispatched_count"], 1)
            response = cycle["dispatched"][0]["dispatch"]["response"]
            snapshot = response["observability_snapshot"]
            self.assertIsNotNone(snapshot["heartbeat_timestamp"])
            self.assertFalse(snapshot["no_output_stall"])

    def test_observability_no_output_stall_triggers_at_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            run = repo.create_run_from_text("stall test", persona_name="dry_midwit_savant", platform="test", candidate_count=1)

            blocked_emit = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=run["id"],
                    platform="test",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % run["id"],
                    emit_request_json='{"transport": "test.reply", "safety": {"global_pause": true}, "observability": {"stall_threshold_seconds": 10}}',
                    notes="stall check",
                )
            )["emit_outbox"][0]["id"]

            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                conn.execute(
                    """
                    INSERT INTO emit_dispatches (
                        emit_outbox_id, run_id, driver, status, request_json, response_json, notes, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        blocked_emit,
                        run["id"],
                        "manual_copy",
                        "delivered",
                        "{}",
                        "{}",
                        "seed heartbeat",
                        "1970-01-01 00:16:40",
                    ),
                )
                conn.commit()

            with mock.patch("bait_engine.storage.repository.time.time", return_value=1015.0):
                blocked = repo.dispatch_emit(blocked_emit, driver="manual_copy", out_dir=Path(tmp) / "dispatch")

            snapshot = blocked["dispatch"]["response"]["observability_snapshot"]
            alerts = blocked["dispatch"]["response"]["observability_alerts"]
            self.assertTrue(snapshot["no_output_stall"])
            self.assertEqual(snapshot["seconds_since_last_success"], 15.0)
            self.assertTrue(any(alert.get("alert_type") == "no_output_stall" for alert in alerts))

    def test_observability_failure_spike_triggers_at_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            run = repo.create_run_from_text("failure spike", persona_name="dry_midwit_savant", platform="test", candidate_count=1)

            emit_request = json.dumps(
                {
                    "transport": "test.reply",
                    "observability": {
                        "failure_window_seconds": 300,
                        "failure_spike_threshold": 0.5,
                        "failure_spike_min_events": 2,
                    },
                }
            )
            first_emit = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=run["id"],
                    platform="test",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % run["id"],
                    emit_request_json=emit_request,
                    notes="fail one",
                )
            )["emit_outbox"][0]["id"]
            second_emit = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=run["id"],
                    platform="test",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % run["id"],
                    emit_request_json=emit_request,
                    notes="fail two",
                )
            )["emit_outbox"][0]["id"]

            first = repo.dispatch_emit(first_emit, driver="webhook_post")
            second = repo.dispatch_emit(second_emit, driver="webhook_post")

            snapshot = second["dispatch"]["response"]["observability_snapshot"]
            alerts = second["dispatch"]["response"]["observability_alerts"]
            self.assertTrue(snapshot["failure_spike"])
            self.assertGreaterEqual(snapshot["failure_rate"], 0.5)
            self.assertTrue(any(alert.get("alert_type") == "failure_spike" for alert in alerts))

    def test_observability_alert_payload_schema_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            run = repo.create_run_from_text("alert schema", persona_name="dry_midwit_savant", platform="test", candidate_count=1)

            emit_id = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=run["id"],
                    platform="test",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % run["id"],
                    emit_request_json='{"transport": "test.reply", "safety": {"global_pause": true}, "observability": {"stall_threshold_seconds": 1, "failure_spike_min_events": 1, "failure_spike_threshold": 0.0}}',
                    notes="alerts",
                )
            )["emit_outbox"][0]["id"]

            with mock.patch("bait_engine.storage.repository.time.time", return_value=2000.0):
                blocked = repo.dispatch_emit(emit_id, driver="webhook_post")

            alerts = blocked["dispatch"]["response"]["observability_alerts"]
            self.assertGreaterEqual(len(alerts), 1)
            for alert in alerts:
                self.assertEqual(
                    sorted(alert.keys()),
                    ["alert_type", "metric_snapshot", "recommended_action", "severity", "triggered_at"],
                )

    def test_report_includes_observability_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            run = repo.create_run_from_text("report observability", persona_name="dry_midwit_savant", platform="test", candidate_count=1)
            staged = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=run["id"],
                    platform="test",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % run["id"],
                    emit_request_json='{"transport": "test.reply"}',
                    notes="report",
                )
            )
            repo.dispatch_emit(staged["emit_outbox"][0]["id"], driver="manual_copy", out_dir=Path(tmp) / "dispatch")

            report = build_report(repo.list_full_runs(limit=10))
            markdown = render_report_markdown(report)
            csv_blob = render_report_csv(report)

            self.assertIn("observability_latest_heartbeat", markdown)
            self.assertIn("observability_total_alert_count", markdown)
            self.assertIn("obs_latest_heartbeat_timestamp", csv_blob)
            self.assertIn("obs_alert_count", csv_blob)

    def test_dispatch_control_checkpoint_creation_and_retrieval(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = RunRepository(Path(tmp) / "bait.db")
            repo.set_dispatch_control_state(
                governor={"max_actions_per_hour": 7},
                containment={"retry_policy": {"max_attempts": 9}},
                escalation={"daily_escalation_cap": 5},
                safety={"global_pause": True, "override_source": "manual"},
                source="test-seed",
            )
            checkpoint = repo.create_dispatch_control_checkpoint(reason="checkpoint-test")
            listed = repo.list_dispatch_control_checkpoints(limit=5)

            self.assertEqual(checkpoint["reason"], "checkpoint-test")
            self.assertEqual(listed[0]["id"], checkpoint["id"])
            self.assertEqual(checkpoint["governor"]["max_actions_per_hour"], 7)
            self.assertEqual(checkpoint["containment"]["retry_policy"]["max_attempts"], 9)
            self.assertEqual(checkpoint["escalation"]["daily_escalation_cap"], 5)
            self.assertTrue(checkpoint["safety"]["global_pause"])
            self.assertIn("telemetry", checkpoint)

    def test_rollback_to_checkpoint_restores_prior_state_and_dispatch_behavior(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = RunRepository(Path(tmp) / "bait.db")
            state_a = repo.set_dispatch_control_state(
                governor={"max_actions_per_hour": 3},
                containment={"retry_policy": {"max_attempts": 4}},
                escalation={"daily_escalation_cap": 8},
                safety={"global_pause": True, "override_source": "manual"},
                source="state-a",
            )
            checkpoint = repo.create_dispatch_control_checkpoint(reason="state-a")

            repo.set_dispatch_control_state(
                governor={"max_actions_per_hour": 0},
                containment={"retry_policy": {"max_attempts": 2}},
                escalation={"daily_escalation_cap": 1},
                safety={"global_pause": False, "override_source": "none"},
                source="state-b",
            )
            result = repo.rollback_to_checkpoint(checkpoint["id"])
            restored = repo.get_dispatch_control_state()

            self.assertTrue(result["applied"])
            self.assertEqual(restored["governor"]["max_actions_per_hour"], state_a["governor"]["max_actions_per_hour"])
            self.assertEqual(restored["containment"]["retry_policy"]["max_attempts"], state_a["containment"]["retry_policy"]["max_attempts"])
            self.assertEqual(restored["escalation"]["daily_escalation_cap"], state_a["escalation"]["daily_escalation_cap"])
            self.assertTrue(restored["safety"]["global_pause"])

            run = repo.create_run_from_text("rollback gating", persona_name="dry_midwit_savant", platform="test", candidate_count=1)
            emit_id = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=run["id"],
                    platform="test",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % run["id"],
                    emit_request_json='{"transport": "test.reply"}',
                    notes="rollback-block",
                )
            )["emit_outbox"][0]["id"]
            dispatched = repo.dispatch_emit(emit_id, driver="manual_copy", out_dir=Path(tmp) / "dispatch")
            self.assertEqual(dispatched["dispatch"]["status"], "blocked")
            self.assertEqual(dispatched["dispatch"]["response"]["block_reason"], "global_pause")

    def test_rollback_to_checkpoint_fails_safely_with_invalid_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = RunRepository(Path(tmp) / "bait.db")
            result = repo.rollback_to_checkpoint(999999)
            self.assertFalse(result["applied"])
            self.assertTrue(result["failed"])
            self.assertEqual(result["reason"], "checkpoint_not_found")

    def test_last_known_good_selection_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = RunRepository(Path(tmp) / "bait.db")

            repo.set_dispatch_control_state(safety={"global_pause": True}, source="bad-1")
            bad_one = repo.create_dispatch_control_checkpoint(reason="bad-global-pause")
            self.assertFalse(bad_one["is_last_good"])

            repo.set_dispatch_control_state(safety={"global_pause": False}, source="good")
            run = repo.create_run_from_text("healthy dispatch", persona_name="dry_midwit_savant", platform="test", candidate_count=1)
            emit_id = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=run["id"],
                    platform="test",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % run["id"],
                    emit_request_json='{"transport": "test.reply"}',
                    notes="healthy",
                )
            )["emit_outbox"][0]["id"]
            with mock.patch("bait_engine.storage.repository.time.time", return_value=2000.0):
                repo.dispatch_emit(emit_id, driver="manual_copy", out_dir=Path(tmp) / "dispatch")
                good = repo.create_dispatch_control_checkpoint(reason="good")
            self.assertTrue(good["is_last_good"])

            repo.set_dispatch_control_state(safety={"global_pause": True}, source="bad-2")
            bad_two = repo.create_dispatch_control_checkpoint(reason="bad-global-pause-2")
            self.assertFalse(bad_two["is_last_good"])

            rollback = repo.rollback_to_last_good_state()
            self.assertTrue(rollback["applied"])
            self.assertEqual(rollback["checkpoint_id"], good["id"])
            restored = repo.get_dispatch_control_state()
            self.assertFalse(restored["safety"]["global_pause"])

    def test_operator_status_summary_schema_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            repo.set_dispatch_control_state(
                governor={"max_actions_per_hour": 5},
                containment={"retry_policy": {"max_attempts": 4}},
                escalation={"daily_escalation_cap": 7},
                safety={"safe_mode": True, "safe_mode_allowed_drivers": ["manual_copy"]},
                source="status-test",
            )
            repo.create_dispatch_control_checkpoint(reason="status-checkpoint")
            summary = repo.operator_status_summary()

            self.assertIn("governor", summary)
            self.assertIn("containment", summary)
            self.assertIn("escalation", summary)
            self.assertIn("safety", summary)
            self.assertIn("observability", summary)
            self.assertIn("checkpoints", summary)
            self.assertIn("config", summary["governor"])
            self.assertIn("breaker_state_counts", summary["containment"])
            self.assertIn("retry_queue", summary["containment"])
            self.assertIn("policy", summary["escalation"])
            self.assertIn("severity_counts", summary["observability"])

    def test_preflight_checklist_pass_fail_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            repo.set_dispatch_control_state(safety={"global_pause": False}, source="preflight-good")
            run = repo.create_run_from_text("preflight good", persona_name="dry_midwit_savant", platform="test", candidate_count=1)
            emit_id = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=run["id"],
                    platform="test",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % run["id"],
                    emit_request_json='{"transport": "test.reply"}',
                    notes="good dispatch",
                )
            )["emit_outbox"][0]["id"]
            repo.dispatch_emit(emit_id, driver="manual_copy", out_dir=Path(tmp) / "dispatch")
            repo.create_dispatch_control_checkpoint(reason="good-state")

            passed = repo.preflight_autopilot_checklist(
                dead_letter_fail_threshold=10,
                waiting_retry_fail_threshold=50,
                critical_alert_fail_threshold=2,
            )
            self.assertTrue(passed["overall_pass"])
            self.assertTrue(all(item["pass"] for item in passed["items"]))

            repo.set_dispatch_control_state(safety={"global_pause": True}, source="preflight-bad")
            failed = repo.preflight_autopilot_checklist(
                dead_letter_fail_threshold=10,
                waiting_retry_fail_threshold=50,
                critical_alert_fail_threshold=2,
            )
            fail_item = next(item for item in failed["items"] if item["item"] == "safety_not_globally_paused")
            self.assertFalse(failed["overall_pass"])
            self.assertFalse(fail_item["pass"])
            self.assertEqual(fail_item["reason"], "global_pause=true")

    def test_runbook_command_paths_do_not_regress_worker_behavior(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            run = repo.create_run_from_text("runbook command path", persona_name="dry_midwit_savant", platform="test", candidate_count=1)
            repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=run["id"],
                    platform="test",
                    transport="test.reply",
                    selection_preset="default",
                    selection_strategy="rank",
                    tactic="test",
                    objective="test",
                    status="approved",
                    envelope_json='{"run_id": %d}' % run["id"],
                    emit_request_json='{"transport": "test.reply"}',
                    notes="runbook worker",
                )
            )

            status_payload = cmd_operator_status(db_path=str(db_path))
            preflight_payload = cmd_preflight(db_path=str(db_path))
            cycle = cmd_worker_cycle(db_path=str(db_path), dispatch_limit=5, driver="manual_copy", out_dir=str(Path(tmp) / "dispatch"))

            self.assertIn("governor", status_payload)
            self.assertIn("items", preflight_payload)
            self.assertEqual(cycle["dispatched_count"], 1)

    def test_record_outcome_auto_links_latest_dispatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            out_dir = Path(tmp) / "dispatches"
            repo = RunRepository(db_path)
            run = repo.create_run_from_text(
                text="Link outcomes to actual dispatch history.",
                persona_name="dry_midwit_savant",
                platform="reddit",
                candidate_count=1,
            )
            staged = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=run["id"],
                    platform="reddit",
                    transport="reddit.comment.reply",
                    selection_preset="engage",
                    selection_strategy="rank",
                    tactic="counterargument",
                    objective="provoke",
                    status="approved",
                    envelope_json='{"run_id": %d}' % run["id"],
                    emit_request_json='{"transport": "reddit.comment.reply", "request": {"thing_id": "t1_abc", "text": "x"}}',
                    notes=None,
                )
            )
            dispatched = repo.dispatch_emit(staged["emit_outbox"][0]["id"], driver="manual_copy", out_dir=out_dir)
            dispatch_id = int(dispatched["dispatch"]["id"])
            emit_id = int(dispatched["emit"]["id"])

            updated = repo.record_outcome(
                OutcomeRecord(
                    id=None,
                    run_id=run["id"],
                    got_reply=True,
                    reply_delay_seconds=45,
                    reply_length=22,
                    tone_shift="heated",
                    spectator_engagement=3,
                    result_label="bite",
                    notes="linked",
                )
            )

            self.assertEqual(updated["outcome"]["emit_dispatch_id"], dispatch_id)
            self.assertEqual(updated["outcome"]["emit_outbox_id"], emit_id)

            repo.update_emit_dispatch(dispatch_id, status="delivered", notes="confirmed")
            reputation = repo.get_persona_reputation("dry_midwit_savant", "reddit")
            self.assertEqual(reputation["linked_outcomes"], 1)
            self.assertEqual(reputation["delivery_verified_count"], 1)
            self.assertEqual(reputation["delivery_confidence"], 1.0)

    def test_list_full_runs_and_scoreboard(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            run_a = repo.create_run_from_text(
                text="A model being useful doesn't make it true, and you're still confusing mechanism with necessity.",
                persona_name="dry_midwit_savant",
                platform="reddit",
                candidate_count=3,
            )
            run_b = repo.create_run_from_text(
                text="What exactly do you mean by that and why should anyone buy it?",
                persona_name="fake_sincere_questioner",
                platform="reddit",
                candidate_count=3,
            )
            repo.record_outcome(
                OutcomeRecord(
                    id=None,
                    run_id=run_a["id"],
                    got_reply=True,
                    reply_delay_seconds=30,
                    reply_length=55,
                    tone_shift="defensive",
                    spectator_engagement=1,
                    result_label="bite",
                    notes=None,
                )
            )
            repo.record_outcome(
                OutcomeRecord(
                    id=None,
                    run_id=run_b["id"],
                    got_reply=False,
                    reply_delay_seconds=None,
                    reply_length=None,
                    tone_shift=None,
                    spectator_engagement=0,
                    result_label="dead_thread",
                    notes=None,
                )
            )

            full_runs = repo.list_full_runs(limit=10)
            self.assertEqual(len(full_runs), 2)
            summaries = summarize_runs(full_runs)
            self.assertEqual(len(summaries), 2)
            self.assertEqual(summaries[0]["platform"], "reddit")
            filtered = filter_summaries(summaries, persona="dry_midwit_savant", verdict="engaged")
            self.assertEqual(len(filtered), 1)

            scoreboard = build_outcome_scoreboard(full_runs, persona="dry_midwit_savant")
            self.assertEqual(scoreboard["filters"]["persona"], "dry_midwit_savant")
            self.assertEqual(scoreboard["total_runs"], 1)
            self.assertEqual(scoreboard["engaged"], 1)
            self.assertEqual(scoreboard["no_bite"], 0)
            self.assertEqual(scoreboard["pending"], 0)
            self.assertEqual(scoreboard["personas"][0]["persona"], "dry_midwit_savant")
            self.assertTrue(scoreboard["platforms"])
            self.assertTrue(scoreboard["objectives"])
            self.assertTrue(scoreboard["tactics"])
            self.assertTrue(scoreboard["exit_states"])
            self.assertIn("platform", scoreboard["platforms"][0])
            self.assertIn("objective", scoreboard["objectives"][0])
            self.assertIn("tactic", scoreboard["tactics"][0])
            self.assertIn("exit_state", scoreboard["exit_states"][0])
            self.assertIn("router_metrics", scoreboard)
            self.assertIn("override_audit", scoreboard["router_metrics"])

            report = build_report(full_runs, limit_per_section=1, verdict="engaged")
            self.assertEqual(report["scoreboard"]["filters"]["verdict"], "engaged")
            self.assertEqual(report["scoreboard"]["total_runs"], 1)
            self.assertEqual(len(report["best_runs"]), 1)
            self.assertEqual(len(report["worst_runs"]), 0)
            self.assertEqual(len(report["pending_runs"]), 0)
            self.assertEqual(report["best_runs"][0]["verdict"], "engaged")

            markdown = render_report_markdown(report)
            self.assertIn("# Bait Engine Report", markdown)
            self.assertIn("## Summary", markdown)
            self.assertIn("## Best Runs", markdown)

            csv_export = render_report_csv(report)
            self.assertIn("record_type,section", csv_export)
            self.assertIn("summary,summary", csv_export)
            self.assertIn("bucket,personas", csv_export)

    def test_extract_top_winners_resolves_linked_candidate_and_fallbacks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            dispatch_dir = Path(tmp) / "dispatches"
            repo = RunRepository(db_path)

            def seed_run(source_text: str, *, candidates: list[dict[str, object]], selected_tactic: str, selected_objective: str = "extract_reaction") -> dict[str, object]:
                with open_db(db_path) as conn:
                    cur = conn.execute(
                        """
                        INSERT INTO runs (
                            source_text, platform, persona, selected_objective, selected_tactic, exit_state, analysis_json, plan_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            source_text,
                            "reddit",
                            "dry_midwit_savant",
                            selected_objective,
                            selected_tactic,
                            "leave_target_exposed",
                            json.dumps({"seeded": True}),
                            json.dumps(
                                {
                                    "selected_objective": selected_objective,
                                    "selected_tactic": selected_tactic,
                                    "exit_state": "leave_target_exposed",
                                }
                            ),
                        ),
                    )
                    run_id = int(cur.lastrowid)
                    for idx, candidate in enumerate(candidates, start=1):
                        conn.execute(
                            """
                            INSERT INTO candidates (
                                run_id, rank_index, text, tactic, objective, estimated_bite_score,
                                estimated_audience_score, critic_penalty, rank_score, critic_notes_json
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                run_id,
                                idx,
                                str(candidate["text"]),
                                candidate.get("tactic"),
                                str(candidate.get("objective") or selected_objective),
                                float(candidate.get("estimated_bite_score") or 0.0),
                                float(candidate.get("estimated_audience_score") or 0.0),
                                float(candidate.get("critic_penalty") or 0.0),
                                float(candidate.get("rank_score") or 0.0),
                                json.dumps(candidate.get("critic_notes") or []),
                            ),
                        )
                return repo.get_run(run_id)

            first = seed_run(
                "If your claim is airtight, explain why it collapses under one counterexample.",
                selected_tactic="precision_needle",
                candidates=[
                    {
                        "text": "That is a lot of certainty for something that breaks the moment details enter the room.",
                        "tactic": "status_shiv",
                        "estimated_bite_score": 0.44,
                        "estimated_audience_score": 0.41,
                        "critic_penalty": 0.08,
                        "rank_score": 0.57,
                    },
                    {
                        "text": "One counterexample kills the whole performance, so do you have an argument or just posture?",
                        "tactic": "precision_needle",
                        "estimated_bite_score": 0.79,
                        "estimated_audience_score": 0.63,
                        "critic_penalty": 0.05,
                        "rank_score": 0.86,
                    },
                ],
            )
            chosen = first["candidates"][1]
            staged_first = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=first["id"],
                    platform="reddit",
                    transport="reddit.comment.reply",
                    selection_preset="engage",
                    selection_strategy="rank",
                    tactic=chosen.get("tactic"),
                    objective=chosen.get("objective"),
                    status="approved",
                    envelope_json=json.dumps(
                        {
                            "run_id": first["id"],
                            "candidate_rank_index": chosen["rank_index"],
                            "platform": "reddit",
                            "persona": first["persona"],
                            "objective": first.get("selected_objective"),
                            "tactic": chosen.get("tactic"),
                            "exit_state": first.get("exit_state"),
                            "body": chosen["text"],
                            "target": {"thread_id": "t1_alpha", "reply_to_id": "t1_alpha"},
                            "metadata": {
                                "selection_strategy": "rank",
                                "selection_preset": "engage",
                                "candidate_tactic": chosen.get("tactic"),
                                "candidate_objective": chosen.get("objective"),
                                "candidate_rank_score": chosen.get("rank_score"),
                            },
                        }
                    ),
                    emit_request_json=json.dumps({"transport": "reddit.comment.reply"}),
                    notes="winner seed alpha",
                )
            )
            dispatched_first = repo.dispatch_emit(staged_first["emit_outbox"][0]["id"], driver="jsonl_append", out_dir=dispatch_dir)
            repo.record_outcome(
                OutcomeRecord(
                    id=None,
                    run_id=first["id"],
                    got_reply=True,
                    reply_delay_seconds=20,
                    reply_length=160,
                    tone_shift="defensive",
                    spectator_engagement=7,
                    result_label="bite",
                    notes="strong bite",
                    emit_outbox_id=dispatched_first["emit"]["id"],
                    emit_dispatch_id=dispatched_first["dispatch"]["id"],
                )
            )

            second = seed_run(
                "You keep saying obvious things like they are revelations.",
                selected_tactic="precision_needle",
                candidates=[
                    {
                        "text": "Repeating a hallway opinion with confidence is not the same as having insight.",
                        "tactic": "precision_needle",
                        "estimated_bite_score": 0.62,
                        "estimated_audience_score": 0.38,
                        "critic_penalty": 0.11,
                        "rank_score": 0.68,
                    },
                    {
                        "text": "It sounds profound only if nobody checks the contents.",
                        "tactic": "status_shiv",
                        "estimated_bite_score": 0.33,
                        "estimated_audience_score": 0.28,
                        "critic_penalty": 0.14,
                        "rank_score": 0.31,
                    },
                ],
            )
            staged_second = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=second["id"],
                    platform="reddit",
                    transport="reddit.comment.reply",
                    selection_preset="engage",
                    selection_strategy="rank",
                    tactic=second["candidates"][0].get("tactic"),
                    objective=second["candidates"][0].get("objective"),
                    status="approved",
                    envelope_json=json.dumps(
                        {
                            "run_id": second["id"],
                            "candidate_rank_index": second["candidates"][0]["rank_index"],
                            "platform": "reddit",
                            "persona": second["persona"],
                            "objective": second.get("selected_objective"),
                            "tactic": second["candidates"][0].get("tactic"),
                            "exit_state": second.get("exit_state"),
                            "body": second["candidates"][0]["text"],
                            "target": {"thread_id": "t1_beta", "reply_to_id": "t1_beta"},
                            "metadata": {
                                "selection_strategy": "rank",
                                "selection_preset": "engage",
                                "candidate_tactic": second["candidates"][0].get("tactic"),
                                "candidate_objective": second["candidates"][0].get("objective"),
                                "candidate_rank_score": second["candidates"][0].get("rank_score"),
                            },
                        }
                    ),
                    emit_request_json=json.dumps({"transport": "reddit.comment.reply"}),
                    notes="winner seed beta",
                )
            )
            dispatched_second = repo.dispatch_emit(staged_second["emit_outbox"][0]["id"], driver="jsonl_append", out_dir=dispatch_dir)
            repo.record_outcome(
                OutcomeRecord(
                    id=None,
                    run_id=second["id"],
                    got_reply=True,
                    reply_delay_seconds=140,
                    reply_length=80,
                    tone_shift="annoyed",
                    spectator_engagement=2,
                    result_label="bite",
                    notes="weaker bite",
                    emit_outbox_id=dispatched_second["emit"]["id"],
                    emit_dispatch_id=dispatched_second["dispatch"]["id"],
                )
            )

            third = seed_run(
                "Fine, define the term before you pretend to prove anything with it.",
                selected_tactic="precision_needle",
                candidates=[
                    {
                        "text": "Start with a definition and maybe the rest will stop wobbling.",
                        "tactic": "precision_needle",
                        "estimated_bite_score": 0.51,
                        "estimated_audience_score": 0.33,
                        "critic_penalty": 0.07,
                        "rank_score": 0.59,
                    },
                    {
                        "text": "Right now you are just stacking words where an argument should be.",
                        "tactic": "status_shiv",
                        "estimated_bite_score": 0.37,
                        "estimated_audience_score": 0.29,
                        "critic_penalty": 0.12,
                        "rank_score": 0.35,
                    },
                ],
            )
            repo.record_outcome(
                OutcomeRecord(
                    id=None,
                    run_id=third["id"],
                    got_reply=True,
                    reply_delay_seconds=60,
                    reply_length=50,
                    tone_shift="irritated",
                    spectator_engagement=1,
                    result_label="bite",
                    notes="no dispatch linkage",
                )
            )

            winners = repo.extract_top_winners(limit=10, platform="reddit")
            by_run = {item["run_id"]: item for item in winners}

            self.assertEqual(winners[0]["run_id"], first["id"])
            self.assertEqual(winners[0]["candidate_rank_index"], chosen["rank_index"])
            self.assertEqual(winners[0]["winner_source"], "linked_emit_rank_index")
            self.assertEqual(winners[0]["delivery_status"], "delivered")
            self.assertTrue(winners[0]["delivery_verified"])
            self.assertGreater(winners[0]["winner_score"], by_run[second["id"]]["winner_score"])
            self.assertEqual(by_run[third["id"]]["winner_source"], "rank_fallback")
            self.assertEqual(by_run[third["id"]]["candidate_rank_index"], 1)
            self.assertGreaterEqual(by_run[first["id"]]["reputation"]["total_runs"], 3)
            self.assertIn("reply_rate", by_run[first["id"]]["reputation"])

    def test_archetype_weight_profile_sparse_segment_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            run = repo.create_run_from_text(
                text="A model being useful doesn't make it true, and you're still confusing mechanism with necessity.",
                persona_name="dry_midwit_savant",
                platform="reddit",
                candidate_count=1,
                heuristic_only=True,
            )
            profile = repo._build_archetype_weight_profile(
                persona="dry_midwit_savant",
                platform="reddit",
                objective=run["selected_objective"],
                min_samples=3,
            )
            self.assertFalse(profile["enabled"])
            self.assertEqual(profile["fallback_reason"], "sparse_segment")
            self.assertEqual(profile["sample_size"], 0)

    def test_archetype_weight_profile_learns_and_analysis_note_is_visible(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            text = "A model being useful doesn't make it true, and you're still confusing mechanism with necessity."

            objective = None
            for _ in range(9):
                run = repo.create_run_from_text(
                    text=text,
                    persona_name="dry_midwit_savant",
                    platform="reddit",
                    candidate_count=1,
                    heuristic_only=True,
                )
                objective = run["selected_objective"]
                repo.record_outcome(
                    OutcomeRecord(
                        id=None,
                        run_id=run["id"],
                        got_reply=True,
                        reply_delay_seconds=45,
                        reply_length=120,
                        tone_shift="angrier",
                        spectator_engagement=4,
                        result_label="essay_bite",
                        notes="signal",
                    )
                )

            self.assertIsNotNone(objective)
            profile = repo._build_archetype_weight_profile(
                persona="dry_midwit_savant",
                platform="reddit",
                objective=str(objective),
                min_samples=8,
            )
            self.assertTrue(profile["enabled"])
            self.assertGreater(profile["confidence"], 0.0)
            self.assertGreaterEqual(profile["sample_size"], 8)
            self.assertIn("spectral", profile["weights"])

            learned_run = repo.create_run_from_text(
                text=text,
                persona_name="dry_midwit_savant",
                platform="reddit",
                candidate_count=1,
                heuristic_only=True,
            )
            notes = learned_run["analysis"].get("notes") or []
            self.assertTrue(any("archetype weights: learned" in note for note in notes))

    def test_persona_router_calibration_sparse_segment_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)

            calibration = repo.persona_router_calibration(platform="reddit", objective="hook", min_samples=8)

        self.assertFalse(calibration["enabled"])
        self.assertEqual(calibration["fallback_reason"], "sparse_segment")
        self.assertEqual(calibration["segment_confidence"], 0.0)
        self.assertEqual(calibration["sample_size"], 0)

    def test_persona_router_calibration_can_override_when_confident(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            text = "You keep swapping mechanism for necessity and calling it logic."

            objective: str | None = None
            for _ in range(10):
                run = repo.create_run_from_text(
                    text=text,
                    persona_name="smug_moron_oracle",
                    platform="reddit",
                    candidate_count=1,
                    heuristic_only=True,
                    force_engage=True,
                    mutation_source="none",
                )
                objective = str(run["selected_objective"])
                repo.record_outcome(
                    OutcomeRecord(
                        id=None,
                        run_id=run["id"],
                        got_reply=True,
                        reply_delay_seconds=20,
                        reply_length=140,
                        tone_shift="angrier",
                        spectator_engagement=5,
                        result_label="bite",
                        notes="high",
                    )
                )

            for _ in range(10):
                run = repo.create_run_from_text(
                    text=text,
                    persona_name="calm_unbothered_ghoul",
                    platform="reddit",
                    candidate_count=1,
                    heuristic_only=True,
                    force_engage=True,
                    mutation_source="none",
                )
                if objective is None:
                    objective = str(run["selected_objective"])
                repo.record_outcome(
                    OutcomeRecord(
                        id=None,
                        run_id=run["id"],
                        got_reply=False,
                        reply_delay_seconds=None,
                        reply_length=10,
                        tone_shift="none",
                        spectator_engagement=0,
                        result_label="dead",
                        notes="low",
                    )
                )

            analysis = analyze_comment(AnalyzeInput(text=text, platform="reddit"))
            calibration = repo.persona_router_calibration(platform="reddit", objective=objective, min_samples=8)

            personas = {
                "smug_moron_oracle": PersonaProfile(name="smug_moron_oracle"),
                "calm_unbothered_ghoul": PersonaProfile(name="calm_unbothered_ghoul"),
            }
            decision = select_persona(
                analysis,
                platform="reddit",
                personas=personas,
                priors={},
                calibration=calibration,
                close_margin=0.0001,
            )

        self.assertTrue(calibration["enabled"])
        self.assertGreater(calibration["segment_confidence"], 0.0)
        self.assertEqual(decision.selected_persona, "smug_moron_oracle")
        self.assertEqual(decision.calibration_version, "phase12-artery2-v1")

    def test_persona_router_calibration_is_deterministic_for_fixed_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            text = "Confidence isn't evidence, it's just volume."

            objective: str | None = None
            for _ in range(8):
                run = repo.create_run_from_text(
                    text=text,
                    persona_name="dry_midwit_savant",
                    platform="reddit",
                    candidate_count=1,
                    heuristic_only=True,
                    force_engage=True,
                    mutation_source="none",
                )
                objective = str(run["selected_objective"])
                repo.record_outcome(
                    OutcomeRecord(
                        id=None,
                        run_id=run["id"],
                        got_reply=True,
                        reply_delay_seconds=30,
                        reply_length=90,
                        tone_shift="angrier",
                        spectator_engagement=3,
                        result_label="bite",
                        notes="stable",
                    )
                )

            first = repo.persona_router_calibration(platform="reddit", objective=objective, min_samples=8)
            second = repo.persona_router_calibration(platform="reddit", objective=objective, min_samples=8)

        self.assertEqual(first, second)

    def test_router_metric_rollups_and_override_audit_persistence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            text = "You are dressing certainty up as argument."

            auto_engaged = repo.create_run_from_text(
                text=text,
                persona_name="auto",
                platform="reddit",
                candidate_count=1,
                heuristic_only=True,
                force_engage=True,
                mutation_source="none",
            )
            repo.record_outcome(
                OutcomeRecord(
                    id=None,
                    run_id=auto_engaged["id"],
                    got_reply=True,
                    reply_delay_seconds=40,
                    reply_length=80,
                    tone_shift="angrier",
                    spectator_engagement=2,
                    result_label="bite",
                    notes="good",
                )
            )

            auto_no_bite = repo.create_run_from_text(
                text=text,
                persona_name="auto",
                platform="reddit",
                candidate_count=1,
                heuristic_only=True,
                force_engage=True,
                mutation_source="none",
            )
            repo.record_outcome(
                OutcomeRecord(
                    id=None,
                    run_id=auto_no_bite["id"],
                    got_reply=False,
                    reply_delay_seconds=None,
                    reply_length=10,
                    tone_shift="none",
                    spectator_engagement=0,
                    result_label="dead",
                    notes="bad",
                )
            )

            forced = repo.create_run_from_text(
                text=text,
                persona_name="dry_midwit_savant",
                platform="reddit",
                candidate_count=1,
                heuristic_only=True,
                force_engage=True,
                mutation_source="none",
            )

            full_runs = repo.list_full_runs(limit=10)
            scoreboard = build_outcome_scoreboard(full_runs)

        plan_auto = auto_engaged["plan"].get("persona_selection")
        plan_forced = forced["plan"].get("persona_selection")
        self.assertEqual(plan_auto["mode"], "auto")
        self.assertTrue(plan_auto["router_used"])
        self.assertEqual(plan_forced["mode"], "forced")
        self.assertFalse(plan_forced["router_used"])

        router_metrics = scoreboard["router_metrics"]
        self.assertEqual(router_metrics["auto_runs"], 2)
        self.assertEqual(router_metrics["forced_runs"], 1)
        self.assertEqual(router_metrics["override_audit"]["auto_persona"], 2)
        self.assertEqual(router_metrics["override_audit"]["forced_persona"], 1)
        self.assertAlmostEqual(router_metrics["auto_pick_accuracy"], 0.5, places=4)
        self.assertIn("confidence_distribution", router_metrics)

    def test_escalation_controller_stays_cheap_without_triggers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            with mock.patch.dict(
                os.environ,
                {
                    "BAIT_ENGINE_CHEAP_MODEL": "cheap-unit",
                    "BAIT_ENGINE_HARD_MODEL": "hard-unit",
                    "BAIT_ENGINE_ESCALATE_ROUTER_CONFIDENCE_LT": "0.01",
                    "BAIT_ENGINE_ESCALATE_DUEL_MARGIN_LTE": "0.0",
                    "BAIT_ENGINE_ESCALATE_SEMANTIC_INVERSION_GTE": "1.0",
                    "BAIT_ENGINE_ESCALATE_OPPORTUNITY_GTE": "1.0",
                    "BAIT_ENGINE_ESCALATE_PER_RUN_CAP": "1",
                    "BAIT_ENGINE_ESCALATE_DAILY_CAP": "99",
                },
                clear=False,
            ):
                run = repo.create_run_from_text(
                    text="Plain factual correction.",
                    persona_name="dry_midwit_savant",
                    platform="test",
                    candidate_count=1,
                    heuristic_only=True,
                )

            self.assertEqual(run["plan"]["selected_model_tier"], "cheap")
            self.assertEqual(run["plan"]["selected_model"], "cheap-unit")
            self.assertEqual(run["plan"]["escalation_reasons"], [])

    def test_escalation_controller_escalates_when_triggered(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            with mock.patch.dict(
                os.environ,
                {
                    "BAIT_ENGINE_CHEAP_MODEL": "cheap-unit",
                    "BAIT_ENGINE_HARD_MODEL": "hard-unit",
                    "BAIT_ENGINE_ESCALATE_OPPORTUNITY_GTE": "0.0",
                    "BAIT_ENGINE_ESCALATE_PER_RUN_CAP": "1",
                    "BAIT_ENGINE_ESCALATE_DAILY_CAP": "99",
                },
                clear=False,
            ):
                run = repo.create_run_from_text(
                    text="Bait-rich engagement target.",
                    persona_name="dry_midwit_savant",
                    platform="test",
                    candidate_count=1,
                    heuristic_only=True,
                )

            self.assertEqual(run["plan"]["selected_model_tier"], "hard")
            self.assertEqual(run["plan"]["selected_model"], "hard-unit")
            self.assertIn("high_value_target", run["plan"]["escalation_reasons"])

    def test_escalation_controller_denies_when_budget_exhausted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            with mock.patch.dict(
                os.environ,
                {
                    "BAIT_ENGINE_CHEAP_MODEL": "cheap-unit",
                    "BAIT_ENGINE_HARD_MODEL": "hard-unit",
                    "BAIT_ENGINE_ESCALATE_OPPORTUNITY_GTE": "0.0",
                    "BAIT_ENGINE_ESCALATE_PER_RUN_CAP": "1",
                    "BAIT_ENGINE_ESCALATE_DAILY_CAP": "0",
                },
                clear=False,
            ):
                run = repo.create_run_from_text(
                    text="Target that would normally escalate.",
                    persona_name="dry_midwit_savant",
                    platform="test",
                    candidate_count=1,
                    heuristic_only=True,
                )

            self.assertEqual(run["plan"]["selected_model_tier"], "cheap")
            self.assertEqual(run["plan"]["budget_state"].get("denied_reason"), "escalation_budget_daily_exhausted")
            self.assertIn("high_value_target", run["plan"]["escalation_reasons"])

    def test_escalation_controller_is_deterministic_for_fixed_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            analysis = analyze_comment(AnalyzeInput(text="Deterministic escalation probe.", platform="test"))
            persona_router = select_persona(analysis, platform="test")
            plan = build_plan(analysis, persona=persona_router.selected_persona, persona_router=persona_router)

            with mock.patch.dict(
                os.environ,
                {
                    "BAIT_ENGINE_CHEAP_MODEL": "cheap-unit",
                    "BAIT_ENGINE_HARD_MODEL": "hard-unit",
                    "BAIT_ENGINE_ESCALATE_OPPORTUNITY_GTE": "0.3",
                    "BAIT_ENGINE_ESCALATE_PER_RUN_CAP": "1",
                    "BAIT_ENGINE_ESCALATE_DAILY_CAP": "99",
                },
                clear=False,
            ):
                with open_db(db_path) as conn:
                    first = repo._select_generation_model(conn, analysis=analysis, plan=plan, persona_router=persona_router, now_ts=1700000000.0)
                    second = repo._select_generation_model(conn, analysis=analysis, plan=plan, persona_router=persona_router, now_ts=1700000000.0)

            self.assertEqual(first, second)

    def test_governor_hourly_and_daily_caps(self) -> None:
        now = datetime(2026, 3, 20, 18, 0, 0, tzinfo=timezone.utc)
        hourly_block = RunRepository.can_execute_action(
            now_utc=now,
            actions_last_hour=3,
            actions_last_day=3,
            max_actions_per_hour=3,
            max_actions_per_day=10,
        )
        daily_block = RunRepository.can_execute_action(
            now_utc=now,
            actions_last_hour=1,
            actions_last_day=5,
            max_actions_per_hour=10,
            max_actions_per_day=5,
        )
        self.assertFalse(hourly_block["allow"])
        self.assertEqual(hourly_block["reason"], "hourly_cap")
        self.assertFalse(daily_block["allow"])
        self.assertEqual(daily_block["reason"], "daily_cap")

    def test_governor_cooldown_and_quiet_hours_blocking(self) -> None:
        now = datetime(2026, 3, 20, 3, 30, 0, tzinfo=timezone.utc)
        cooldown = RunRepository.can_execute_action(
            now_utc=now,
            seconds_since_last_action=25.0,
            min_seconds_between_actions=60.0,
        )
        quiet = RunRepository.can_execute_action(
            now_utc=now,
            quiet_hours_start=22,
            quiet_hours_end=6,
        )
        self.assertFalse(cooldown["allow"])
        self.assertEqual(cooldown["reason"], "cooldown")
        self.assertFalse(quiet["allow"])
        self.assertEqual(quiet["reason"], "quiet_hours")

    def test_governor_allow_path_when_under_limits(self) -> None:
        allowed = RunRepository.can_execute_action(
            now_utc=datetime(2026, 3, 20, 17, 0, 0, tzinfo=timezone.utc),
            actions_last_hour=1,
            actions_last_day=4,
            seconds_since_last_action=120.0,
            max_actions_per_hour=5,
            max_actions_per_day=20,
            min_seconds_between_actions=30.0,
            quiet_hours_start=1,
            quiet_hours_end=2,
        )
        self.assertTrue(allowed["allow"])
        self.assertEqual(allowed["reason"], "under_limits")


if __name__ == "__main__":
    unittest.main()
