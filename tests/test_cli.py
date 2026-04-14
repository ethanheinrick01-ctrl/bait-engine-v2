from __future__ import annotations

from pathlib import Path
import os
import json
import sys
import tempfile
import threading
import unittest
from urllib.request import Request, urlopen
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bait_engine.cli.main import (
    cmd_adapter,
    cmd_adapter_preview,
    cmd_adapters,
    cmd_analyze,
    cmd_autopsy_many,
    cmd_context_preview,
    cmd_daemon_manage,
    cmd_dispatch_approved,
    cmd_dispatch_emit,
    cmd_draft,
    cmd_redrive_dispatch,
    cmd_update_dispatch_status,
    cmd_worker_cycle,
    cmd_worker_run,
    cmd_emit_preview,
    _create_panel_http_server,
    cmd_panel_preview,
    cmd_panel_serve,
    cmd_personas,
    cmd_outbox,
    cmd_recommend_preset,
    cmd_record_outcome,
    cmd_record_panel_review,
    cmd_replay,
    cmd_mutate_run,
    cmd_mutation_report,
    cmd_report,
    cmd_show_run,
    cmd_report_csv,
    cmd_report_markdown,
    cmd_scoreboard,
    cmd_target_preview,
)


class CliTests(unittest.TestCase):
    def test_analyze_respects_platform(self) -> None:
        result = cmd_analyze("you're very certain for someone arguing by vibe", platform="reddit")
        self.assertEqual(result["platform"], "reddit")

    def test_personas_lists_known_profiles(self) -> None:
        result = cmd_personas()
        names = {persona["name"] for persona in result["personas"]}
        self.assertIn("dry_midwit_savant", names)
        self.assertIn("fake_sincere_questioner", names)

    def test_draft_heuristic_only_skips_provider_pipeline(self) -> None:
        with mock.patch("bait_engine.cli.main.draft_candidates_with_provider", side_effect=AssertionError("provider path should not run")):
            result = cmd_draft(
                text="A model being useful doesn't make it true, and you're still confusing mechanism with necessity.",
                persona_name="dry_midwit_savant",
                candidate_count=3,
                heuristic_only=True,
            )
        self.assertFalse(result["saved"])
        self.assertGreaterEqual(len(result["draft"]["candidates"]), 1)

    def test_draft_persona_auto_exposes_router_schema(self) -> None:
        result = cmd_draft(
            text="You're certainty-maxing a claim that has no proof and no mechanism.",
            persona_name="auto",
            candidate_count=3,
            heuristic_only=True,
            platform="reddit",
        )
        self.assertTrue(result["selected_persona"])
        self.assertIn(result["selected_persona"], result["persona_scores"])
        self.assertIn("confidence", result)
        self.assertIn("why_selected", result)
        self.assertIn("persona_router", result["plan"])
        self.assertIn("calibration_version", result["plan"]["persona_router"])
        self.assertIn("calibration_timestamp", result["plan"]["persona_router"])
        self.assertIn("segment_confidence", result["plan"]["persona_router"])

    def test_draft_persona_auto_is_deterministic(self) -> None:
        kwargs = {
            "text": "Useful is not true; stop swapping utility for truth and pretending that's logic.",
            "persona_name": "auto",
            "candidate_count": 3,
            "heuristic_only": True,
            "platform": "reddit",
        }
        first = cmd_draft(**kwargs)
        second = cmd_draft(**kwargs)

        self.assertEqual(first["selected_persona"], second["selected_persona"])
        self.assertEqual(first["persona_scores"], second["persona_scores"])
        self.assertEqual(first["why_selected"], second["why_selected"])

    def test_draft_persona_auto_save_persists_router_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            saved = cmd_draft(
                text="You're dressing confidence up as evidence again.",
                persona_name="auto",
                candidate_count=3,
                save=True,
                db_path=str(db_path),
                platform="reddit",
                heuristic_only=True,
            )
        self.assertTrue(saved["selected_persona"])
        self.assertIn("persona_router", saved["plan"])
        self.assertEqual(saved["plan"]["persona_router"]["selected_persona"], saved["selected_persona"])

    def test_replay_reuses_stored_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            saved = cmd_draft(
                text="That study being useful doesn't make your conclusion true.",
                persona_name="dry_midwit_savant",
                candidate_count=3,
                save=True,
                db_path=str(db_path),
                platform="reddit",
            )
            replayed = cmd_replay(saved["run_id"], candidate_count=2, db_path=str(db_path))
            self.assertEqual(replayed["run_id"], saved["run_id"])
            self.assertEqual(replayed["persona"], "dry_midwit_savant")
            self.assertEqual(replayed["plan"]["selected_objective"], saved["plan"]["selected_objective"])
            self.assertLessEqual(len(replayed["draft"]["candidates"]), 2)
            self.assertEqual(replayed["prompt_payload"]["source_text"], "That study being useful doesn't make your conclusion true.")

    def test_replay_heuristic_only_skips_provider_pipeline(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            saved = cmd_draft(
                text="You're doing a lot of certainty theater for a claim with no spine.",
                persona_name="dry_midwit_savant",
                candidate_count=3,
                save=True,
                db_path=str(db_path),
            )
            with mock.patch("bait_engine.cli.main.draft_candidates_with_provider", side_effect=AssertionError("provider path should not run")):
                replayed = cmd_replay(saved["run_id"], candidate_count=2, db_path=str(db_path), heuristic_only=True)
        self.assertEqual(replayed["run_id"], saved["run_id"])
        self.assertLessEqual(len(replayed["draft"]["candidates"]), 2)

    def test_replay_respects_mutation_source_none(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            seed = cmd_draft(
                text="That certainty sounds loud, not correct.",
                persona_name="dry_midwit_savant",
                candidate_count=3,
                save=True,
                db_path=str(db_path),
                platform="reddit",
                force_engage=True,
                mutation_source="none",
            )
            replayed = cmd_replay(
                seed["run_id"],
                candidate_count=3,
                db_path=str(db_path),
                heuristic_only=True,
                mutation_source="none",
            )
        payload = replayed["prompt_payload"]
        self.assertEqual(payload["mutation_seeds"], [])
        self.assertIsNone(payload["mutation_context"])
        self.assertEqual(payload["winner_anchors"], [])
        self.assertEqual(payload["avoid_patterns"], [])

    def test_draft_defaults_to_mutation_source_none_and_surfaces_generation_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, mock.patch("bait_engine.storage.repository.RunRepository._select_mutation_seeds", side_effect=AssertionError("mutation seeds should not be selected")):
            db_path = Path(tmp) / "bait.db"
            saved = cmd_draft(
                text="That certainty sounds loud, not correct.",
                persona_name="dry_midwit_savant",
                candidate_count=3,
                save=True,
                db_path=str(db_path),
                platform="reddit",
                heuristic_only=True,
            )
        self.assertEqual(saved["prompt_payload"]["mutation_seeds"], [])
        self.assertEqual(saved["generation_state"]["provider_fallback_state"], "heuristic_only")
        self.assertFalse(saved["generation_state"]["provider_requested"])

    def test_replay_defaults_to_mutation_source_none(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, mock.patch("bait_engine.storage.repository.RunRepository._select_mutation_seeds", side_effect=AssertionError("mutation seeds should not be selected")):
            db_path = Path(tmp) / "bait.db"
            saved = cmd_draft(
                text="You're doing a lot of certainty theater for a claim with no spine.",
                persona_name="dry_midwit_savant",
                candidate_count=3,
                save=True,
                db_path=str(db_path),
                platform="reddit",
                heuristic_only=True,
            )
            replayed = cmd_replay(
                saved["run_id"],
                candidate_count=2,
                db_path=str(db_path),
                heuristic_only=True,
            )
        self.assertEqual(replayed["prompt_payload"]["mutation_seeds"], [])
        self.assertEqual(replayed["generation_state"]["provider_fallback_state"], "heuristic_only")

    def test_replay_uses_stored_selected_model_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            saved = cmd_draft(
                text="That study being useful doesn't make your conclusion true.",
                persona_name="dry_midwit_savant",
                candidate_count=3,
                save=True,
                db_path=str(db_path),
                platform="reddit",
                heuristic_only=True,
            )
            with mock.patch("bait_engine.cli.main._build_provider") as build_provider:
                build_provider.return_value.is_available.return_value = False
                replayed = cmd_replay(saved["run_id"], candidate_count=2, db_path=str(db_path))
        build_provider.assert_called_once_with(model=saved["plan"]["selected_model"], base_url=None, timeout_seconds=None)
        self.assertEqual(replayed["selected_model"], saved["plan"]["selected_model"])
        self.assertEqual(replayed["generation_state"]["provider_fallback_state"], "provider_unavailable")

    def test_draft_uses_recovery_default_model_when_unspecified(self) -> None:
        with mock.patch.dict(os.environ, {"BAIT_ENGINE_CHEAP_MODEL": "recovery-model"}, clear=True):
            with mock.patch("bait_engine.cli.main.OpenAICompatibleProvider") as provider_cls:
                provider_cls.return_value.is_available.return_value = False
                provider_cls.return_value.model = "recovery-model"
                result = cmd_draft(
                    text="That model is useful, but usefulness still isn't truth.",
                    persona_name="dry_midwit_savant",
                    candidate_count=3,
                    heuristic_only=False,
                )
        provider_cls.assert_called_once_with(model="recovery-model", base_url=None, timeout_seconds=None)
        self.assertEqual(result["generation_state"]["provider_model"], "recovery-model")

    def test_cmd_mutate_run_and_mutation_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            saved = cmd_draft(
                text="Your premise is swaggering without receipts.",
                persona_name="dry_midwit_savant",
                candidate_count=3,
                save=True,
                db_path=str(db_path),
                platform="reddit",
                heuristic_only=True,
                force_engage=True,
                mutation_source="none",
            )
            mutated = cmd_mutate_run(saved["run_id"], db_path=str(db_path), variants_per_winner=3)
            self.assertTrue(mutated["ok"])
            self.assertGreaterEqual(mutated["variant_count"], 1)

            report = cmd_mutation_report(
                db_path=str(db_path),
                limit=25,
                persona="dry_midwit_savant",
                platform="reddit",
            )
            self.assertTrue(report["ok"])
            self.assertGreaterEqual(report["total"], 1)
            self.assertTrue(report["by_transform"])

    def test_draft_explicit_provider_settings_are_forwarded(self) -> None:
        with mock.patch("bait_engine.cli.main.OpenAICompatibleProvider") as provider_cls:
            provider_cls.return_value.is_available.return_value = False
            cmd_draft(
                text="That study being useful doesn't make your conclusion true.",
                persona_name="dry_midwit_savant",
                candidate_count=3,
                model="test-model",
                base_url="https://example.invalid/v1",
                timeout_seconds=12,
            )
        provider_cls.assert_called_once_with(model="test-model", base_url="https://example.invalid/v1", timeout_seconds=12)

    def test_adapters_surface_registry(self) -> None:
        adapters = cmd_adapters()
        self.assertTrue(adapters["adapters"])
        reddit = cmd_adapter("reddit")
        self.assertEqual(reddit["platform"], "reddit")
        self.assertTrue(reddit["capabilities"]["can_reply"])

    def test_target_preview_normalizes_platform_target(self) -> None:
        result = cmd_target_preview("reddit", reply_to_id=" t1_crypt ", author_handle="/u/GraveScholar")
        self.assertEqual(result["target"]["thread_id"], "t1_crypt")
        self.assertEqual(result["target"]["reply_to_id"], "t1_crypt")
        self.assertEqual(result["target"]["author_handle"], "GraveScholar")

    def test_target_preview_enforces_capabilities(self) -> None:
        with self.assertRaises(ValueError):
            cmd_target_preview("web", thread_id="thread-1")

    def test_context_preview_builds_thread_context(self) -> None:
        result = cmd_context_preview(
            "reddit",
            "thread-9",
            subject="graveyard debate",
            messages_json='[{"message_id":"m1","body":"first"},{"message_id":"m2","body":"second"}]',
        )
        self.assertEqual(result["context"]["platform"], "reddit")
        self.assertEqual(result["context"]["thread_id"], "thread-9")
        self.assertEqual(len(result["context"]["messages"]), 2)

    def test_recommend_preset_from_context(self) -> None:
        result = cmd_recommend_preset(
            "reddit",
            context_json='{"platform":"reddit","thread_id":"t1","messages":[{"message_id":"m1","body":"lol you idiot"},{"message_id":"m2","body":"cope moron"}]}'
        )
        self.assertEqual(result["recommendation"]["name"], "safe")

    def test_adapter_preview_builds_reply_envelope(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            saved = cmd_draft(
                text="A model being useful doesn't make it true, and you're still confusing mechanism with necessity.",
                persona_name="dry_midwit_savant",
                candidate_count=3,
                save=True,
                db_path=str(db_path),
                platform="reddit",
            )
            envelope = cmd_adapter_preview(
                saved["run_id"],
                candidate_rank_index=1,
                selection_preset="default",
                db_path=str(db_path),
                thread_id="t-123",
                reply_to_id="c-456",
                author_handle="necroposter",
                context_json='{"platform":"reddit","thread_id":"t-123","messages":[{"message_id":"m1","body":"opening"}]}'
            )

        self.assertEqual(envelope["action"], "reply")
        self.assertEqual(envelope["run_id"], saved["run_id"])
        self.assertEqual(envelope["target"]["thread_id"], "t-123")
        self.assertEqual(envelope["target"]["reply_to_id"], "c-456")
        self.assertEqual(envelope["target"]["author_handle"], "necroposter")
        self.assertEqual(envelope["metadata"]["selection_strategy"], "top_score")
        self.assertEqual(envelope["metadata"]["selection_preset"], "default")
        self.assertEqual(envelope["metadata"]["thread_context"]["message_count"], 1)
        self.assertTrue(envelope["body"])

    def test_emit_preview_and_panel_preview(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            html_path = Path(tmp) / "panel.html"
            prior = cmd_draft(
                text="A model being useful doesn't make it true, and you're still confusing mechanism with necessity.",
                persona_name="dry_midwit_savant",
                candidate_count=3,
                save=True,
                db_path=str(db_path),
                platform="reddit",
            )
            cmd_record_outcome(
                prior["run_id"],
                got_reply=True,
                reply_delay_seconds=33,
                reply_length=52,
                tone_shift="defensive",
                spectator_engagement=3,
                result_label="bite",
                notes=None,
                db_path=str(db_path),
            )
            cmd_record_panel_review(
                prior["run_id"],
                disposition="promote",
                selection_preset="engage",
                selection_strategy="rank",
                tactic="calm_reduction",
                objective="tilt",
                notes="operator liked this shape",
                db_path=str(db_path),
            )
            saved = cmd_draft(
                text="A model being useful doesn't make it true, and you're still confusing mechanism with necessity.",
                persona_name="dry_midwit_savant",
                candidate_count=3,
                save=True,
                db_path=str(db_path),
                platform="reddit",
            )
            emit = cmd_emit_preview(
                saved["run_id"],
                selection_preset="default",
                db_path=str(db_path),
                thread_id="t-123",
                reply_to_id="c-456",
                author_handle="necroposter",
                context_json='{"platform":"reddit","thread_id":"t-123","messages":[{"message_id":"m1","body":"opening"}]}'
            )
            panel = cmd_panel_preview(
                saved["run_id"],
                db_path=str(db_path),
                thread_id="t-123",
                reply_to_id="c-456",
                author_handle="necroposter",
                context_json='{"platform":"reddit","thread_id":"t-123","messages":[{"message_id":"m1","body":"opening"}]}',
                out_path=str(html_path),
                include_strategy_variants=True,
                variant_limit=4,
                history_limit=10,
            )

            self.assertEqual(emit["emit_request"]["transport"], "reddit.comment.reply")
            self.assertEqual(panel["recommended_preset"]["name"], "engage")
            self.assertIn("controls", panel)
            self.assertIn("variants", panel)
            self.assertIn("variant_generation", panel)
            self.assertIn("primary_variant", panel)
            self.assertIn("outcome_overlay", panel)
            self.assertIn("review_overlay", panel)
            self.assertIn("review_bridge", panel)
            self.assertGreaterEqual(panel["outcome_overlay"]["history_runs_considered"], 2)
            self.assertGreaterEqual(panel["variants"][0]["outcome_overlay"]["matching_runs"], 1)
            self.assertEqual(panel["variants"][0]["history_rank"], 1)
            self.assertIn("history_rationale", panel["primary_variant"])
            self.assertIn("review_rationale", panel["primary_variant"])
            self.assertIn("review_action_templates", panel["primary_variant"])
            self.assertIn("shell_command", panel["primary_variant"]["review_action_templates"]["promote"])
            self.assertIn("comparison_to_runner_up", panel["primary_variant"])
            self.assertTrue(panel["variant_generation"]["include_strategy_variants"])
            self.assertEqual(panel["variant_generation"]["variant_limit"], 4)
            self.assertLessEqual(len(panel["variants"]), 4)
            self.assertEqual(panel["html_path"], str(html_path))
            self.assertTrue(html_path.exists())
            html_text = html_path.read_text(encoding="utf-8")
            self.assertIn("Local Controls", html_text)
            self.assertIn("Variant generation", html_text)
            self.assertIn("Outcome overlay", html_text)
            self.assertIn("Operator review overlay", html_text)
            self.assertIn("Review Submission Bridge", html_text)
            self.assertIn("Recent matches", html_text)
            self.assertIn("Comparison deltas", html_text)
            self.assertIn("comparisonDeltaList", html_text)
            self.assertIn("comparisonDominant", html_text)
            self.assertIn("Back to dashboard", html_text)
            self.assertIn("bait-engine:last-review-note", html_text)
            self.assertIn("Copy emit JSON", html_text)
            self.assertIn("Download envelope JSON", html_text)
            self.assertIn("emitActionStatus", html_text)
            self.assertIn("Stage emit locally", html_text)
            self.assertIn("/api/stage-emit", html_text)

    def test_panel_serve_can_auto_open_browser(self) -> None:
        mock_server = mock.Mock()
        mock_server.server_address = ("127.0.0.1", 9999)
        with mock.patch("bait_engine.cli.main._create_panel_http_server", return_value=mock_server), mock.patch("bait_engine.cli.main.webbrowser.open") as open_mock:
            cmd_panel_serve(7, open_browser=True)
        open_mock.assert_called_once_with("http://127.0.0.1:9999/?run_id=7")
        mock_server.serve_forever.assert_called_once()
        mock_server.server_close.assert_called_once()

    def test_daemon_manage_can_install_and_uninstall_launch_agent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            launch_dir = Path(tmp) / "LaunchAgents"
            log_dir = Path(tmp) / "Logs"
            payload = cmd_daemon_manage(
                "install",
                db_path=str(Path(tmp) / "bait.db"),
                launch_agents_dir=str(launch_dir),
                log_dir=str(log_dir),
                load=False,
            )
            self.assertTrue(payload["installed"])
            self.assertEqual(payload["mode"], "panel")
            self.assertTrue(Path(payload["plist_path"]).exists())
            self.assertIn("panel-serve", payload["plist"])
            self.assertIn("PYTHONPATH", payload["plist"])
            removed = cmd_daemon_manage(
                "uninstall",
                db_path=str(Path(tmp) / "bait.db"),
                launch_agents_dir=str(launch_dir),
                log_dir=str(log_dir),
                unload=False,
            )
            self.assertFalse(removed["installed"])
            self.assertFalse(Path(removed["plist_path"]).exists())

    def test_daemon_manage_can_install_worker_launch_agent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            launch_dir = Path(tmp) / "LaunchAgents"
            log_dir = Path(tmp) / "Logs"
            dispatch_dir = Path(tmp) / "dispatches"
            payload = cmd_daemon_manage(
                "install",
                db_path=str(Path(tmp) / "bait.db"),
                launch_agents_dir=str(launch_dir),
                log_dir=str(log_dir),
                load=False,
                mode="worker",
                dispatch_limit=7,
                driver="manual_copy",
                out_dir=str(dispatch_dir),
                interval_seconds=12.5,
                max_cycles=3,
                include_failed_redrive=True,
                redrive_limit=4,
                min_failed_age_seconds=90,
            )
            self.assertTrue(payload["installed"])
            self.assertEqual(payload["mode"], "worker")
            self.assertEqual(payload["label"], "ai.bait-engine.worker")
            self.assertIn("worker-run", payload["plist"])
            self.assertIn("--dispatch-limit", payload["plist"])
            self.assertIn("12.5", payload["plist"])
            self.assertIn(str(dispatch_dir), payload["plist"])
            self.assertEqual(payload["max_cycles"], 3)
            self.assertIn("manual_copy", payload["supported_drivers"])
            self.assertIn("webhook_post", payload["supported_drivers"])
            self.assertTrue(payload["include_failed_redrive"])
            self.assertEqual(payload["redrive_limit"], 4)
            self.assertEqual(payload["min_failed_age_seconds"], 90.0)
            self.assertTrue(any("KeepAlive" in warning for warning in payload["daemon_warnings"]))
            self.assertIn("--max-cycles", payload["plist"])
            self.assertIn("<string>3</string>", payload["plist"])
            self.assertIn("--include-failed-redrive", payload["plist"])
            self.assertIn("--redrive-limit", payload["plist"])
            self.assertIn("--min-failed-age-seconds", payload["plist"])
            removed = cmd_daemon_manage(
                "uninstall",
                db_path=str(Path(tmp) / "bait.db"),
                launch_agents_dir=str(launch_dir),
                log_dir=str(log_dir),
                unload=False,
                mode="worker",
            )
            self.assertFalse(removed["installed"])

    def test_daemon_manage_warns_on_unknown_worker_driver(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            payload = cmd_daemon_manage(
                "install",
                db_path=str(Path(tmp) / "bait.db"),
                launch_agents_dir=str(Path(tmp) / "LaunchAgents"),
                log_dir=str(Path(tmp) / "Logs"),
                load=False,
                mode="worker",
                driver="bogus_driver",
            )
            self.assertTrue(any("unknown" in warning for warning in payload["daemon_warnings"]))
            self.assertIn("manual_copy", payload["supported_drivers"])
            self.assertIn("jsonl_append", payload["supported_drivers"])

    def test_dispatch_emit_writes_manual_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            dispatch_dir = Path(tmp) / "dispatches"
            saved = cmd_draft(
                text="A model being useful doesn't make it true, and you're still confusing mechanism with necessity.",
                persona_name="dry_midwit_savant",
                candidate_count=3,
                save=True,
                db_path=str(db_path),
                platform="reddit",
            )
            panel = cmd_panel_preview(saved["run_id"], db_path=str(db_path), thread_id="t-123", reply_to_id="c-456", author_handle="necroposter")
            from bait_engine.storage import RunRepository, EmitOutboxRecord
            repo = RunRepository(str(db_path))
            staged = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=saved["run_id"],
                    platform=panel["adapter"]["platform"],
                    transport=panel["emit_request"]["transport"],
                    selection_preset=panel["primary_variant"]["selection"]["preset"],
                    selection_strategy=panel["primary_variant"]["selection"]["strategy"],
                    tactic=panel["primary_variant"]["selection"]["tactic"],
                    objective=panel["primary_variant"]["selection"]["objective"],
                    status="approved",
                    envelope_json=json.dumps(panel["envelope"]),
                    emit_request_json=json.dumps(panel["emit_request"]),
                    notes="approved for dispatch",
                )
            )
            emit_id = staged["emit_outbox"][0]["id"]
            dispatched = cmd_dispatch_emit(emit_id, db_path=str(db_path), out_dir=str(dispatch_dir))
            self.assertTrue(dispatched["ok"])
            self.assertEqual(dispatched["emit"]["status"], "dispatched")
            artifact_path = Path(dispatched["dispatch"]["response"]["artifact_path"])
            self.assertTrue(artifact_path.exists())
            lifecycle = cmd_update_dispatch_status(dispatched["dispatch"]["id"], status="failed", db_path=str(db_path), notes="clipboard mangled")
            self.assertTrue(lifecycle["ok"])
            self.assertEqual(lifecycle["dispatch"]["status"], "failed")
            redriven = cmd_redrive_dispatch(lifecycle["dispatch"]["id"], db_path=str(db_path), out_dir=str(dispatch_dir), notes="retry after cleanup")
            self.assertTrue(redriven["ok"])
            self.assertEqual(redriven["dispatch"]["status"], "dispatched")
            self.assertEqual(redriven["previous_dispatch"]["status"], "failed")
            outbox = cmd_outbox(db_path=str(db_path), status="dispatched")
            self.assertEqual(outbox["emit_outbox"][0]["id"], emit_id)

    def test_dispatch_approved_batches_multiple_emits(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            dispatch_dir = Path(tmp) / "dispatches"
            from bait_engine.storage import RunRepository, EmitOutboxRecord
            repo = RunRepository(str(db_path))
            emit_ids = []
            for idx in range(2):
                saved = cmd_draft(
                    text="A model being useful doesn't make it true, and you're still confusing mechanism with necessity.",
                    persona_name="dry_midwit_savant",
                    candidate_count=3,
                    save=True,
                    db_path=str(db_path),
                    platform="reddit",
                )
                panel = cmd_panel_preview(saved["run_id"], db_path=str(db_path), thread_id="t-123", reply_to_id=f"c-45{idx}", author_handle="necroposter")
                staged = repo.stage_emit(
                    EmitOutboxRecord(
                        id=None,
                        run_id=saved["run_id"],
                        platform=panel["adapter"]["platform"],
                        transport=panel["emit_request"]["transport"],
                        selection_preset=panel["primary_variant"]["selection"]["preset"],
                        selection_strategy=panel["primary_variant"]["selection"]["strategy"],
                        tactic=panel["primary_variant"]["selection"]["tactic"],
                        objective=panel["primary_variant"]["selection"]["objective"],
                        status="approved",
                        envelope_json=json.dumps(panel["envelope"]),
                        emit_request_json=json.dumps(panel["emit_request"]),
                        notes=f"approved {idx}",
                    )
                )
                emit_ids.append(staged["emit_outbox"][0]["id"])
            batch = cmd_dispatch_approved(limit=10, db_path=str(db_path), out_dir=str(dispatch_dir), notes="batch handoff")
            self.assertTrue(batch["ok"])
            self.assertEqual(batch["approved_found"], 2)
            self.assertEqual(batch["dispatched_count"], 2)
            self.assertEqual(len(batch["dispatched"]), 2)
            for item in batch["dispatched"]:
                self.assertEqual(item["emit"]["status"], "dispatched")
                self.assertTrue(Path(item["dispatch"]["response"]["artifact_path"]).exists())
            outbox = cmd_outbox(db_path=str(db_path), status="dispatched")
            self.assertEqual(len(outbox["emit_outbox"]), 2)
            cycle = cmd_worker_cycle(db_path=str(db_path), dispatch_limit=10, out_dir=str(dispatch_dir), notes="idle pass")
            self.assertTrue(cycle["ok"])
            self.assertEqual(cycle["approved_found"], 0)
            self.assertEqual(cycle["dispatched_count"], 0)
            self.assertEqual(cycle["failed_found"], 0)
            self.assertEqual(cycle["redriven_count"], 0)

    def test_panel_http_server_dashboard_defaults_to_latest_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            first = cmd_draft(
                text="A model being useful doesn't make it true, and you're still confusing mechanism with necessity.",
                persona_name="dry_midwit_savant",
                candidate_count=3,
                save=True,
                db_path=str(db_path),
                platform="reddit",
            )
            second = cmd_draft(
                text="What exactly do you mean by that and why should anyone buy it?",
                persona_name="dry_midwit_savant",
                candidate_count=3,
                save=True,
                db_path=str(db_path),
                platform="reddit",
            )
            server = _create_panel_http_server(
                None,
                db_path=str(db_path),
                thread_id="t-123",
                reply_to_id="c-456",
                author_handle="necroposter",
                context_json='{"platform":"reddit","thread_id":"t-123","messages":[{"message_id":"m1","body":"opening"}]}',
                host="127.0.0.1",
                port=0,
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                base_url = f"http://{server.server_address[0]}:{server.server_address[1]}"
                dashboard = urlopen(f"{base_url}/dashboard").read().decode("utf-8")
                panel = json.loads(urlopen(f"{base_url}/panel.json").read().decode("utf-8"))
                runs_payload = json.loads(urlopen(f"{base_url}/api/runs").read().decode("utf-8"))
                self.assertIn("Bait Engine Dashboard", dashboard)
                self.assertIn(f"Run #{second['run_id']}", dashboard)
                self.assertEqual(panel["envelope"]["run_id"], second["run_id"])
                self.assertEqual(runs_payload["latest_run_id"], second["run_id"])
                switched = json.loads(urlopen(f"{base_url}/panel.json?run_id={first['run_id']}").read().decode("utf-8"))
                self.assertEqual(switched["envelope"]["run_id"], first["run_id"])
            finally:
                server.shutdown()
                thread.join(timeout=2)
                server.server_close()

    def test_panel_http_server_can_create_draft_from_dashboard(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            server = _create_panel_http_server(
                None,
                db_path=str(db_path),
                thread_id="t-123",
                reply_to_id="c-456",
                author_handle="necroposter",
                context_json='{"platform":"reddit","thread_id":"t-123","messages":[{"message_id":"m1","body":"opening"}]}',
                host="127.0.0.1",
                port=0,
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                base_url = f"http://{server.server_address[0]}:{server.server_address[1]}"
                dashboard = urlopen(f"{base_url}/dashboard").read().decode("utf-8")
                self.assertIn("Quick draft", dashboard)
                self.assertIn("bait-engine:last-run-id", dashboard)
                self.assertIn("restoreDraftDefaults", dashboard)
                with mock.patch("bait_engine.cli.main.cmd_draft") as draft_mock:
                    draft_mock.return_value = {"ok": True, "run_id": 999, "plan": {}, "draft": {"candidates": []}}
                    request = Request(
                        f"{base_url}/api/draft",
                        data=json.dumps({
                            "text": "A model being useful doesn't make it true, and you're still confusing mechanism with necessity.",
                            "persona": "dry_midwit_savant",
                            "platform": "reddit",
                            "count": 4,
                        }).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    response = json.loads(urlopen(request).read().decode("utf-8"))
                self.assertTrue(response["ok"])
                draft_mock.assert_called_once()
                self.assertFalse(draft_mock.call_args.kwargs["force_engage"])
            finally:
                server.shutdown()
                thread.join(timeout=2)
                server.server_close()

    def test_panel_http_server_can_stage_emit_to_outbox(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            saved = cmd_draft(
                text="A model being useful doesn't make it true, and you're still confusing mechanism with necessity.",
                persona_name="dry_midwit_savant",
                candidate_count=3,
                save=True,
                db_path=str(db_path),
                platform="reddit",
            )
            server = _create_panel_http_server(
                saved["run_id"],
                db_path=str(db_path),
                thread_id="t-123",
                reply_to_id="c-456",
                author_handle="necroposter",
                context_json='{"platform":"reddit","thread_id":"t-123","messages":[{"message_id":"m1","body":"opening"}]}',
                include_strategy_variants=True,
                host="127.0.0.1",
                port=0,
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                base_url = f"http://{server.server_address[0]}:{server.server_address[1]}"
                panel = json.loads(urlopen(f"{base_url}/panel.json").read().decode("utf-8"))
                primary = panel["primary_variant"]
                request = Request(
                    f"{base_url}/api/stage-emit",
                    data=json.dumps({
                        "run_id": panel["envelope"]["run_id"],
                        "platform": panel["adapter"]["platform"],
                        "selection_preset": primary["selection"]["preset"],
                        "selection_strategy": primary["selection"]["strategy"],
                        "tactic": primary["selection"]["tactic"],
                        "objective": primary["selection"]["objective"],
                        "status": "staged",
                        "envelope": panel["envelope"],
                        "emit_request": panel["emit_request"],
                        "notes": "rack this one",
                    }).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                response = json.loads(urlopen(request).read().decode("utf-8"))
                self.assertTrue(response["ok"])
                self.assertEqual(response["staged_emit"]["status"], "staged")
                dashboard = urlopen(f"{base_url}/dashboard").read().decode("utf-8")
                outbox_payload = json.loads(urlopen(f"{base_url}/api/outbox?outbox_status=staged").read().decode("utf-8"))
                self.assertIn("Local outbox", dashboard)
                self.assertIn("MISSION HEALTH", dashboard)
                self.assertIn("AUTO PICK ACCURACY", dashboard)
                self.assertIn("PERSONA DRIFT", dashboard)
                self.assertIn("Staged (1)", dashboard)
                self.assertIn("Router audit", dashboard)
                self.assertIn("forced_persona=", dashboard)
                self.assertEqual(outbox_payload["outbox_status"], "staged")
                self.assertEqual(outbox_payload["emit_outbox"][0]["status"], "staged")
            finally:
                server.shutdown()
                thread.join(timeout=2)
                server.server_close()

            updated = cmd_show_run(saved["run_id"], db_path=str(db_path))
            self.assertEqual(updated["emit_outbox"][0]["notes"], "rack this one")

    def test_panel_http_server_can_manage_daemon_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            launch_dir = Path(tmp) / "LaunchAgents"
            log_dir = Path(tmp) / "Logs"
            server = _create_panel_http_server(
                None,
                db_path=str(db_path),
                host="127.0.0.1",
                port=0,
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                base_url = f"http://{server.server_address[0]}:{server.server_address[1]}"
                with mock.patch("bait_engine.cli.main._build_dashboard_payload") as dashboard_mock, mock.patch("bait_engine.cli.main.cmd_daemon_manage") as daemon_mock:
                    dashboard_mock.return_value = {
                        "runs": [],
                        "latest_run_id": None,
                        "active_run_id": None,
                        "total_runs": 0,
                        "default_persona": "dry_midwit_savant",
                        "default_platform": "reddit",
                        "emit_outbox": [],
                        "pending_emit_count": 0,
                        "outbox_status": None,
                        "outbox_counts": {"all": 0},
                        "daemon": {"label": "ai.bait-engine.panel", "mode": "panel", "installed": False, "loaded": False, "plist": "plist-body", "plist_path": "/tmp/example.plist", "daemon_warnings": []},
                    }
                    dashboard = urlopen(f"{base_url}/dashboard").read().decode("utf-8")
                    self.assertIn("Daemon mode", dashboard)
                    self.assertIn("Install login daemon", dashboard)
                    self.assertIn("daemonModeSelect", dashboard)
                    self.assertIn("daemonWorkerControls", dashboard)
                    self.assertIn("display:none", dashboard)
                    self.assertIn("daemonWorkerDispatchLimit", dashboard)
                    self.assertIn("daemonWorkerDriver", dashboard)
                    self.assertIn('<option value="auto" selected>auto</option>', dashboard)
                    self.assertIn("daemonWorkerIntervalSeconds", dashboard)
                    self.assertIn("daemonWorkerOutDir", dashboard)
                    self.assertIn("daemonWorkerIncludeFailedRedrive", dashboard)
                    self.assertIn("daemonWorkerRedriveLimit", dashboard)
                    self.assertIn("daemonWorkerMinFailedAgeSeconds", dashboard)
                    self.assertIn("daemonWorkerMaxCycles", dashboard)
                    self.assertIn("daemonPolicySummary", dashboard)
                    self.assertIn("daemonConfigSummary", dashboard)
                    self.assertIn("daemonWarningsSummary", dashboard)
                    self.assertIn("applyDaemonMode", dashboard)
                    self.assertIn("panel mode — no worker retry policy active", dashboard)
                    self.assertIn("panel mode — worker launch config inactive", dashboard)
                    self.assertIn("Warnings: <span id=\"daemonWarningsSummary\">none</span>", dashboard)
                    self.assertIn("max cycles", dashboard)
                    self.assertIn("syncWorkerPolicyControls", dashboard)
                    self.assertNotIn("bait-engine:daemon-worker-include-failed-redrive", dashboard)
                    self.assertIn("/api/daemon", dashboard)
                    daemon_mock.return_value = {"label": "ai.bait-engine.worker", "mode": "worker", "installed": True, "loaded": False, "plist": "plist-body", "plist_path": "/tmp/example.plist", "dispatch_limit": 9, "driver": "manual_copy", "out_dir": "/tmp/dispatches", "interval_seconds": 5.0, "max_cycles": 2, "include_failed_redrive": True, "redrive_limit": 6, "min_failed_age_seconds": 45.0, "daemon_warnings": ["max_cycles is bounded, but LaunchAgent KeepAlive will relaunch the worker after exit"], "supported_drivers": ["manual_copy"]}
                    request = Request(
                        f"{base_url}/api/daemon",
                        data=json.dumps({"action": "install", "load": False, "mode": "worker", "dispatch_limit": 9, "driver": "manual_copy", "out_dir": "/tmp/dispatches", "interval_seconds": 5, "max_cycles": 2, "include_failed_redrive": True, "redrive_limit": 6, "min_failed_age_seconds": 45}).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    response = json.loads(urlopen(request).read().decode("utf-8"))
                    self.assertTrue(response["ok"])
                    self.assertTrue(response["daemon"]["installed"])
                    self.assertEqual(response["daemon"]["mode"], "worker")
                    self.assertEqual(response["daemon"]["dispatch_limit"], 9)
                    self.assertEqual(response["daemon"]["driver"], "manual_copy")
                    self.assertEqual(response["daemon"]["out_dir"], "/tmp/dispatches")
                    self.assertEqual(response["daemon"]["interval_seconds"], 5.0)
                    self.assertEqual(response["daemon"]["max_cycles"], 2)
                    self.assertTrue(any("KeepAlive" in warning for warning in response["daemon"]["daemon_warnings"]))
                    self.assertEqual(response["daemon"]["supported_drivers"], ["manual_copy"])
                    self.assertTrue(response["daemon"]["include_failed_redrive"])
                    self.assertEqual(response["daemon"]["redrive_limit"], 6)
                    self.assertEqual(response["daemon"]["min_failed_age_seconds"], 45.0)
                    daemon_mock.assert_called_with(
                        "install",
                        db_path=str(db_path),
                        load=False,
                        unload=False,
                        mode="worker",
                        dispatch_limit=9,
                        driver="manual_copy",
                        out_dir="/tmp/dispatches",
                        interval_seconds=5.0,
                        max_cycles=2,
                        include_failed_redrive=True,
                        redrive_limit=6,
                        min_failed_age_seconds=45.0,
                        max_actions_per_hour=0,
                        max_actions_per_day=0,
                        min_seconds_between_actions=0.0,
                        quiet_hours_start=None,
                        quiet_hours_end=None,
                    )
            finally:
                server.shutdown()
                thread.join(timeout=2)
                server.server_close()

    def test_panel_http_server_can_dispatch_approved_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            from bait_engine.storage import RunRepository, EmitOutboxRecord
            repo = RunRepository(str(db_path))
            for idx in range(2):
                saved = cmd_draft(
                    text="A model being useful doesn't make it true, and you're still confusing mechanism with necessity.",
                    persona_name="dry_midwit_savant",
                    candidate_count=3,
                    save=True,
                    db_path=str(db_path),
                    platform="reddit",
                )
                panel = cmd_panel_preview(saved["run_id"], db_path=str(db_path), thread_id="t-123", reply_to_id=f"c-b{idx}", author_handle="necroposter")
                repo.stage_emit(
                    EmitOutboxRecord(
                        id=None,
                        run_id=saved["run_id"],
                        platform=panel["adapter"]["platform"],
                        transport=panel["emit_request"]["transport"],
                        selection_preset=panel["primary_variant"]["selection"]["preset"],
                        selection_strategy=panel["primary_variant"]["selection"]["strategy"],
                        tactic=panel["primary_variant"]["selection"]["tactic"],
                        objective=panel["primary_variant"]["selection"]["objective"],
                        status="approved",
                        envelope_json=json.dumps(panel["envelope"]),
                        emit_request_json=json.dumps(panel["emit_request"]),
                        notes=f"approved batch {idx}",
                    )
                )
            server = _create_panel_http_server(None, db_path=str(db_path), host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                base_url = f"http://{server.server_address[0]}:{server.server_address[1]}"
                request = Request(
                    f"{base_url}/api/worker-cycle",
                    data=json.dumps({"dispatch_limit": 10, "include_failed_redrive": True, "redrive_limit": 5, "min_failed_age_seconds": 60}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                batch = json.loads(urlopen(request).read().decode("utf-8"))
                self.assertTrue(batch["ok"])
                self.assertEqual(batch["approved_found"], 2)
                self.assertEqual(batch["dispatched_count"], 2)
                self.assertEqual(batch["failed_found"], 0)
                self.assertEqual(batch["redriven_count"], 0)
                self.assertEqual(batch["min_failed_age_seconds"], 60.0)
                dashboard = urlopen(f"{base_url}/dashboard?outbox_status=dispatched").read().decode("utf-8")
                self.assertIn("Dispatched (2)", dashboard)
                self.assertIn("Run worker cycle", dashboard)
                self.assertIn("workerIncludeFailedRedrive", dashboard)
                self.assertIn("workerRedriveLimit", dashboard)
                self.assertIn("workerMinFailedAgeSeconds", dashboard)
                self.assertIn("redrove", dashboard)
                self.assertIn("include_failed_redrive", dashboard)
                self.assertIn("redrive_limit", dashboard)
                self.assertIn("min_failed_age_seconds", dashboard)
                self.assertIn("/api/worker-cycle", dashboard)
            finally:
                server.shutdown()
                thread.join(timeout=2)
                server.server_close()

    def test_worker_run_loops_with_max_cycles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            dispatch_dir = Path(tmp) / "dispatches"
            from bait_engine.storage import RunRepository, EmitOutboxRecord
            repo = RunRepository(str(db_path))
            saved = cmd_draft(
                text="A model being useful doesn't make it true, and you're still confusing mechanism with necessity.",
                persona_name="dry_midwit_savant",
                candidate_count=3,
                save=True,
                db_path=str(db_path),
                platform="reddit",
            )
            panel = cmd_panel_preview(saved["run_id"], db_path=str(db_path), thread_id="t-123", reply_to_id="c-loop", author_handle="necroposter")
            staged = repo.stage_emit(
                EmitOutboxRecord(
                    id=None,
                    run_id=saved["run_id"],
                    platform=panel["adapter"]["platform"],
                    transport=panel["emit_request"]["transport"],
                    selection_preset=panel["primary_variant"]["selection"]["preset"],
                    selection_strategy=panel["primary_variant"]["selection"]["strategy"],
                    tactic=panel["primary_variant"]["selection"]["tactic"],
                    objective=panel["primary_variant"]["selection"]["objective"],
                    status="approved",
                    envelope_json=json.dumps(panel["envelope"]),
                    emit_request_json=json.dumps(panel["emit_request"]),
                    notes="worker loop candidate",
                )
            )
            emit_id = staged["emit_outbox"][0]["id"]
            dispatched = cmd_dispatch_emit(emit_id, db_path=str(db_path), out_dir=str(dispatch_dir))
            cmd_update_dispatch_status(dispatched["dispatch"]["id"], status="failed", db_path=str(db_path), notes="loop fail")
            with mock.patch("bait_engine.cli.main.time.sleep") as sleep_mock:
                result = cmd_worker_run(
                    db_path=str(db_path),
                    dispatch_limit=10,
                    out_dir=str(dispatch_dir),
                    notes="loop pass",
                    interval_seconds=0.01,
                    max_cycles=2,
                    include_failed_redrive=True,
                    redrive_limit=10,
                    min_failed_age_seconds=0.0,
                )
            self.assertTrue(result["ok"])
            self.assertEqual(result["cycles_run"], 2)
            self.assertEqual(result["cycles"][0]["dispatched_count"], 0)
            self.assertEqual(result["cycles"][0]["redriven_count"], 1)
            self.assertEqual(result["cycles"][1]["dispatched_count"], 0)
            self.assertEqual(result["cycles"][1]["redriven_count"], 0)
            sleep_mock.assert_called_once_with(0.01)

    def test_panel_http_server_can_update_outbox_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            saved = cmd_draft(
                text="A model being useful doesn't make it true, and you're still confusing mechanism with necessity.",
                persona_name="dry_midwit_savant",
                candidate_count=3,
                save=True,
                db_path=str(db_path),
                platform="reddit",
            )
            server = _create_panel_http_server(
                saved["run_id"],
                db_path=str(db_path),
                thread_id="t-123",
                reply_to_id="c-456",
                author_handle="necroposter",
                context_json='{"platform":"reddit","thread_id":"t-123","messages":[{"message_id":"m1","body":"opening"}]}',
                host="127.0.0.1",
                port=0,
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                base_url = f"http://{server.server_address[0]}:{server.server_address[1]}"
                panel = json.loads(urlopen(f"{base_url}/panel.json").read().decode("utf-8"))
                stage_request = Request(
                    f"{base_url}/api/stage-emit",
                    data=json.dumps({
                        "run_id": panel["envelope"]["run_id"],
                        "platform": panel["adapter"]["platform"],
                        "selection_preset": panel["primary_variant"]["selection"]["preset"],
                        "selection_strategy": panel["primary_variant"]["selection"]["strategy"],
                        "tactic": panel["primary_variant"]["selection"]["tactic"],
                        "objective": panel["primary_variant"]["selection"]["objective"],
                        "status": "staged",
                        "envelope": panel["envelope"],
                        "emit_request": panel["emit_request"],
                    }).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                staged = json.loads(urlopen(stage_request).read().decode("utf-8"))
                emit_id = staged["staged_emit"]["id"]
                update_request = Request(
                    f"{base_url}/api/outbox-status",
                    data=json.dumps({"emit_id": emit_id, "status": "approved", "notes": "operator approved"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                updated = json.loads(urlopen(update_request).read().decode("utf-8"))
                self.assertTrue(updated["ok"])
                self.assertEqual(updated["emit"]["status"], "approved")
                replace_notes_request = Request(
                    f"{base_url}/api/outbox-status",
                    data=json.dumps({"emit_id": emit_id, "status": "approved", "notes": "replace this note", "notes_mode": "replace"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                replaced = json.loads(urlopen(replace_notes_request).read().decode("utf-8"))
                self.assertTrue(replaced["ok"])
                self.assertEqual(replaced["emit"]["notes"], "replace this note")
                dispatch_request = Request(
                    f"{base_url}/api/dispatch-emit",
                    data=json.dumps({"emit_id": emit_id}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                dispatched = json.loads(urlopen(dispatch_request).read().decode("utf-8"))
                self.assertTrue(dispatched["ok"])
                self.assertEqual(dispatched["emit"]["status"], "dispatched")
                dispatch_status_request = Request(
                    f"{base_url}/api/dispatch-status",
                    data=json.dumps({"dispatch_id": dispatched["dispatch"]["id"], "status": "failed", "notes": "landed crooked", "notes_mode": "append"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                failed = json.loads(urlopen(dispatch_status_request).read().decode("utf-8"))
                self.assertTrue(failed["ok"])
                self.assertEqual(failed["dispatch"]["status"], "failed")
                redrive_request = Request(
                    f"{base_url}/api/dispatch-redrive",
                    data=json.dumps({"dispatch_id": failed["dispatch"]["id"], "notes": "second shot"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                redriven = json.loads(urlopen(redrive_request).read().decode("utf-8"))
                self.assertTrue(redriven["ok"])
                self.assertEqual(redriven["previous_dispatch"]["status"], "failed")
                self.assertEqual(redriven["dispatch"]["status"], "dispatched")
                delivered_request = Request(
                    f"{base_url}/api/dispatch-status",
                    data=json.dumps({"dispatch_id": redriven["dispatch"]["id"], "status": "delivered", "notes": "landed clean", "notes_mode": "append"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                delivered = json.loads(urlopen(delivered_request).read().decode("utf-8"))
                self.assertTrue(delivered["ok"])
                self.assertEqual(delivered["dispatch"]["status"], "delivered")
                dashboard = urlopen(f"{base_url}/dashboard").read().decode("utf-8")
                self.assertIn("Restage", dashboard)
                self.assertIn("Archive", dashboard)
                self.assertIn("Save notes", dashboard)
                self.assertIn("Dispatch history", dashboard)
                self.assertIn("Manual dispatch artifacts", dashboard)
                self.assertIn("Dispatch approved", dashboard)
                self.assertIn("Save dispatch notes", dashboard)
                self.assertIn("Acknowledge", dashboard)
                self.assertIn("Delivered", dashboard)
                self.assertIn("Failed", dashboard)
                self.assertIn("data-outbox-notes", dashboard)
                self.assertIn("data-dispatch-notes", dashboard)
                self.assertIn("notes_mode", dashboard)
                self.assertIn("/api/outbox-status", dashboard)
                self.assertIn("/api/dispatch-emit", dashboard)
                self.assertIn("/api/dispatch-approved", dashboard)
                self.assertIn("/api/dispatch-status", dashboard)
                self.assertIn("/api/dispatch-redrive", dashboard)
                self.assertIn("Redrive", dashboard)
            finally:
                server.shutdown()
                thread.join(timeout=2)
                server.server_close()

            run = cmd_show_run(saved["run_id"], db_path=str(db_path))
            self.assertEqual(run["emit_outbox"][0]["status"], "delivered")
            self.assertEqual(run["emit_outbox"][0]["notes"], "replace this note")
            self.assertTrue(run["emit_dispatches"])
            self.assertEqual(run["emit_dispatches"][0]["status"], "delivered")

    def test_panel_http_server_accepts_direct_review_submission(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            saved = cmd_draft(
                text="A model being useful doesn't make it true, and you're still confusing mechanism with necessity.",
                persona_name="dry_midwit_savant",
                candidate_count=3,
                save=True,
                db_path=str(db_path),
                platform="reddit",
            )
            server = _create_panel_http_server(
                saved["run_id"],
                db_path=str(db_path),
                thread_id="t-123",
                reply_to_id="c-456",
                author_handle="necroposter",
                context_json='{"platform":"reddit","thread_id":"t-123","messages":[{"message_id":"m1","body":"opening"}]}',
                include_strategy_variants=True,
                host="127.0.0.1",
                port=0,
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                base_url = f"http://{server.server_address[0]}:{server.server_address[1]}"
                panel_res = urlopen(f"{base_url}/panel.json")
                panel = json.loads(panel_res.read().decode("utf-8"))
                self.assertEqual(panel["review_bridge"]["kind"], "http")
                template = panel["primary_variant"]["review_action_templates"]["promote"]
                self.assertEqual(template["bridge_request"]["kind"], "http")
                request = Request(
                    f"{base_url}/api/review",
                    data=json.dumps({**template["args"], "notes": "bound in browser"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                response = json.loads(urlopen(request).read().decode("utf-8"))
                self.assertTrue(response["ok"])
                self.assertEqual(response["latest_review"]["disposition"], "promote")
                self.assertEqual(response["latest_review"]["notes"], "bound in browser")
            finally:
                server.shutdown()
                thread.join(timeout=2)
                server.server_close()

            updated = cmd_show_run(saved["run_id"], db_path=str(db_path))
            self.assertEqual(updated["panel_reviews"][0]["notes"], "bound in browser")

    def test_autopsy_many_and_scoreboard(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            first = cmd_draft(
                text="A model being useful doesn't make it true, and you're still confusing mechanism with necessity.",
                persona_name="dry_midwit_savant",
                candidate_count=3,
                save=True,
                db_path=str(db_path),
                platform="reddit",
            )
            second = cmd_draft(
                text="What exactly do you mean by that and why should anyone buy it?",
                persona_name="fake_sincere_questioner",
                candidate_count=3,
                save=True,
                db_path=str(db_path),
                platform="twitter",
            )
            third = cmd_draft(
                text="This one is still waiting in the crypt.",
                persona_name="dry_midwit_savant",
                candidate_count=3,
                save=True,
                db_path=str(db_path),
                platform="reddit",
            )
            cmd_record_outcome(
                first["run_id"],
                got_reply=True,
                reply_delay_seconds=45,
                reply_length=40,
                tone_shift="defensive",
                spectator_engagement=2,
                result_label="bite",
                notes=None,
                db_path=str(db_path),
            )
            cmd_record_outcome(
                second["run_id"],
                got_reply=False,
                reply_delay_seconds=None,
                reply_length=None,
                tone_shift=None,
                spectator_engagement=0,
                result_label="dead_thread",
                notes=None,
                db_path=str(db_path),
            )

            autopsies = cmd_autopsy_many(limit=10, db_path=str(db_path), platform="reddit")
            scoreboard = cmd_scoreboard(limit=10, db_path=str(db_path), persona="dry_midwit_savant")
            report = cmd_report(limit=10, section_limit=1, db_path=str(db_path), verdict="engaged")
            markdown = cmd_report_markdown(limit=10, section_limit=1, db_path=str(db_path), verdict="engaged")
            csv_export = cmd_report_csv(limit=10, section_limit=1, db_path=str(db_path), verdict="engaged")

        self.assertEqual(len(autopsies["runs"]), 2)
        self.assertEqual(autopsies["filters"]["platform"], "reddit")
        self.assertEqual(scoreboard["filters"]["persona"], "dry_midwit_savant")
        self.assertEqual(scoreboard["total_runs"], 2)
        self.assertEqual(scoreboard["engaged"], 1)
        self.assertEqual(scoreboard["pending"], 1)
        self.assertTrue(scoreboard["platforms"])
        self.assertTrue(scoreboard["objectives"])
        self.assertTrue(scoreboard["tactics"])
        self.assertTrue(scoreboard["exit_states"])
        self.assertEqual(report["scoreboard"]["filters"]["verdict"], "engaged")
        self.assertEqual(report["scoreboard"]["total_runs"], 1)
        self.assertEqual(len(report["best_runs"]), 1)
        self.assertEqual(len(report["worst_runs"]), 0)
        self.assertIn("# Bait Engine Report", markdown["markdown"])
        self.assertIn("## Best Runs", markdown["markdown"])
        self.assertIn("record_type,section", csv_export["csv"])
        self.assertIn("summary,summary", csv_export["csv"])


class PanelHttpEdgeCaseTests(unittest.TestCase):
    def _start_server(self, db_path: str) -> tuple[object, str]:
        server = _create_panel_http_server(None, db_path=db_path, host="127.0.0.1", port=0)
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        base_url = f"http://{server.server_address[0]}:{server.server_address[1]}"
        return server, base_url

    def test_panel_post_malformed_json_returns_400(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            server, base_url = self._start_server(str(Path(tmp) / "bait.db"))
            try:
                req = Request(
                    f"{base_url}/api/draft",
                    data=b"this is not json {{{{",
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                try:
                    urlopen(req)
                    self.fail("expected HTTPError 400")
                except Exception as exc:
                    code = getattr(exc, "code", None)
                    self.assertEqual(code, 400)
                    body = json.loads(exc.read().decode("utf-8"))
                    self.assertIn("error", body)
                    self.assertIn("invalid json", body["error"])
            finally:
                server.shutdown()
                server.server_close()

    def test_panel_post_draft_missing_text_returns_400(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            server, base_url = self._start_server(str(Path(tmp) / "bait.db"))
            try:
                req = Request(
                    f"{base_url}/api/draft",
                    data=json.dumps({"persona": "dry_midwit_savant", "count": 2}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                try:
                    urlopen(req)
                    self.fail("expected HTTPError 400")
                except Exception as exc:
                    code = getattr(exc, "code", None)
                    self.assertEqual(code, 400)
                    body = json.loads(exc.read().decode("utf-8"))
                    self.assertIn("error", body)
            finally:
                server.shutdown()
                server.server_close()

    def test_panel_post_unknown_path_returns_404(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            server, base_url = self._start_server(str(Path(tmp) / "bait.db"))
            try:
                req = Request(
                    f"{base_url}/api/does-not-exist",
                    data=json.dumps({}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                try:
                    urlopen(req)
                    self.fail("expected HTTPError 404")
                except Exception as exc:
                    code = getattr(exc, "code", None)
                    self.assertEqual(code, 404)
            finally:
                server.shutdown()
                server.server_close()

    def test_panel_post_empty_body_draft_returns_400(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            server, base_url = self._start_server(str(Path(tmp) / "bait.db"))
            try:
                req = Request(
                    f"{base_url}/api/draft",
                    data=b"",
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                # empty body parses as "{}" which is missing "text", so expect 400
                try:
                    urlopen(req)
                    self.fail("expected HTTPError 400")
                except Exception as exc:
                    code = getattr(exc, "code", None)
                    self.assertEqual(code, 400)
            finally:
                server.shutdown()
                server.server_close()


if __name__ == "__main__":
    unittest.main()
