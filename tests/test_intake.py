from __future__ import annotations

from pathlib import Path
import io
import json
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bait_engine.cli.main import _resolve_hunt_generation_lane, cmd_hunt_cycle, cmd_hunt_list, cmd_hunt_preview, cmd_hunt_promote
from bait_engine.intake.sources import fetch_targets
from bait_engine.storage import MutationFamilyRecord, MutationVariantRecord, RunRepository
from bait_engine.storage.db import open_db


class IntakeTests(unittest.TestCase):
    def _write_jsonl(self, path: Path) -> None:
        records = [
            {
                "platform": "reddit",
                "source_item_id": "t3_hotone",
                "thread_id": "t3_hotone",
                "reply_to_id": "t3_hotone",
                "author_handle": "necrolad",
                "subject": "Bro is wildly confident",
                "body": "A model being useful doesn't make it true, and you're still confusing mechanism with necessity.",
                "metadata": {"score": 88, "num_comments": 34},
            },
            {
                "platform": "reddit",
                "source_item_id": "t3_coldtwo",
                "thread_id": "t3_coldtwo",
                "reply_to_id": "t3_coldtwo",
                "author_handle": "gravelet",
                "subject": "Low-energy corpse",
                "body": "I guess maybe perhaps this could sort of be true.",
                "metadata": {"score": 1, "num_comments": 0},
            },
        ]
        path.write_text("\n".join(json.dumps(item) for item in records), encoding="utf-8")

    def test_hunt_preview_ranks_jsonl_targets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            jsonl_path = Path(tmp) / "targets.jsonl"
            self._write_jsonl(jsonl_path)
            preview = cmd_hunt_preview("jsonl_file", file_path=str(jsonl_path))

        self.assertEqual(preview["fetched"], 2)
        self.assertEqual(preview["targets"][0]["source_item_id"], "t3_hotone")
        self.assertGreater(preview["targets"][0]["score"]["score"], preview["targets"][1]["score"]["score"])
        self.assertIn(preview["targets"][0]["generation_lane"], {"fast", "deep"})
        self.assertTrue(preview["targets"][0]["generation_reason"])

    def test_resolve_hunt_generation_lane_defaults_to_fast_until_score_earns_deep(self) -> None:
        low = _resolve_hunt_generation_lane({"score": {"score": 0.42, "signals": {"reply_probability": 0.28, "essay_probability": 0.11, "contradiction_signal": 0.12, "audience_value": 0.09}}})
        hot = _resolve_hunt_generation_lane({"score": {"score": 0.73, "signals": {"reply_probability": 0.46, "essay_probability": 0.34, "contradiction_signal": 0.38, "audience_value": 0.31}}})

        self.assertEqual(low["lane"], "fast")
        self.assertTrue(low["heuristic_only"])
        self.assertEqual(hot["lane"], "deep")
        self.assertFalse(hot["heuristic_only"])

    def test_resolve_hunt_generation_lane_uses_effective_score_when_present(self) -> None:
        routed = _resolve_hunt_generation_lane(
            {
                "score": {
                    "score": 0.61,
                    "effective_score": 0.73,
                    "signals": {
                        "reply_probability": 0.44,
                        "essay_probability": 0.35,
                        "contradiction_signal": 0.34,
                        "audience_value": 0.31,
                    },
                    "bite_detection": {"qualified": True, "score": 0.49},
                }
            }
        )
        self.assertEqual(routed["lane"], "deep")
        self.assertAlmostEqual(routed["effective_score"], 0.73, places=2)

    def test_resolve_hunt_generation_lane_respects_explicit_overrides(self) -> None:
        forced_fast = _resolve_hunt_generation_lane({"score": {"score": 0.99, "signals": {}}}, heuristic_only=True)
        forced_deep = _resolve_hunt_generation_lane({"score": {"score": 0.01, "signals": {}}}, heuristic_only=False)

        self.assertEqual(forced_fast["lane"], "fast")
        self.assertTrue(forced_fast["heuristic_only"])
        self.assertEqual(forced_deep["lane"], "deep")
        self.assertFalse(forced_deep["heuristic_only"])

    def test_resolve_hunt_generation_lane_requires_bite_qualification_for_deep_lane(self) -> None:
        suppressed = _resolve_hunt_generation_lane(
            {
                "score": {
                    "score": 0.91,
                    "signals": {
                        "reply_probability": 0.60,
                        "essay_probability": 0.52,
                        "contradiction_signal": 0.41,
                        "audience_value": 0.48,
                    },
                    "bite_detection": {"qualified": False, "score": 0.21},
                }
            }
        )

        self.assertEqual(suppressed["lane"], "fast")
        self.assertTrue(suppressed["heuristic_only"])
        self.assertIn("bite detection", suppressed["reason"])

    def test_hunt_preview_exposes_bite_detection_block(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            jsonl_path = Path(tmp) / "targets.jsonl"
            self._write_jsonl(jsonl_path)
            preview = cmd_hunt_preview("jsonl_file", file_path=str(jsonl_path))

        score = preview["targets"][0]["score"]
        self.assertIn("bite_detection", score)
        self.assertIn("qualified", score["bite_detection"])

    def test_hunt_preview_persona_auto_exposes_router_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            jsonl_path = Path(tmp) / "targets.jsonl"
            self._write_jsonl(jsonl_path)
            preview = cmd_hunt_preview("jsonl_file", file_path=str(jsonl_path), persona_name="auto")

        first = preview["targets"][0]
        self.assertTrue(first["selected_persona"])
        self.assertIsInstance(first["persona_scores"], dict)
        self.assertIn(first["selected_persona"], first["persona_scores"])
        self.assertIn("confidence", first)
        self.assertIn("why_selected", first)
        self.assertIsInstance(first["why_selected"], list)

    def test_hunt_preview_applies_lane_prior_metadata_when_mutation_history_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            jsonl_path = Path(tmp) / "targets.jsonl"
            self._write_jsonl(jsonl_path)
            repo = RunRepository(db_path)

            seeded = repo.create_run_from_text(
                "You declared certainty before proving the premise.",
                persona_name="dry_midwit_savant",
                platform="reddit",
                candidate_count=3,
                heuristic_only=True,
                force_engage=True,
                mutation_source="none",
            )
            run_id = int(seeded["id"])
            winner_candidate_id = int((seeded.get("candidates") or [])[0]["id"])
            family = repo.create_mutation_family(
                MutationFamilyRecord(
                    id=None,
                    run_id=run_id,
                    winner_candidate_id=winner_candidate_id,
                    winner_rank_index=1,
                    persona="dry_midwit_savant",
                    platform="reddit",
                    tactic=None,
                    objective=None,
                    winner_score=0.86,
                    source="seed",
                    strategy="controlled_v1",
                    notes=None,
                    lineage_json=json.dumps({"winner": {"run_id": run_id}}),
                )
            )
            family_id = int(family["id"])
            repo.create_mutation_variants(
                [
                    MutationVariantRecord(
                        id=None,
                        family_id=family_id,
                        run_id=run_id,
                        parent_candidate_id=winner_candidate_id,
                        transform="compress",
                        variant_text="You skipped evidence and called it certainty.",
                        variant_hash=repo._variant_hash("You skipped evidence and called it certainty."),
                        status="replied",
                        score_json=json.dumps({"seed_winner_score": 0.86, "seed_rank_score": 0.81}),
                        lineage_json=json.dumps({"mutation_metrics": {"delta_ratio": 0.31, "novelty_ratio": 0.44}}),
                    )
                ]
            )

            preview = cmd_hunt_preview("jsonl_file", file_path=str(jsonl_path), db_path=str(db_path), persona_name="dry_midwit_savant")

        first = preview["targets"][0]["score"]
        self.assertIn("effective_score", first)
        self.assertIn("lane_prior", first)
        self.assertGreaterEqual(first["lane_prior"].get("confidence") or 0.0, 0.0)

    def test_hunt_cycle_promotes_and_stages(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            jsonl_path = Path(tmp) / "targets.jsonl"
            self._write_jsonl(jsonl_path)

            cycle = cmd_hunt_cycle(
                "jsonl_file",
                db_path=str(db_path),
                file_path=str(jsonl_path),
                promote_limit=1,
                heuristic_only=True,
                approve_emit=True,
            )
            repo = RunRepository(db_path)
            stored_targets = repo.list_intake_targets(limit=10)

        self.assertEqual(cycle["saved"], 2)
        self.assertEqual(cycle["promoted"], 1)
        self.assertEqual(cycle["promotion_results"][0]["emit"]["status"], "approved")
        promoted = next(item for item in stored_targets if item["source_item_id"] == "t3_hotone")
        self.assertEqual(promoted["status"], "approved")
        self.assertIsNotNone(promoted["promoted_run_id"])
        self.assertIsNotNone(promoted["emit_outbox_id"])

    def test_hunt_cycle_can_dispatch_approved(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            out_dir = Path(tmp) / "dispatches"
            jsonl_path = Path(tmp) / "targets.jsonl"
            self._write_jsonl(jsonl_path)

            cycle = cmd_hunt_cycle(
                "jsonl_file",
                db_path=str(db_path),
                file_path=str(jsonl_path),
                promote_limit=1,
                heuristic_only=True,
                dispatch_approved=True,
                driver="jsonl_append",
                out_dir=str(out_dir),
            )

            dispatch_jsonl = out_dir / "dispatches.jsonl"
            listed = cmd_hunt_list(db_path=str(db_path), limit=10)

            self.assertIsNotNone(cycle["dispatch"])
            self.assertEqual(cycle["dispatch"]["dispatched_count"], 1)
            self.assertTrue(dispatch_jsonl.exists())
            self.assertEqual(len(listed["targets"]), 2)

    def test_hunt_promote_is_idempotent_without_force(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            jsonl_path = Path(tmp) / "targets.jsonl"
            self._write_jsonl(jsonl_path)
            cycle = cmd_hunt_cycle(
                "jsonl_file",
                db_path=str(db_path),
                file_path=str(jsonl_path),
                promote_limit=1,
                heuristic_only=True,
                stage_emit=False,
            )
            target_id = cycle["targets"][0]["id"]
            again = cmd_hunt_promote(target_id, db_path=str(db_path), heuristic_only=True, stage_emit=False)

        self.assertTrue(again["promoted"]["already_promoted"])
        self.assertEqual(again["promoted"]["target"]["id"], target_id)

    def test_hunt_promote_auto_fast_lane_skips_provider_for_cold_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            jsonl_path = Path(tmp) / "targets.jsonl"
            self._write_jsonl(jsonl_path)
            cycle = cmd_hunt_cycle(
                "jsonl_file",
                db_path=str(db_path),
                file_path=str(jsonl_path),
                promote_limit=0,
                stage_emit=False,
            )
            cold = next(item for item in cycle["targets"] if item["source_item_id"] == "t3_coldtwo")
            with mock.patch("bait_engine.cli.main._build_provider", side_effect=AssertionError("provider should not run for fast-lane cold target")):
                promoted = cmd_hunt_promote(int(cold["id"]), db_path=str(db_path), stage_emit=False)

        self.assertEqual(promoted["generation_lane"]["lane"], "fast")
        self.assertTrue(promoted["generation_lane"]["heuristic_only"])

    def test_fetch_reddit_listing_parses_payload(self) -> None:
        payload = {
            "data": {
                "children": [
                    {
                        "data": {
                            "id": "abc123",
                            "name": "t3_abc123",
                            "title": "Impossible take",
                            "selftext": "You all keep pretending evidence exists.",
                            "author": "graveking",
                            "score": 42,
                            "num_comments": 9,
                            "subreddit": "testcrypt",
                            "permalink": "/r/testcrypt/comments/abc123/impossible_take/",
                        }
                    }
                ]
            }
        }
        raw = json.dumps(payload).encode("utf-8")

        class FakeResponse(io.BytesIO):
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        with mock.patch("urllib.request.urlopen", return_value=FakeResponse(raw)):
            targets = fetch_targets("reddit_listing", subreddit="testcrypt", limit=5, user_agent="bait-engine-test")

        self.assertEqual(len(targets), 1)
        self.assertEqual(targets[0].thread_id, "t3_abc123")
        self.assertEqual(targets[0].author_handle, "graveking")
        self.assertEqual(targets[0].metadata["num_comments"], 9)
