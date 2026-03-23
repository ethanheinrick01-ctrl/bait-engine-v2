from __future__ import annotations

from pathlib import Path
import json
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bait_engine.generation.mutate import generate_controlled_variants
from bait_engine.storage import MutationFamilyRecord, MutationVariantRecord, OutcomeRecord, RunRepository
from bait_engine.storage.db import open_db


class MutationPipelineTests(unittest.TestCase):
    def test_mutate_top_winners_persists_lineage_and_variants(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)

            def seed_run(source_text: str, selected_tactic: str) -> dict[str, object]:
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
                            "extract_reaction",
                            selected_tactic,
                            "leave_target_exposed",
                            json.dumps({"seeded": True}),
                            json.dumps(
                                {
                                    "selected_objective": "extract_reaction",
                                    "selected_tactic": selected_tactic,
                                    "exit_state": "leave_target_exposed",
                                }
                            ),
                        ),
                    )
                    run_id = int(cur.lastrowid)
                    conn.execute(
                        """
                        INSERT INTO candidates (
                            run_id, rank_index, text, tactic, objective, estimated_bite_score,
                            estimated_audience_score, critic_penalty, rank_score, critic_notes_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            run_id,
                            1,
                            "You skipped the premise and sprinted to certainty.",
                            selected_tactic,
                            "extract_reaction",
                            0.72,
                            0.61,
                            0.06,
                            0.81,
                            json.dumps([]),
                        ),
                    )
                return repo.get_run(run_id)

            first = seed_run("Confidence is not evidence, no matter the volume.", "precision_needle")
            second = seed_run("You announced a conclusion before proving a premise.", "status_shiv")

            repo.record_outcome(
                OutcomeRecord(
                    id=None,
                    run_id=int(first["id"]),
                    got_reply=True,
                    reply_delay_seconds=35,
                    reply_length=90,
                    tone_shift="defensive",
                    spectator_engagement=5,
                    result_label="bite",
                    notes="seed winner one",
                )
            )
            repo.record_outcome(
                OutcomeRecord(
                    id=None,
                    run_id=int(second["id"]),
                    got_reply=True,
                    reply_delay_seconds=75,
                    reply_length=60,
                    tone_shift="annoyed",
                    spectator_engagement=3,
                    result_label="bite",
                    notes="seed winner two",
                )
            )

            result = repo.mutate_top_winners(
                winner_limit=2,
                variants_per_winner=3,
                platform="reddit",
                strategy="controlled_v1",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["winner_count"], 2)
            self.assertEqual(result["family_count"], 2)
            self.assertGreaterEqual(result["variant_count"], 2)
            self.assertLessEqual(result["variant_count"], 6)

            variants = result["variants"]
            self.assertTrue(all(item.get("family_id") is not None for item in variants))
            self.assertTrue(all(item.get("run_id") in {first["id"], second["id"]} for item in variants))
            self.assertTrue(all(item.get("lineage", {}).get("run_id") in {first["id"], second["id"]} for item in variants))
            self.assertTrue(all(item.get("transform") for item in variants))

            repeat = repo.mutate_top_winners(
                winner_limit=2,
                variants_per_winner=3,
                platform="reddit",
                strategy="controlled_v1",
            )
            self.assertEqual(repeat["family_count"], 2)
            self.assertEqual(repeat["variant_count"], result["variant_count"])

    def test_rebuild_request_recovers_recent_mutation_seeds(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)

            with open_db(db_path) as conn:
                cur = conn.execute(
                    """
                    INSERT INTO runs (
                        source_text, platform, persona, selected_objective, selected_tactic, exit_state, analysis_json, plan_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "Confidence is not evidence, no matter the volume.",
                        "reddit",
                        "dry_midwit_savant",
                        "extract_reaction",
                        "precision_needle",
                        "leave_target_exposed",
                        json.dumps({"seeded": True}),
                        json.dumps(
                            {
                                "selected_objective": "extract_reaction",
                                "selected_tactic": "precision_needle",
                                "exit_state": "leave_target_exposed",
                            }
                        ),
                    ),
                )
                run_id = int(cur.lastrowid)
                conn.execute(
                    """
                    INSERT INTO candidates (
                        run_id, rank_index, text, tactic, objective, estimated_bite_score,
                        estimated_audience_score, critic_penalty, rank_score, critic_notes_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        1,
                        "You skipped the premise and sprinted to certainty.",
                        "precision_needle",
                        "extract_reaction",
                        0.72,
                        0.61,
                        0.06,
                        0.81,
                        json.dumps([]),
                    ),
                )

            repo.record_outcome(
                OutcomeRecord(
                    id=None,
                    run_id=run_id,
                    got_reply=True,
                    reply_delay_seconds=35,
                    reply_length=90,
                    tone_shift="defensive",
                    spectator_engagement=5,
                    result_label="bite",
                    notes="seed winner",
                )
            )
            repo.mutate_top_winners(
                winner_limit=1,
                variants_per_winner=3,
                platform="reddit",
                strategy="controlled_v1",
            )

            created = repo.create_run_from_text(
                "A model being useful doesn't make it true, and you're still confusing mechanism with necessity.",
                persona_name="dry_midwit_savant",
                platform="reddit",
                candidate_count=3,
                heuristic_only=True,
            )
            request = repo.rebuild_request(int(created["id"]))
            self.assertGreaterEqual(len(request.mutation_seeds), 1)
            self.assertTrue(request.mutation_context)
            self.assertGreaterEqual(len(request.winner_anchors), 1)
            seed_texts = {seed.text for seed in request.mutation_seeds}
            variant_texts = {item["variant_text"] for item in repo.list_mutation_variants(limit=10)}
            self.assertTrue(seed_texts & variant_texts)

            request_no_mutation = repo.rebuild_request(int(created["id"]), mutation_source="none")
            self.assertEqual(request_no_mutation.mutation_seeds, [])
            self.assertIn("tone-shift trend", request_no_mutation.mutation_context or "")
            self.assertEqual(request_no_mutation.winner_anchors, [])
            self.assertEqual(request_no_mutation.avoid_patterns, [])

    def test_mutate_run_and_mutation_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            created = repo.create_run_from_text(
                "You declared certainty before proving the premise.",
                persona_name="dry_midwit_savant",
                platform="reddit",
                candidate_count=3,
                heuristic_only=True,
                force_engage=True,
                mutation_source="none",
            )
            result = repo.mutate_run(int(created["id"]), variants_per_winner=3, strategy="controlled_v1")
            self.assertTrue(result["ok"])
            self.assertEqual(result["run_id"], int(created["id"]))
            self.assertGreaterEqual(result["variant_count"], 1)

            report = repo.mutation_report(limit=25, persona="dry_midwit_savant", platform="reddit")
            self.assertTrue(report["ok"])
            self.assertGreaterEqual(report["total"], 1)
            self.assertTrue(report["by_transform"])

    def test_mutation_seed_selection_prefers_stronger_status_signals(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)

            seeded_run = repo.create_run_from_text(
                "You skipped the premise and still demanded applause.",
                persona_name="dry_midwit_savant",
                platform="reddit",
                candidate_count=3,
                heuristic_only=True,
                force_engage=True,
                mutation_source="none",
            )
            run_id = int(seeded_run["id"])
            winner_candidate_id = int((seeded_run.get("candidates") or [])[0]["id"])

            family = repo.create_mutation_family(
                MutationFamilyRecord(
                    id=None,
                    run_id=run_id,
                    winner_candidate_id=winner_candidate_id,
                    winner_rank_index=1,
                    persona="dry_midwit_savant",
                    platform="reddit",
                    tactic="precision_needle",
                    objective="extract_reaction",
                    winner_score=0.5,
                    source="seed",
                    strategy="controlled_v1",
                    notes=None,
                    lineage_json=json.dumps({"winner": {"run_id": run_id}}),
                )
            )
            family_id = int(family["id"])

            drafted = MutationVariantRecord(
                id=None,
                family_id=family_id,
                run_id=run_id,
                parent_candidate_id=winner_candidate_id,
                transform="remove_hedge",
                variant_text="Maybe you're right if we ignore the premise.",
                variant_hash=repo._variant_hash("Maybe you're right if we ignore the premise."),
                status="drafted",
                score_json=json.dumps({"seed_winner_score": 0.5, "seed_rank_score": 0.5}),
                lineage_json=json.dumps({"mutation_metrics": {"delta_ratio": 0.33, "novelty_ratio": 0.45}}),
            )
            replied = MutationVariantRecord(
                id=None,
                family_id=family_id,
                run_id=run_id,
                parent_candidate_id=winner_candidate_id,
                transform="inject_contrast",
                variant_text="You skipped the premise, then acted shocked by scrutiny.",
                variant_hash=repo._variant_hash("You skipped the premise, then acted shocked by scrutiny."),
                status="replied",
                score_json=json.dumps({"seed_winner_score": 0.5, "seed_rank_score": 0.5}),
                lineage_json=json.dumps({"mutation_metrics": {"delta_ratio": 0.33, "novelty_ratio": 0.45}}),
            )
            repo.create_mutation_variants([drafted, replied])

            seeds = repo._select_mutation_seeds(
                persona="dry_midwit_savant",
                platform="reddit",
                tactic="precision_needle",
                objective="extract_reaction",
                limit=1,
            )
            self.assertEqual(len(seeds), 1)
            self.assertEqual(seeds[0].transform, "inject_contrast")

            variants = repo.list_mutation_variants(family_id=family_id, limit=10)
            selected = {int(item["id"]): item["status"] for item in variants}
            selected_status = selected.get(int(seeds[0].variant_id or 0))
            self.assertIn(selected_status, {"selected", "replied"})

    def test_get_lane_prior_aggregates_mutation_family_outcomes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)

            seeded_run = repo.create_run_from_text(
                "You skipped the premise and still demanded applause.",
                persona_name="dry_midwit_savant",
                platform="reddit",
                candidate_count=3,
                heuristic_only=True,
                force_engage=True,
                mutation_source="none",
            )
            run_id = int(seeded_run["id"])
            winner_candidate_id = int((seeded_run.get("candidates") or [])[0]["id"])

            family = repo.create_mutation_family(
                MutationFamilyRecord(
                    id=None,
                    run_id=run_id,
                    winner_candidate_id=winner_candidate_id,
                    winner_rank_index=1,
                    persona="dry_midwit_savant",
                    platform="reddit",
                    tactic="essay_collapse",
                    objective="collapse",
                    winner_score=0.82,
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
                        score_json=json.dumps({"seed_winner_score": 0.82, "seed_rank_score": 0.79}),
                        lineage_json=json.dumps({"mutation_metrics": {"delta_ratio": 0.31, "novelty_ratio": 0.42}}),
                    ),
                    MutationVariantRecord(
                        id=None,
                        family_id=family_id,
                        run_id=run_id,
                        parent_candidate_id=winner_candidate_id,
                        transform="inject_contrast",
                        variant_text="Your conclusion outran your premise.",
                        variant_hash=repo._variant_hash("Your conclusion outran your premise."),
                        status="promoted",
                        score_json=json.dumps({"seed_winner_score": 0.82, "seed_rank_score": 0.74}),
                        lineage_json=json.dumps({"mutation_metrics": {"delta_ratio": 0.35, "novelty_ratio": 0.48}}),
                    ),
                ]
            )

            prior = repo.get_lane_prior(
                persona="dry_midwit_savant",
                platform="reddit",
                tactic="essay_collapse",
                objective="collapse",
                days=30,
            )

            self.assertGreater(prior["prior_score"], 0.5)
            self.assertGreater(prior["confidence"], 0.0)
            self.assertGreaterEqual(prior["sample_count"], 2)
            self.assertEqual(prior["scope"], "persona+platform")

    def test_generate_controlled_variants_respects_transform_policy_order(self) -> None:
        winner = {
            "run_id": 1,
            "candidate_id": 1,
            "candidate_rank_index": 1,
            "candidate_text": "You announced certainty before proving a premise, then called that debate.",
            "candidate_objective": "inflate",
            "winner_score": 0.7,
            "winner_source": "test",
        }
        variants = generate_controlled_variants(
            winner,
            max_variants=3,
            transform_policy=["invert_confidence_posture", "compress"],
        )
        self.assertTrue(variants)
        self.assertEqual(variants[0]["transform"], "invert_confidence_posture")

    def test_rebuild_request_adapts_persona_pressure_from_tone_shift(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(db_path)
            created = repo.create_run_from_text(
                "Your conclusion arrived before your evidence.",
                persona_name="dry_midwit_savant",
                platform="reddit",
                candidate_count=3,
                heuristic_only=True,
                force_engage=True,
                mutation_source="none",
            )
            run_id = int(created["id"])
            repo.record_outcome(
                OutcomeRecord(
                    id=None,
                    run_id=run_id,
                    got_reply=True,
                    reply_delay_seconds=42,
                    reply_length=84,
                    tone_shift="defensive",
                    spectator_engagement=5,
                    result_label="bite",
                    notes="pressure adaptation seed",
                )
            )

            rebuilt = repo.rebuild_request(run_id, mutation_source="none")
            self.assertIn("premise lock", rebuilt.persona.escalation_cues)
            self.assertIn("tone-shift trend: defensive", rebuilt.mutation_context or "")


if __name__ == "__main__":
    unittest.main()
