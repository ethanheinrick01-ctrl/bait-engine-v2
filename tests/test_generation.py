from __future__ import annotations

from pathlib import Path
import re
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bait_engine.analysis import AnalyzeInput, analyze_comment
from bait_engine.core.types import TacticalObjective, TacticFamily
from bait_engine.generation import CandidateReply, DraftRequest, MutationSeed, build_prompt_payload, draft_candidates
from bait_engine.generation.fallbacks import build_disagreement_fallbacks
from bait_engine.generation.ranker import rank_candidates
from bait_engine.generation.writer import generate_candidates, reset_style_memory
from bait_engine.planning import build_plan, get_persona


class GenerationTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_style_memory()

    def test_draft_pipeline_produces_ranked_candidates(self) -> None:
        text = "A model being useful does not make it true, and you are confusing mechanism with necessity."
        analysis = analyze_comment(AnalyzeInput(text=text))
        plan = build_plan(analysis, persona="dry_midwit_savant")
        request = DraftRequest(source_text=text, plan=plan, persona=get_persona("dry_midwit_savant"), candidate_count=4)
        result = draft_candidates(request)
        self.assertGreaterEqual(len(result.candidates), 1)
        scores = [item.rank_score for item in result.candidates]
        self.assertEqual(scores, sorted(scores, reverse=True))

    def test_prompt_payload_matches_plan(self) -> None:
        text = "What exactly do you mean by that and why should anyone buy it?"
        analysis = analyze_comment(AnalyzeInput(text=text))
        plan = build_plan(analysis, persona="fake_sincere_questioner")
        request = DraftRequest(
            source_text=text,
            plan=plan,
            persona=get_persona("fake_sincere_questioner"),
            candidate_count=3,
            mutation_seeds=[
                MutationSeed(
                    text="You announced a conclusion before proving a premise.",
                    transform="compress",
                    tactic=plan.selected_tactic.value if plan.selected_tactic else None,
                    objective=plan.selected_objective.value,
                    winner_score=1.44,
                )
            ],
        )
        payload = build_prompt_payload(request)
        self.assertEqual(payload["plan"]["objective"], plan.selected_objective.value)
        self.assertEqual(payload["persona"]["name"], "fake_sincere_questioner")
        self.assertEqual(payload["mutation_seeds"][0]["transform"], "compress")
        self.assertTrue(any("Do not copy mutation seeds verbatim" in rule for rule in payload["writer_rules"]))

    def test_heuristic_candidates_stay_grounded_to_source_text(self) -> None:
        text = "A model being useful does not make it true, and you are confusing mechanism with necessity."
        analysis = analyze_comment(AnalyzeInput(text=text))
        plan = build_plan(analysis, persona="dry_midwit_savant")
        request = DraftRequest(source_text=text, plan=plan, persona=get_persona("dry_midwit_savant"), candidate_count=3)
        result = draft_candidates(request)

        self.assertGreaterEqual(len(result.candidates), 1)
        winner = result.candidates[0]
        self.assertEqual(winner.generation_source, "heuristic")
        self.assertGreaterEqual(winner.grounding_score, 0.3)
        self.assertTrue(any(token in winner.text.lower() for token in ("useful", "truth", "mechanism", "necessity")))

    def test_mutation_seeds_do_not_leak_verbatim_into_candidates(self) -> None:
        text = "A model being useful does not make it true, and you are confusing mechanism with necessity."
        seed_text = "You skipped the premise and sprinted to certainty."
        analysis = analyze_comment(AnalyzeInput(text=text))
        plan = build_plan(analysis, persona="dry_midwit_savant")
        request = DraftRequest(
            source_text=text,
            plan=plan,
            persona=get_persona("dry_midwit_savant"),
            candidate_count=3,
            mutation_seeds=[
                MutationSeed(
                    text=seed_text,
                    transform="compress",
                    tactic=plan.selected_tactic.value if plan.selected_tactic else None,
                    objective=plan.selected_objective.value,
                )
            ],
        )
        result = draft_candidates(request)
        self.assertGreaterEqual(len(result.candidates), 1)
        self.assertFalse(any(seed_text.lower() in item.text.lower() for item in result.candidates))

    def test_question_objectives_keep_question_shape_and_persona_band(self) -> None:
        text = "What exactly do you mean by that and why should anyone buy it?"
        analysis = analyze_comment(AnalyzeInput(text=text))
        plan = build_plan(analysis, persona="fake_sincere_questioner").model_copy(update={"selected_objective": TacticalObjective.RESURRECT})
        request = DraftRequest(source_text=text, plan=plan, persona=get_persona("fake_sincere_questioner"), candidate_count=3)
        result = draft_candidates(request)

        self.assertGreaterEqual(len(result.candidates), 1)
        min_words, max_words = request.persona.length_band_words
        for candidate in result.candidates:
            self.assertIn("?", candidate.text)
            self.assertTrue(candidate.text.strip().endswith("?"))
            word_count = len(candidate.text.split())
            self.assertGreaterEqual(word_count, min_words)
            self.assertLessEqual(word_count, max_words)

    def test_heuristic_generation_seeds_complementary_weave_roles(self) -> None:
        text = "A model being useful does not make it true, and you are confusing mechanism with necessity."
        analysis = analyze_comment(AnalyzeInput(text=text))
        plan = build_plan(analysis, persona="dry_midwit_savant")
        request = DraftRequest(source_text=text, plan=plan, persona=get_persona("dry_midwit_savant"), candidate_count=5)
        seeded = generate_candidates(request)

        self.assertGreaterEqual(len(seeded), 3)
        self.assertEqual([item.weave_role for item in seeded[:3]], ["lead", "support", "sting"])
        self.assertTrue(any("mechanism" in item.text.lower() for item in seeded[:3]))
        self.assertTrue(any("useful" in item.text.lower() for item in seeded[:3]))

    def test_question_subjects_do_not_emit_malformed_sting_templates(self) -> None:
        text = "Will you ever forgive Israel? People forgave quickly but I cannot."
        analysis = analyze_comment(AnalyzeInput(text=text))
        plan = build_plan(analysis, persona="dry_midwit_savant").model_copy(
            update={
                "selected_objective": TacticalObjective.TILT,
                "selected_tactic": TacticFamily.LABEL_AND_LEAVE,
            }
        )
        request = DraftRequest(source_text=text, plan=plan, persona=get_persona("dry_midwit_savant"), candidate_count=4)
        seeded = generate_candidates(request)

        malformed = re.compile(
            r"^(will|would|can|could|do|does|did|is|are|why|what|how|when|where)\b.*\bis still the trick you're hiding\??$",
            re.IGNORECASE,
        )
        self.assertGreaterEqual(len(seeded), 1)
        self.assertFalse(any(malformed.match(item.text.strip()) for item in seeded))

    def test_noisy_source_text_does_not_leak_broken_fragment_phrasing(self) -> None:
        text = (
            "Why the downvotes? lol :D I'm really working hard on this... "
            "You'll see - once I have something to show, it's going to be really good I promise."
        )
        analysis = analyze_comment(AnalyzeInput(text=text))
        plan = build_plan(analysis, persona="dry_midwit_savant").model_copy(
            update={
                "selected_objective": TacticalObjective.RESURRECT,
                "selected_tactic": TacticFamily.ESSAY_COLLAPSE,
            }
        )
        request = DraftRequest(source_text=text, plan=plan, persona=get_persona("dry_midwit_savant"), candidate_count=3)
        seeded = generate_candidates(request)

        self.assertGreaterEqual(len(seeded), 1)
        lowered = [item.text.lower() for item in seeded]
        self.assertFalse(any("you'll -" in text for text in lowered))
        self.assertFalse(any("it's going be" in text for text in lowered))
        self.assertFalse(any("promise is still one bad premise" in text for text in lowered))

    def test_grounded_candidate_outranks_generic_one(self) -> None:
        generic = CandidateReply(
            text="nah, that conclusion doesn't actually follow",
            objective="collapse",
            persona="dry_midwit_savant",
            grounding_score=0.1,
            generation_source="disagreement_fallback",
            estimated_bite_score=0.62,
            estimated_audience_score=0.58,
            critic_penalty=0.05,
        )
        grounded = CandidateReply(
            text="useful does not make it true, so why are you treating mechanism like necessity?",
            objective="resurrect",
            persona="dry_midwit_savant",
            grounding_score=0.72,
            generation_source="provider",
            estimated_bite_score=0.6,
            estimated_audience_score=0.55,
            critic_penalty=0.08,
        )
        ranked = rank_candidates([generic, grounded])
        self.assertEqual(ranked[0].text, grounded.text)

    def test_do_not_engage_yields_no_candidates(self) -> None:
        text = "lol nah"
        analysis = analyze_comment(AnalyzeInput(text=text))
        plan = build_plan(analysis)
        request = DraftRequest(source_text=text, plan=plan, persona=get_persona(), candidate_count=3)
        result = draft_candidates(request)
        self.assertEqual(result.candidates, [])

    def test_disagreement_fallbacks_include_source_anchor_when_available(self) -> None:
        text = "Main compatibility for Claude code and Opencode is already working."
        analysis = analyze_comment(AnalyzeInput(text=text))
        plan = build_plan(analysis, persona="dry_midwit_savant").model_copy(
            update={"selected_objective": TacticalObjective.RESURRECT}
        )
        request = DraftRequest(source_text=text, plan=plan, persona=get_persona("dry_midwit_savant"), candidate_count=3)
        fallbacks = build_disagreement_fallbacks(request)

        self.assertEqual(len(fallbacks), 3)
        self.assertTrue(any("compatibility" in line.lower() for line in fallbacks))
        self.assertTrue(all("?" in line for line in fallbacks))

    def test_disagreement_fallbacks_prioritize_signal_over_chatter(self) -> None:
        text = (
            "Why the downvotes? lol I'm working hard. "
            "Main compatibility for claude code and opencode is the core update."
        )
        analysis = analyze_comment(AnalyzeInput(text=text))
        plan = build_plan(analysis, persona="dry_midwit_savant").model_copy(
            update={"selected_objective": TacticalObjective.RESURRECT}
        )
        request = DraftRequest(source_text=text, plan=plan, persona=get_persona("dry_midwit_savant"), candidate_count=3)
        fallbacks = build_disagreement_fallbacks(request)
        joined = " ".join(fallbacks).lower()

        self.assertIn("compatibility", joined)
        self.assertNotIn("downvotes", joined)

    def test_disagreement_fallbacks_are_tactic_aware_for_essay_collapse(self) -> None:
        text = "Main compatibility for claude code and opencode is already working."
        analysis = analyze_comment(AnalyzeInput(text=text))
        plan = build_plan(analysis, persona="dry_midwit_savant").model_copy(
            update={
                "selected_objective": TacticalObjective.RESURRECT,
                "selected_tactic": TacticFamily.ESSAY_COLLAPSE,
            }
        )
        request = DraftRequest(source_text=text, plan=plan, persona=get_persona("dry_midwit_savant"), candidate_count=3)
        fallbacks = build_disagreement_fallbacks(request)
        joined = " ".join(fallbacks).lower()

        self.assertIn("quality", joined)
        self.assertIn("integration", joined)


if __name__ == "__main__":
    unittest.main()
