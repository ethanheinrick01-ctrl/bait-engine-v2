from __future__ import annotations

from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bait_engine.analysis import AnalyzeInput, analyze_comment
from bait_engine.generation import DraftRequest, MutationSeed, build_prompt_payload, draft_candidates
from bait_engine.generation.writer import PERSONA_STYLE_PACKS, reset_style_memory
from bait_engine.planning import build_plan, get_persona


class GenerationTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_style_memory()

    def test_draft_pipeline_produces_ranked_candidates(self) -> None:
        text = "A model being useful doesn't make it true, and you're still confusing mechanism with necessity."
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

    def test_mutation_seeds_feed_local_generation_pool(self) -> None:
        text = "A model being useful doesn't make it true, and you're still confusing mechanism with necessity."
        analysis = analyze_comment(AnalyzeInput(text=text))
        plan = build_plan(analysis, persona="dry_midwit_savant")
        request = DraftRequest(
            source_text=text,
            plan=plan,
            persona=get_persona("dry_midwit_savant"),
            candidate_count=3,
            mutation_seeds=[
                MutationSeed(
                    text="You skipped the premise and sprinted to certainty.",
                    transform="compress",
                    tactic=plan.selected_tactic.value if plan.selected_tactic else None,
                    objective=plan.selected_objective.value,
                )
            ],
        )
        result = draft_candidates(request)
        self.assertGreaterEqual(len(result.candidates), 1)
        self.assertTrue(any("skipped the premise" in item.text.lower() for item in result.candidates))

    def test_pressure_profile_shapes_candidate_language(self) -> None:
        text = "What exactly do you mean by that and why should anyone buy it?"
        analysis = analyze_comment(AnalyzeInput(text=text))
        plan = build_plan(analysis, persona="fake_sincere_questioner")
        request = DraftRequest(source_text=text, plan=plan, persona=get_persona("fake_sincere_questioner"), candidate_count=2)
        result = draft_candidates(request)
        self.assertGreaterEqual(len(result.candidates), 1)
        # velvet_snare pressure profile prepends "quick question" on ~50% of candidates;
        # the persona's own style (fake_sincere_questioner) always shows through.
        persona_markers = ("genuinely", "honest", "curious", "clarify", "walk me", "help me", "trying to follow", "genuine question", "quick question", "sanity check")
        self.assertTrue(any(
            any(marker in item.text.lower() for marker in persona_markers)
            for item in result.candidates
        ))

    def test_persona_flavor_inventory_expanded(self) -> None:
        for persona_name, style_pack in PERSONA_STYLE_PACKS.items():
            suffixes = style_pack["suffixes"]
            if persona_name == "dry_midwit_savant":
                self.assertGreaterEqual(len(suffixes), 50)
            else:
                self.assertGreaterEqual(len(suffixes), 40)

    def test_adjacent_candidates_avoid_style_repetition(self) -> None:
        text = "You keep claiming confidence is evidence and that's not how evidence works."
        analysis = analyze_comment(AnalyzeInput(text=text))
        plan = build_plan(analysis, persona="dry_midwit_savant")
        request = DraftRequest(source_text=text, plan=plan, persona=get_persona("dry_midwit_savant"), candidate_count=10)
        result = draft_candidates(request)

        normalized = [" ".join(item.text.lower().split()) for item in result.candidates]
        adjacent_duplicates = sum(1 for idx in range(1, len(normalized)) if normalized[idx] == normalized[idx - 1])
        self.assertEqual(adjacent_duplicates, 0)

    def test_adjacent_generations_reduce_first_candidate_repetition(self) -> None:
        text = "A model being useful doesn't make it true, and you're still confusing mechanism with necessity."
        analysis = analyze_comment(AnalyzeInput(text=text))
        plan = build_plan(analysis, persona="dry_midwit_savant")
        persona = get_persona("dry_midwit_savant")

        first_candidates: list[str] = []
        for idx in range(8):
            request = DraftRequest(
                source_text=text,
                plan=plan,
                persona=persona,
                candidate_count=3,
                mutation_context=f"adjacent-run-{idx}",
            )
            result = draft_candidates(request)
            first_candidates.append(" ".join(result.candidates[0].text.lower().split()))

        adjacent_duplicates = sum(1 for idx in range(1, len(first_candidates)) if first_candidates[idx] == first_candidates[idx - 1])
        self.assertLessEqual(adjacent_duplicates, 1)

    def test_do_not_engage_yields_no_candidates(self) -> None:
        text = "lol nah"
        analysis = analyze_comment(AnalyzeInput(text=text))
        plan = build_plan(analysis)
        request = DraftRequest(source_text=text, plan=plan, persona=get_persona(), candidate_count=3)
        result = draft_candidates(request)
        self.assertEqual(result.candidates, [])


if __name__ == "__main__":
    unittest.main()
