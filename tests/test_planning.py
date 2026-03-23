from __future__ import annotations

from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bait_engine.analysis import AnalyzeInput, analyze_comment
from bait_engine.core.types import BranchClass, TacticalObjective, TacticFamily
from bait_engine.planning import build_plan, select_persona
from bait_engine.planning.personas import PersonaProfile


class PlanningTests(unittest.TestCase):
    def test_aggressive_target_prefers_calm_or_exit(self) -> None:
        result = analyze_comment(
            AnalyzeInput(text="This is obviously stupid and you're a clown if you believe it. Absolute moronic garbage.")
        )
        plan = build_plan(result, persona="calm_unbothered_ghoul")
        self.assertIn(plan.selected_objective, {TacticalObjective.EXIT_ON_TOP, TacticalObjective.TILT, TacticalObjective.HOOK})
        self.assertIn(plan.selected_tactic, {TacticFamily.CALM_REDUCTION, TacticFamily.LABEL_AND_LEAVE})

    def test_contradiction_rich_input_prefers_structural_tactics(self) -> None:
        result = analyze_comment(
            AnalyzeInput(
                text="A model being useful means it's true enough, and if the mechanism explains it then it's basically necessary."
            )
        )
        plan = build_plan(result, persona="dry_midwit_savant")
        self.assertIn(plan.selected_tactic, {TacticFamily.ESSAY_COLLAPSE, TacticFamily.BURDEN_REVERSAL, TacticFamily.SCHOLAR_HEX})
        self.assertTrue(any(item.branch in {BranchClass.DENIAL, BranchClass.ESSAY_DEFENSE} for item in plan.branch_forecast))

    def test_sealion_prefers_reverse_interrogation(self) -> None:
        result = analyze_comment(
            AnalyzeInput(text="What exactly do you mean by that? How do you know? Why should anyone accept that framing?")
        )
        plan = build_plan(result, persona="fake_sincere_questioner")
        self.assertIn(plan.selected_tactic, {TacticFamily.REVERSE_INTERROGATION, TacticFamily.FAKE_CLARIFICATION})

    def test_low_value_input_stays_do_not_engage(self) -> None:
        result = analyze_comment(AnalyzeInput(text="lol nah"))
        plan = build_plan(result)
        self.assertEqual(plan.selected_objective, TacticalObjective.DO_NOT_ENGAGE)
        self.assertIsNone(plan.selected_tactic)
        self.assertEqual(plan.exit_state, "abandon")

    def test_persona_restriction_filters_forbidden_tactic(self) -> None:
        result = analyze_comment(AnalyzeInput(text="Counterpoint: if we keep following this logic then forks are authoritarian metaphysical artifacts."))
        plan = build_plan(result, persona="calm_unbothered_ghoul")
        self.assertNotEqual(plan.selected_tactic, TacticFamily.ABSURDIST_DERAIL)

    def test_persona_router_is_deterministic_for_identical_input(self) -> None:
        analysis = analyze_comment(
            AnalyzeInput(text="You're certainty-maxing a claim with no mechanism and pretending that confidence is evidence.")
        )
        first = select_persona(analysis, platform="reddit")
        second = select_persona(analysis, platform="reddit")

        self.assertEqual(first.model_dump(mode="json"), second.model_dump(mode="json"))
        self.assertTrue(first.selected_persona)
        self.assertIn(first.selected_persona, first.persona_scores)

    def test_persona_router_close_scores_uses_fallback(self) -> None:
        analysis = analyze_comment(AnalyzeInput(text="Maybe maybe maybe; what if maybe this maybe maybe maybe."))
        personas = {
            "alpha": PersonaProfile(name="alpha", pressure_profile="surgical_pinch"),
            "beta": PersonaProfile(name="beta", pressure_profile="surgical_pinch"),
        }
        decision = select_persona(
            analysis,
            platform="reddit",
            personas=personas,
            fallback_persona="beta",
            close_margin=0.25,
        )

        self.assertEqual(decision.selected_persona, "beta")
        self.assertTrue(any("duel_inconclusive=fallback" in note for note in decision.why_selected))

    def test_persona_router_close_score_duel_can_pick_non_fallback(self) -> None:
        analysis = analyze_comment(AnalyzeInput(text="This is loud and hostile nonsense pretending to be certainty."))
        personas = {
            "alpha": PersonaProfile(name="alpha", calmness_preference=0.95, absurdity_tolerance=0.15),
            "beta": PersonaProfile(name="beta", calmness_preference=0.30, absurdity_tolerance=0.85),
        }
        decision = select_persona(
            analysis,
            platform="reddit",
            personas=personas,
            fallback_persona="beta",
            close_margin=0.5,
        )

        self.assertEqual(decision.selected_persona, "alpha")
        self.assertTrue(any(note.startswith("duel_winner=alpha") for note in decision.why_selected))


if __name__ == "__main__":
    unittest.main()
