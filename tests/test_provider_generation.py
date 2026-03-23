from __future__ import annotations

from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bait_engine.analysis import AnalyzeInput, analyze_comment
from bait_engine.generation import DraftRequest, MutationSeed, draft_candidates_with_provider
from bait_engine.generation.llm_writer import parse_candidate_lines
from bait_engine.planning import build_plan, get_persona
from bait_engine.providers.base import TextGenerationProvider


class FakeProvider(TextGenerationProvider):
    def __init__(self, text: str, available: bool = True, fail: bool = False):
        self.text = text
        self.available_flag = available
        self.fail = fail
        self.last_system_prompt: str | None = None
        self.last_user_prompt: str | None = None

    def is_available(self) -> bool:
        return self.available_flag

    def generate_candidates(self, *, system_prompt: str, user_prompt: str, candidate_count: int) -> str:
        self.last_system_prompt = system_prompt
        self.last_user_prompt = user_prompt
        if self.fail:
            raise RuntimeError("boom")
        return self.text


class ProviderGenerationTests(unittest.TestCase):
    def test_parse_candidate_lines_handles_numbering(self) -> None:
        raw = "1. first line\n2) second line\n- third line\n2) second line"
        parsed = parse_candidate_lines(raw, 3)
        self.assertEqual(parsed, ["first line", "second line", "third line"])

    def test_provider_path_uses_model_output(self) -> None:
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
                    winner_score=1.37,
                )
            ],
        )
        provider = FakeProvider("1. what answer would even satisfy you here\n2. define the standard first\n3. you're asking questions instead of making a case")
        result = draft_candidates_with_provider(request, provider=provider)
        self.assertGreaterEqual(len(result.candidates), 3)
        self.assertEqual(result.candidates[0].persona, "fake_sincere_questioner")
        self.assertIsNotNone(provider.last_user_prompt)
        self.assertIn("Mutation seeds", provider.last_user_prompt or "")
        self.assertIn("You announced a conclusion before proving a premise.", provider.last_user_prompt or "")

    def test_provider_failure_falls_back_cleanly(self) -> None:
        text = "A model being useful doesn't make it true, and you're still confusing mechanism with necessity."
        analysis = analyze_comment(AnalyzeInput(text=text))
        plan = build_plan(analysis, persona="dry_midwit_savant")
        request = DraftRequest(source_text=text, plan=plan, persona=get_persona("dry_midwit_savant"), candidate_count=3)
        provider = FakeProvider("", available=True, fail=True)
        with self.assertLogs("bait_engine.generation.provider_pipeline", level="WARNING") as captured:
            result = draft_candidates_with_provider(request, provider=provider)
        self.assertGreaterEqual(len(result.candidates), 1)
        logged = "\n".join(captured.output)
        self.assertIn("Provider-backed drafting failed", logged)
        self.assertIn("provider=FakeProvider", logged)
        self.assertIn("persona=dry_midwit_savant", logged)
        self.assertIn(f"objective={request.plan.selected_objective.value}", logged)


if __name__ == "__main__":
    unittest.main()
