from __future__ import annotations

from pathlib import Path
import os
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bait_engine.analysis import AnalyzeInput, analyze_comment
from bait_engine.generation import DraftRequest, MutationSeed, draft_candidates_with_provider
from bait_engine.generation.llm_writer import build_provider_prompts, generate_candidates_via_provider, parse_candidate_lines
from bait_engine.planning import build_plan, get_persona
from bait_engine.providers.base import TextGenerationProvider
from bait_engine.providers.openai_compatible import OpenAICompatibleProvider


class FakeProvider(TextGenerationProvider):
    def __init__(
        self,
        text: str,
        available: bool = True,
        fail: bool = False,
        *,
        timeout_fail: bool = False,
        base_url: str | None = None,
    ):
        self.text = text
        self.available_flag = available
        self.fail = fail
        self.timeout_fail = timeout_fail
        self.base_url = base_url
        self.last_system_prompt: str | None = None
        self.last_user_prompt: str | None = None
        self.calls = 0

    def is_available(self) -> bool:
        return self.available_flag

    def generate_candidates(self, *, system_prompt: str, user_prompt: str, candidate_count: int) -> str:
        self.calls += 1
        self.last_system_prompt = system_prompt
        self.last_user_prompt = user_prompt
        if self.timeout_fail:
            raise TimeoutError("timed out")
        if self.fail:
            raise RuntimeError("boom")
        return self.text


class ProviderGenerationTests(unittest.TestCase):
    def test_openai_compatible_provider_uses_longer_default_timeout_for_local_ollama(self) -> None:
        with patch.dict(os.environ, {"OPENAI_BASE_URL": "http://127.0.0.1:11434/v1"}, clear=False):
            provider = OpenAICompatibleProvider(api_key="ollama")
        self.assertEqual(provider.timeout_seconds, 75)

    def test_openai_compatible_provider_env_timeout_overrides_defaults(self) -> None:
        with patch.dict(
            os.environ,
            {
                "OPENAI_BASE_URL": "http://127.0.0.1:11434/v1",
                "BAIT_ENGINE_TIMEOUT_SECONDS": "75",
            },
            clear=False,
        ):
            provider = OpenAICompatibleProvider(api_key="ollama")
        self.assertEqual(provider.timeout_seconds, 75)

    def test_openai_compatible_provider_defaults_to_30_seconds_for_non_local_endpoints(self) -> None:
        with patch.dict(os.environ, {"OPENAI_BASE_URL": "https://api.openai.com/v1"}, clear=False):
            provider = OpenAICompatibleProvider(api_key="sk-test")
        self.assertEqual(provider.timeout_seconds, 30)

    def test_parse_candidate_lines_handles_numbering(self) -> None:
        raw = "1. first line\n2) second line\n- third line\n2) second line"
        parsed = parse_candidate_lines(raw, 3)
        self.assertEqual(parsed, ["first line", "second line", "third line"])

    def test_parse_candidate_lines_handles_json_array(self) -> None:
        raw = "[\"first line\", \"second line\", \"third line\"]"
        parsed = parse_candidate_lines(raw, 3)
        self.assertEqual(parsed, ["first line", "second line", "third line"])

    def test_build_provider_prompts_use_structured_newlines(self) -> None:
        text = "A model being useful does not make it true, and you are confusing mechanism with necessity."
        analysis = analyze_comment(AnalyzeInput(text=text))
        plan = build_plan(analysis, persona="dry_midwit_savant")
        request = DraftRequest(
            source_text=text,
            plan=plan,
            persona=get_persona("dry_midwit_savant"),
            candidate_count=3,
            mutation_seeds=[
                MutationSeed(
                    text="You announced a conclusion before proving a premise.",
                    transform="compress",
                    tactic=plan.selected_tactic.value if plan.selected_tactic else None,
                    objective=plan.selected_objective.value,
                )
            ],
        )
        _, user_prompt = build_provider_prompts(request)
        self.assertIn("Source text:\n", user_prompt)
        self.assertIn("Persona:\n- name: dry_midwit_savant", user_prompt)
        self.assertIn("Return format:\n- Return exactly 3 distinct candidates.", user_prompt)
        self.assertIn("Weave guidance:\n- Make the first three candidates complementary enough to fuse into one reply.", user_prompt)
        self.assertIn("Mutation seeds", user_prompt)
        self.assertNotIn("\\n", user_prompt)
        self.assertNotIn("{'name':", user_prompt)

    def test_provider_generation_assigns_weave_roles_to_first_three_candidates(self) -> None:
        text = "A model being useful does not make it true, and you are confusing mechanism with necessity."
        analysis = analyze_comment(AnalyzeInput(text=text))
        plan = build_plan(analysis, persona="dry_midwit_savant")
        request = DraftRequest(source_text=text, plan=plan, persona=get_persona("dry_midwit_savant"), candidate_count=3)
        provider = FakeProvider(
            "1. useful does not make it true, that's the lead gap\n"
            "2. mechanism still isn't necessity, and that's the pressure point\n"
            "3. useful does not make it true, that's the sting you're hiding"
        )
        candidates = generate_candidates_via_provider(request, provider)
        self.assertEqual([item.weave_role for item in candidates[:3]], ["lead", "support", "sting"])

    def test_provider_path_uses_model_output_and_marks_source(self) -> None:
        text = "A model being useful does not make it true, and you are confusing mechanism with necessity."
        analysis = analyze_comment(AnalyzeInput(text=text))
        plan = build_plan(analysis, persona="dry_midwit_savant")
        request = DraftRequest(
            source_text=text,
            plan=plan,
            persona=get_persona("dry_midwit_savant"),
            candidate_count=3,
            mutation_seeds=[
                MutationSeed(
                    text="You announced a conclusion before proving a premise.",
                    transform="compress",
                    tactic=plan.selected_tactic.value if plan.selected_tactic else None,
                    objective=plan.selected_objective.value,
                )
            ],
        )
        provider = FakeProvider(
            "1. useful does not make it true, so why are you treating mechanism like necessity?\n"
            "2. if usefulness proved truth, mechanism versus necessity would stop mattering, right?\n"
            "3. you keep swapping usefulness for truth and calling that a mechanism?"
        )
        result = draft_candidates_with_provider(request, provider=provider)

        self.assertEqual([item.generation_source for item in result.candidates], ["provider", "provider", "provider"])
        self.assertIn("Mutation seeds", provider.last_user_prompt or "")
        self.assertTrue(any("mechanism" in item.text.lower() for item in result.candidates))

    def test_provider_failure_falls_back_cleanly_and_marks_fallback(self) -> None:
        text = "A model being useful does not make it true, and you are confusing mechanism with necessity."
        analysis = analyze_comment(AnalyzeInput(text=text))
        plan = build_plan(analysis, persona="dry_midwit_savant")
        request = DraftRequest(source_text=text, plan=plan, persona=get_persona("dry_midwit_savant"), candidate_count=3)
        provider = FakeProvider("", available=True, fail=True)
        with self.assertLogs("bait_engine.generation.provider_pipeline", level="WARNING") as captured:
            result = draft_candidates_with_provider(request, provider=provider)
        self.assertGreaterEqual(len(result.candidates), 1)
        self.assertTrue(all(item.generation_source == "heuristic_fallback" for item in result.candidates))
        logged = "\n".join(captured.output)
        self.assertIn("Provider-backed drafting failed", logged)
        self.assertIn("provider=FakeProvider", logged)
        self.assertIn("persona=dry_midwit_savant", logged)
        self.assertIn(f"objective={request.plan.selected_objective.value}", logged)

    def test_provider_local_ollama_timeout_does_not_retry(self) -> None:
        text = "A model being useful does not make it true."
        analysis = analyze_comment(AnalyzeInput(text=text))
        plan = build_plan(analysis, persona="dry_midwit_savant")
        request = DraftRequest(source_text=text, plan=plan, persona=get_persona("dry_midwit_savant"), candidate_count=3)
        provider = FakeProvider(
            "",
            available=True,
            timeout_fail=True,
            base_url="http://127.0.0.1:11434/v1",
        )
        draft_candidates_with_provider(request, provider=provider)
        self.assertEqual(provider.calls, 1)

    def test_provider_non_local_runtime_error_retries_once(self) -> None:
        text = "A model being useful does not make it true."
        analysis = analyze_comment(AnalyzeInput(text=text))
        plan = build_plan(analysis, persona="dry_midwit_savant")
        request = DraftRequest(source_text=text, plan=plan, persona=get_persona("dry_midwit_savant"), candidate_count=3)
        provider = FakeProvider(
            "",
            available=True,
            fail=True,
            base_url="https://api.openai.com/v1",
        )
        draft_candidates_with_provider(request, provider=provider)
        self.assertEqual(provider.calls, 2)


if __name__ == "__main__":
    unittest.main()
