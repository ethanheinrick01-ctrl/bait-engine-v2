from __future__ import annotations

import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bait_engine.analysis import AnalyzeInput, analyze_comment


FIXTURES = json.loads((ROOT / "tests" / "fixtures" / "comments.json").read_text())


def top_keys(mapping: dict, limit: int = 3) -> list[str]:
    return [key.value if hasattr(key, "value") else str(key) for key in list(mapping.keys())[:limit]]


def values(items: list) -> list[str]:
    out = []
    for item in items:
        out.append(item.value if hasattr(item, "value") else str(item))
    return out


def axis_lookup(result, name: str):
    for axis in result.axes:
        axis_name = axis.axis.value if hasattr(axis.axis, "value") else str(axis.axis)
        if axis_name == name:
            return axis
    raise KeyError(name)


class AnalysisTests(unittest.TestCase):
    def test_fixture_expectations(self) -> None:
        for fixture in FIXTURES:
            result = analyze_comment(AnalyzeInput(text=fixture["text"], platform="reddit"))
            got_archetypes = top_keys(result.archetype_blend)
            got_contradictions = values([item.type for item in result.contradictions])
            got_objectives = values(result.recommended_objectives)

            for expected in fixture["expect_archetypes"]:
                self.assertIn(expected, got_archetypes, f"missing archetype {expected} for fixture {fixture['name']}")

            for expected in fixture["expect_contradictions"]:
                self.assertIn(expected, got_contradictions, f"missing contradiction {expected} for fixture {fixture['name']}")

            for expected in fixture["expect_objectives"]:
                self.assertIn(expected, got_objectives, f"missing objective {expected} for fixture {fixture['name']}")

    def test_scores_are_bounded(self) -> None:
        result = analyze_comment(AnalyzeInput(text="Why are you so certain this always works?"))
        for axis in result.axes:
            self.assertGreaterEqual(axis.score, 0.0)
            self.assertLessEqual(axis.score, 1.0)
            self.assertGreaterEqual(axis.confidence, 0.0)
            self.assertLessEqual(axis.confidence, 1.0)

        opp = result.opportunity
        for field in opp.model_dump().values():
            self.assertGreaterEqual(field, 0.0)
            self.assertLessEqual(field, 1.0)

    def test_semantic_sarcasm_attenuation(self) -> None:
        sarcastic = analyze_comment(AnalyzeInput(text="oh brilliant take champ, totally foolproof lol"))
        literal = analyze_comment(AnalyzeInput(text="This take is foolproof and definitely correct."))

        self.assertTrue(any("semantic_inversion_detected" in note for note in sarcastic.notes))

        s_certainty = axis_lookup(sarcastic, "certainty")
        l_certainty = axis_lookup(literal, "certainty")
        s_aggression = axis_lookup(sarcastic, "aggression")
        l_aggression = axis_lookup(literal, "aggression")

        self.assertLess(s_certainty.confidence, l_certainty.confidence)
        self.assertLess(s_aggression.confidence, l_aggression.confidence)

        for axis in sarcastic.axes:
            self.assertGreaterEqual(axis.score, 0.0)
            self.assertLessEqual(axis.score, 1.0)

    def test_semantic_quoted_text_separation(self) -> None:
        quoted = analyze_comment(AnalyzeInput(text='You said "I never do that" and then did exactly that.'))
        baseline = analyze_comment(AnalyzeInput(text="You never do that and then did exactly that."))

        self.assertTrue(any("quoted_frame_ratio" in note for note in quoted.notes))
        certainty_q = axis_lookup(quoted, "certainty")
        certainty_b = axis_lookup(baseline, "certainty")

        self.assertLessEqual(certainty_q.confidence, certainty_b.confidence)
        contradiction_hits = values([item.type for item in quoted.contradictions])
        self.assertGreaterEqual(len(contradiction_hits), 1)

    def test_semantic_literal_control_stability(self) -> None:
        literal = analyze_comment(AnalyzeInput(text="Your argument is inconsistent because point A contradicts point B."))
        certainty_axis = axis_lookup(literal, "certainty")

        self.assertFalse(any("semantic_inversion_detected" in note for note in literal.notes))
        self.assertGreaterEqual(certainty_axis.confidence, 0.5)

    def test_description_vs_normativity_requires_explicit_bridge(self) -> None:
        bridged = analyze_comment(AnalyzeInput(text="It is natural, so it should be treated as morally right."))
        co_occurrence_only = analyze_comment(AnalyzeInput(text="This policy is wrong and it does not work."))
        negated_bridge = analyze_comment(AnalyzeInput(text="Just because something is natural doesn't mean it's good."))

        bridged_hits = values([item.type for item in bridged.contradictions])
        co_occurrence_hits = values([item.type for item in co_occurrence_only.contradictions])
        negated_hits = values([item.type for item in negated_bridge.contradictions])

        self.assertIn("description_vs_normativity", bridged_hits)
        self.assertNotIn("description_vs_normativity", co_occurrence_hits)
        self.assertNotIn("description_vs_normativity", negated_hits)

    def test_semantic_deterministic_repeatability(self) -> None:
        text = "oh brilliant take champ, totally foolproof lol"
        runs = [analyze_comment(AnalyzeInput(text=text)).model_dump(mode="json") for _ in range(10)]
        first = runs[0]
        for item in runs[1:]:
            self.assertEqual(item["notes"], first["notes"])
            self.assertEqual(item["archetype_blend"], first["archetype_blend"])
            self.assertEqual(item["axes"], first["axes"])

    def test_semantic_adversarial_boundedness(self) -> None:
        samples = [
            "great job genius lol",
            "sure, because that always works",
            "you called it 'objective' lol",
            "amazing point 😂",
            "what exactly are we pretending this proves?",
            "lol",
        ]
        for text in samples:
            result = analyze_comment(AnalyzeInput(text=text))
            for axis in result.axes:
                self.assertGreaterEqual(axis.score, 0.0)
                self.assertLessEqual(axis.score, 1.0)
                self.assertGreaterEqual(axis.confidence, 0.0)
                self.assertLessEqual(axis.confidence, 1.0)


if __name__ == "__main__":
    unittest.main()
