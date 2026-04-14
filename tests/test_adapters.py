from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bait_engine.adapters import InboundThreadContext, build_emit_request, build_preview_panel, build_reply_envelope, get_adapter, list_adapters, normalize_target, recommend_selection_preset, resolve_selection_preset, select_candidate, summarize_thread_context, validate_target
from bait_engine.adapters.contracts import AdapterSelectionPreset
from bait_engine.storage import RunRepository


class AdapterTests(unittest.TestCase):
    def test_registry_lists_known_adapters(self) -> None:
        adapters = list_adapters()
        names = {adapter["name"] for adapter in adapters}
        self.assertIn("reddit", names)
        self.assertIn("discord", names)

        x_adapter = get_adapter("x")
        self.assertEqual(x_adapter["platform"], "x")
        self.assertTrue(x_adapter["capabilities"]["supports_media"])
        self.assertEqual(x_adapter["default_selection_preset"], "audience")
        self.assertTrue(x_adapter["selection_presets"])

    def test_normalize_target_by_platform(self) -> None:
        reddit = normalize_target("reddit", reply_to_id=" t3_deadbeef ", author_handle="/u/NecroLad ")
        self.assertEqual(reddit.thread_id, "t3_deadbeef")
        self.assertEqual(reddit.reply_to_id, "t3_deadbeef")
        self.assertEqual(reddit.author_handle, "NecroLad")

        x_target = normalize_target("x", author_handle="@LichPoster")
        self.assertEqual(x_target.author_handle, "LichPoster")

        discord = normalize_target("discord", author_handle="  Mage#1337 ")
        self.assertEqual(discord.author_handle, "mage#1337")

    def test_selection_presets_resolve_by_platform(self) -> None:
        reddit = resolve_selection_preset("reddit", "engage")
        self.assertEqual(reddit["strategy"], "highest_bite")
        self.assertEqual(reddit["objective"], "tilt")

        discord = resolve_selection_preset("discord")
        self.assertEqual(discord["name"], "safe")
        self.assertEqual(discord["strategy"], "lowest_penalty")

    def test_recommend_selection_preset_from_context(self) -> None:
        reddit = recommend_selection_preset(
            "reddit",
            InboundThreadContext.model_validate(
                {
                    "platform": "reddit",
                    "thread_id": "r1",
                    "messages": [
                        {"message_id": "m1", "body": "lol you're stupid", "author_handle": "grave1"},
                        {"message_id": "m2", "body": "cope harder moron", "author_handle": "grave2"},
                    ],
                }
            ),
        )
        self.assertEqual(reddit["name"], "safe")
        self.assertEqual(reddit["metrics"]["unique_authors"], 2)

        x = recommend_selection_preset(
            "x",
            InboundThreadContext.model_validate(
                {
                    "platform": "x",
                    "thread_id": "x1",
                    "metadata": {"participant_count": 4},
                    "messages": [
                        {"message_id": "m1", "body": "one", "author_handle": "a", "metadata": {"like_count": 2}},
                        {"message_id": "m2", "body": "two", "author_handle": "b", "metadata": {"like_count": 3}},
                    ],
                }
            ),
        )
        self.assertEqual(x["name"], "audience")
        self.assertGreaterEqual(x["metrics"]["audience_signal"], 5)

    def test_recommend_selection_preset_uses_discord_room_and_root_author_signals(self) -> None:
        discord = recommend_selection_preset(
            "discord",
            InboundThreadContext.model_validate(
                {
                    "platform": "discord",
                    "thread_id": "d1",
                    "subject": "crypt argument",
                    "root_author_handle": "necrolord",
                    "metadata": {"participant_count": 5},
                    "messages": [
                        {"message_id": "m1", "body": "opening", "author_handle": "necrolord"},
                        {"message_id": "m2", "body": "second", "author_handle": "boneboy", "metadata": {"reaction_count": 4}},
                        {"message_id": "m3", "body": "third", "author_handle": "ghoulgal"},
                    ],
                }
            ),
        )
        self.assertEqual(discord["name"], "audience")
        self.assertTrue(discord["metrics"]["root_author_present"])
        self.assertTrue(discord["metrics"]["has_subject"])

    def test_candidate_selection_strategies(self) -> None:
        candidates = [
            {"rank_index": 1, "text": "a", "tactic": "calm_reduction", "objective": "tilt", "rank_score": 0.7, "estimated_bite_score": 0.4, "estimated_audience_score": 0.2, "critic_penalty": 0.3},
            {"rank_index": 2, "text": "b", "tactic": "reverse_interrogation", "objective": "tilt", "rank_score": 0.6, "estimated_bite_score": 0.9, "estimated_audience_score": 0.5, "critic_penalty": 0.4},
            {"rank_index": 3, "text": "c", "tactic": "calm_reduction", "objective": "audience_win", "rank_score": 0.5, "estimated_bite_score": 0.3, "estimated_audience_score": 0.95, "critic_penalty": 0.1},
        ]
        self.assertEqual(select_candidate(candidates, strategy="rank", candidate_rank_index=2)["text"], "b")
        self.assertEqual(select_candidate(candidates, strategy="top_score")["text"], "a")
        self.assertEqual(select_candidate(candidates, strategy="highest_bite")["text"], "b")
        self.assertEqual(select_candidate(candidates, strategy="highest_audience")["text"], "c")
        self.assertEqual(select_candidate(candidates, strategy="lowest_penalty")["text"], "c")
        self.assertEqual(select_candidate(candidates, strategy="top_score", tactic="calm_reduction")["text"], "a")
        self.assertEqual(select_candidate(candidates, strategy="highest_audience", objective="audience_win")["text"], "c")

    def test_candidate_selection_auto_best_balances_history_with_sample_size(self) -> None:
        candidates = [
            {
                "rank_index": 1,
                "text": "high-rate-low-sample",
                "tactic": "calm_reduction",
                "objective": "tilt",
                "rank_score": 0.82,
                "estimated_bite_score": 0.31,
                "estimated_audience_score": 0.25,
                "critic_penalty": 0.08,
            },
            {
                "rank_index": 2,
                "text": "stable-winner",
                "tactic": "reverse_interrogation",
                "objective": "tilt",
                "rank_score": 0.74,
                "estimated_bite_score": 0.66,
                "estimated_audience_score": 0.58,
                "critic_penalty": 0.14,
            },
        ]
        reputation = {
            "reply_rate": 0.31,
            "total_runs": 22,
            "tactic_performance": {
                "calm_reduction": {"count": 2, "replies": 2, "rate": 1.0},
                "reverse_interrogation": {"count": 16, "replies": 7, "rate": 0.4375},
            },
        }

        picked = select_candidate(candidates, strategy="auto_best", reputation_data=reputation)
        self.assertEqual(picked["text"], "stable-winner")

    def test_candidate_selection_auto_best_prefers_delivery_verified_tactic(self) -> None:
        candidates = [
            {
                "rank_index": 1,
                "text": "fast-but-unverified",
                "tactic": "calm_reduction",
                "objective": "tilt",
                "rank_score": 0.79,
                "estimated_bite_score": 0.57,
                "estimated_audience_score": 0.53,
                "critic_penalty": 0.08,
            },
            {
                "rank_index": 2,
                "text": "verified-consistent",
                "tactic": "reverse_interrogation",
                "objective": "tilt",
                "rank_score": 0.76,
                "estimated_bite_score": 0.61,
                "estimated_audience_score": 0.56,
                "critic_penalty": 0.1,
            },
        ]
        reputation = {
            "reply_rate": 0.44,
            "total_runs": 30,
            "delivery_confidence": 0.7,
            "tactic_performance": {
                "calm_reduction": {
                    "count": 10,
                    "replies": 3,
                    "rate": 0.3,
                    "avg_engagement": 2.4,
                    "avg_reply_delay": 95,
                    "delivery_confidence": 0.0,
                },
                "reverse_interrogation": {
                    "count": 14,
                    "replies": 6,
                    "rate": 0.43,
                    "avg_engagement": 4.2,
                    "avg_reply_delay": 140,
                    "delivery_confidence": 0.86,
                },
            },
        }

        picked = select_candidate(candidates, strategy="auto_best", reputation_data=reputation)
        self.assertEqual(picked["text"], "verified-consistent")

    def test_recommend_selection_preset_requires_sample_before_auto_best(self) -> None:
        context = InboundThreadContext.model_validate(
            {
                "platform": "reddit",
                "thread_id": "r2",
                "messages": [
                    {"message_id": "m1", "body": "cope", "author_handle": "a"},
                    {"message_id": "m2", "body": "you're wrong", "author_handle": "b"},
                ],
            }
        )
        weak_sample = {"reply_rate": 0.9, "total_runs": 2, "tactic_performance": {"calm_reduction": {"count": 2, "replies": 2, "rate": 1.0}}}
        strong_sample = {"reply_rate": 0.32, "total_runs": 14, "tactic_performance": {"calm_reduction": {"count": 9, "replies": 3, "rate": 0.33}}}

        weak = recommend_selection_preset("reddit", context, reputation_data=weak_sample)
        strong = recommend_selection_preset("reddit", context, reputation_data=strong_sample)

        self.assertNotEqual(weak["strategy"], "auto_best")
        self.assertEqual(strong["strategy"], "auto_best")

    def test_build_reply_envelope_auto_best_includes_rationale(self) -> None:
        run = {
            "id": 88,
            "platform": "reddit",
            "persona": "dry_midwit_savant",
            "selected_objective": "tilt",
            "selected_tactic": "calm_reduction",
            "exit_state": "one_more_spike",
            "candidates": [
                {
                    "text": "high-rate-low-sample",
                    "rank_index": 1,
                    "objective": "tilt",
                    "tactic": "calm_reduction",
                    "rank_score": 0.82,
                    "estimated_bite_score": 0.31,
                    "estimated_audience_score": 0.25,
                    "critic_penalty": 0.08,
                },
                {
                    "text": "verified-consistent",
                    "rank_index": 2,
                    "objective": "tilt",
                    "tactic": "reverse_interrogation",
                    "rank_score": 0.74,
                    "estimated_bite_score": 0.66,
                    "estimated_audience_score": 0.58,
                    "critic_penalty": 0.14,
                },
            ],
        }
        reputation = {
            "reply_rate": 0.44,
            "total_runs": 30,
            "delivery_confidence": 0.7,
            "tactic_performance": {
                "calm_reduction": {"count": 2, "replies": 2, "rate": 1.0, "delivery_confidence": 0.0},
                "reverse_interrogation": {"count": 14, "replies": 6, "rate": 0.43, "delivery_confidence": 0.86},
            },
        }

        envelope = build_reply_envelope(
            run,
            selection_strategy="auto_best",
            thread_id="t3_deadbeef",
            reply_to_id="t1_abc",
            reputation_data=reputation,
        )

        rationale = (envelope.get("metadata") or {}).get("auto_best_rationale")
        self.assertIsNotNone(rationale)
        self.assertEqual(rationale["tactic"], "reverse_interrogation")
        self.assertGreater(rationale["delivery_confidence"], 0.0)

    def test_build_reply_envelope_blend_top3_composes_single_body(self) -> None:
        run = {
            "id": 101,
            "platform": "reddit",
            "persona": "dry_midwit_savant",
            "selected_objective": "tilt",
            "selected_tactic": "calm_reduction",
            "exit_state": "one_more_spike",
            "candidates": [
                {
                    "text": "That framing is lazy and ignores basic funding mechanics.",
                    "rank_index": 1,
                    "objective": "tilt",
                    "tactic": "calm_reduction",
                    "rank_score": 0.91,
                    "estimated_bite_score": 0.52,
                    "estimated_audience_score": 0.44,
                    "critic_penalty": 0.08,
                },
                {
                    "text": "Private donations do not replace public priorities.",
                    "rank_index": 2,
                    "objective": "tilt",
                    "tactic": "calm_reduction",
                    "rank_score": 0.88,
                    "estimated_bite_score": 0.49,
                    "estimated_audience_score": 0.41,
                    "critic_penalty": 0.09,
                },
                {
                    "text": "Waving 'pay for it yourself' as a fix is rhetorical cosplay.",
                    "rank_index": 3,
                    "objective": "tilt",
                    "tactic": "calm_reduction",
                    "rank_score": 0.86,
                    "estimated_bite_score": 0.53,
                    "estimated_audience_score": 0.39,
                    "critic_penalty": 0.11,
                },
            ],
        }

        envelope = build_reply_envelope(
            run,
            selection_strategy="blend_top3",
            thread_id="t3_deadbeef",
            reply_to_id="t1_abc",
        )

        body = envelope["body"]
        self.assertIn("funding mechanics", body)
        self.assertIn("private donations", body.lower())
        self.assertIn("rhetorical cosplay", body)
        metadata = envelope.get("metadata") or {}
        self.assertTrue(metadata.get("combined_top_candidates"))
        self.assertEqual(metadata.get("composition_strategy"), "blend_top3")
        self.assertEqual(metadata.get("combined_candidate_rank_indexes"), [1, 2, 3])
        self.assertEqual(metadata.get("selected_candidate_text"), "That framing is lazy and ignores basic funding mechanics.")
        self.assertTrue(metadata.get("emitted_body_differs_from_selected_candidate"))

    def test_build_reply_envelope_mega_bait_uses_weave_strategy(self) -> None:
        run = {
            "id": 103,
            "platform": "reddit",
            "persona": "dry_midwit_savant",
            "selected_objective": "tilt",
            "selected_tactic": "calm_reduction",
            "exit_state": "one_more_spike",
            "candidates": [
                {
                    "text": "That framing is lazy and ignores basic funding mechanics.",
                    "rank_index": 1,
                    "objective": "tilt",
                    "tactic": "calm_reduction",
                    "rank_score": 0.91,
                    "estimated_bite_score": 0.52,
                    "estimated_audience_score": 0.44,
                    "critic_penalty": 0.08,
                },
                {
                    "text": "Private donations do not replace public priorities.",
                    "rank_index": 2,
                    "objective": "tilt",
                    "tactic": "calm_reduction",
                    "rank_score": 0.88,
                    "estimated_bite_score": 0.49,
                    "estimated_audience_score": 0.41,
                    "critic_penalty": 0.09,
                },
                {
                    "text": "Waving 'pay for it yourself' as a fix is rhetorical cosplay.",
                    "rank_index": 3,
                    "objective": "tilt",
                    "tactic": "calm_reduction",
                    "rank_score": 0.86,
                    "estimated_bite_score": 0.53,
                    "estimated_audience_score": 0.39,
                    "critic_penalty": 0.11,
                },
            ],
        }

        envelope = build_reply_envelope(
            run,
            selection_strategy="mega_bait",
            thread_id="t3_deadbeef",
            reply_to_id="t1_abc",
        )

        body = envelope["body"]
        self.assertIn("funding mechanics", body)
        self.assertIn("private donations", body.lower())
        self.assertIn("rhetorical cosplay", body)
        metadata = envelope.get("metadata") or {}
        self.assertEqual(metadata.get("composition_strategy"), "mega_bait")
        self.assertTrue(metadata.get("combined_top_candidates"))
        self.assertEqual(metadata.get("combined_candidate_rank_indexes"), [1, 2, 3])

    def test_build_reply_envelope_mega_bait_skips_low_signal_framing_clauses(self) -> None:
        run = {
            "id": 104,
            "platform": "reddit",
            "persona": "dry_midwit_savant",
            "selected_objective": "tilt",
            "selected_tactic": "calm_reduction",
            "exit_state": "one_more_spike",
            "candidates": [
                {
                    "text": "that conclusion doesn't actually follow",
                    "rank_index": 1,
                    "objective": "tilt",
                    "tactic": "calm_reduction",
                    "weave_role": "lead",
                    "rank_score": 0.91,
                },
                {
                    "text": "you're skipping the missing step",
                    "rank_index": 2,
                    "objective": "tilt",
                    "tactic": "calm_reduction",
                    "weave_role": "support",
                    "rank_score": 0.88,
                },
                {
                    "text": "that's a neat framing. that's the gap",
                    "rank_index": 3,
                    "objective": "tilt",
                    "tactic": "calm_reduction",
                    "weave_role": "sting",
                    "rank_score": 0.86,
                },
            ],
        }

        envelope = build_reply_envelope(
            run,
            selection_strategy="mega_bait",
            thread_id="t3_deadbeef",
            reply_to_id="t1_abc",
        )

        body = envelope["body"].lower()
        self.assertIn("that's the gap", body)
        self.assertNotIn("that's a neat framing", body)

    def test_build_reply_envelope_surfaces_selection_filter_fallback(self) -> None:
        run = {
            "id": 102,
            "platform": "reddit",
            "persona": "dry_midwit_savant",
            "selected_objective": "tilt",
            "selected_tactic": "calm_reduction",
            "exit_state": "one_more_spike",
            "candidates": [
                {
                    "text": "winner text",
                    "rank_index": 1,
                    "objective": "tilt",
                    "tactic": "calm_reduction",
                    "rank_score": 0.9,
                    "estimated_bite_score": 0.5,
                    "estimated_audience_score": 0.4,
                    "critic_penalty": 0.1,
                },
                {
                    "text": "runner up",
                    "rank_index": 2,
                    "objective": "tilt",
                    "tactic": "calm_reduction",
                    "rank_score": 0.7,
                    "estimated_bite_score": 0.4,
                    "estimated_audience_score": 0.35,
                    "critic_penalty": 0.2,
                },
            ],
        }

        envelope = build_reply_envelope(
            run,
            selection_strategy="top_score",
            tactic="reverse_interrogation",
            objective="audience_win",
            thread_id="t3_deadbeef",
            reply_to_id="t1_abc",
        )

        metadata = envelope.get("metadata") or {}
        self.assertTrue(metadata.get("selection_filter_fallback"))
        self.assertEqual(metadata.get("selected_candidate_text"), "winner text")
        self.assertFalse(metadata.get("emitted_body_differs_from_selected_candidate"))
        self.assertEqual(envelope["body"], "winner text")

    def test_validate_target_enforces_capabilities(self) -> None:
        validate_target(normalize_target("reddit", reply_to_id="t1_dead"))

        with self.assertRaises(ValueError):
            validate_target(normalize_target("web", thread_id="thread-1"))

        with self.assertRaises(ValueError):
            validate_target(normalize_target("web"))

    def test_thread_context_summary(self) -> None:
        summary = summarize_thread_context(
            InboundThreadContext.model_validate(
                {
                    "platform": "reddit",
                    "thread_id": "thread-1",
                    "subject": "debate crypt",
                    "messages": [
                        {"message_id": "m1", "body": "first"},
                        {"message_id": "m2", "body": "second"},
                        {"message_id": "m3", "body": "third"},
                    ],
                }
            ),
            max_messages=2,
        )
        self.assertEqual(summary["platform"], "reddit")
        self.assertEqual(summary["message_count"], 3)
        self.assertEqual(len(summary["recent_messages"]), 2)

    def test_build_reply_envelope_uses_selected_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bait.db"
            repo = RunRepository(str(db_path))
            saved = repo.create_run_from_text(
                text="A model being useful doesn't make it true, and you're still confusing mechanism with necessity.",
                persona_name="dry_midwit_savant",
                platform="reddit",
                candidate_count=3,
                heuristic_only=True,
            )

            envelope = build_reply_envelope(
                saved,
                candidate_rank_index=1,
                selection_preset="default",
                thread_id="thread-1",
                reply_to_id="comment-9",
                author_handle="loudguy",
                context={
                    "platform": "reddit",
                    "thread_id": "thread-1",
                    "messages": [
                        {"message_id": "m1", "body": "opening nonsense"},
                        {"message_id": "m2", "body": "escalating nonsense"},
                    ],
                },
            )

        self.assertEqual(envelope["action"], "reply")
        self.assertEqual(envelope["platform"], "reddit")
        self.assertGreaterEqual(envelope["candidate_rank_index"], 1)
        self.assertEqual(envelope["target"]["thread_id"], "thread-1")
        self.assertEqual(envelope["target"]["reply_to_id"], "comment-9")
        self.assertEqual(envelope["target"]["author_handle"], "loudguy")
        self.assertTrue(envelope["body"])
        self.assertIn("candidate_rank_score", envelope["metadata"])
        self.assertEqual(envelope["metadata"]["selection_strategy"], "top_score")
        self.assertEqual(envelope["metadata"]["selection_preset"], "default")
        self.assertEqual(envelope["metadata"]["preferred_dispatch_driver"], "reddit_api")
        self.assertEqual(envelope["metadata"]["thread_context"]["message_count"], 2)
        self.assertEqual(envelope["metadata"]["selected_candidate_text"], envelope["body"])
        self.assertFalse(envelope["metadata"]["emitted_body_differs_from_selected_candidate"])

    def test_emit_request_and_preview_panel(self) -> None:
        run = {
            "id": 7,
            "platform": "reddit",
            "persona": "dry_midwit_savant",
            "selected_objective": "tilt",
            "selected_tactic": "calm_reduction",
            "exit_state": "one_more_spike",
            "candidates": [
                {"text": "test body", "rank_index": 1, "objective": "tilt", "tactic": "calm_reduction", "rank_score": 0.5}
            ],
        }
        panel = build_preview_panel(
            run,
            thread_id="t3_deadbeef",
            reply_to_id="t1_abc",
            author_handle="grave",
            context={
                "platform": "reddit",
                "thread_id": "t3_deadbeef",
                "messages": [{"message_id": "m1", "body": "lol sure buddy"}],
            },
        )
        self.assertIn("recommended_preset", panel)
        self.assertEqual(panel["emit_request"]["transport"], "reddit.comment.reply")
        self.assertIn("controls", panel)
        self.assertIn("variants", panel)
        self.assertIn("primary_variant", panel)
        self.assertIn("outcome_overlay", panel)
        self.assertEqual(panel["outcome_overlay"]["matching_runs"], 0)
        self.assertEqual(panel["outcome_overlay"]["confidence"], "none")
        self.assertEqual(panel["envelope"]["body"], "test body")
        self.assertEqual(panel["envelope"]["metadata"]["selected_candidate_text"], "test body")
        self.assertFalse(panel["envelope"]["metadata"]["combined_top_candidates"])
        self.assertFalse(panel["body_alignment"]["emitted_body_differs_from_selected_candidate"])
        self.assertFalse(panel["body_alignment"]["selection_filter_fallback"])
        self.assertGreaterEqual(len(panel["variants"]), 1)
        self.assertEqual(panel["variant_generation"]["dedupe_basis"], "body, candidate_tactic, candidate_objective, platform")
        self.assertEqual(panel["variants"][0]["history_rank"], 1)
        self.assertTrue(panel["variants"][0]["is_primary"])
        self.assertIn("highest_bite", panel["controls"]["selection_strategies"])

        emit_request = build_emit_request(panel["envelope"])
        self.assertEqual(emit_request["request"]["thing_id"], "t1_abc")
        self.assertEqual((emit_request.get("metadata") or {}).get("preferred_dispatch_driver"), "reddit_api")

    def test_build_emit_request_sanitizes_codeblock_style_reply_text(self) -> None:
        envelope = {
            "run_id": 1,
            "candidate_rank_index": 1,
            "platform": "reddit",
            "action": "reply",
            "body": "```\n    Which split do you mean, exactly?\n```",
            "target": {"reply_to_id": "t1_abc", "thread_id": "t3_thread"},
            "metadata": {},
        }

        emit_request = build_emit_request(envelope)
        self.assertEqual(emit_request["request"]["thing_id"], "t1_abc")
        self.assertEqual(emit_request["request"]["text"], "Which split do you mean, exactly?")

    def test_build_preview_panel_ranks_variants_by_history(self) -> None:
        run = {
            "id": 9,
            "platform": "reddit",
            "persona": "dry_midwit_savant",
            "plan": {
                "selected_objective": "tilt",
                "selected_tactic": "calm_reduction",
                "exit_state": "one_more_spike",
            },
            "selected_objective": "tilt",
            "selected_tactic": "calm_reduction",
            "exit_state": "one_more_spike",
            "candidates": [
                {
                    "text": "calm spike",
                    "rank_index": 1,
                    "objective": "tilt",
                    "tactic": "calm_reduction",
                    "rank_score": 0.91,
                    "estimated_bite_score": 0.4,
                    "estimated_audience_score": 0.2,
                    "critic_penalty": 0.3,
                },
                {
                    "text": "audience drag",
                    "rank_index": 2,
                    "objective": "audience_win",
                    "tactic": "reverse_interrogation",
                    "rank_score": 0.61,
                    "estimated_bite_score": 0.5,
                    "estimated_audience_score": 0.98,
                    "critic_penalty": 0.2,
                },
            ],
        }
        history_runs = [
            {
                "id": 101,
                "created_at": "2026-03-14T10:00:00Z",
                "platform": "reddit",
                "persona": "dry_midwit_savant",
                "plan": {"selected_objective": "audience_win", "selected_tactic": "reverse_interrogation", "exit_state": "hold"},
                "candidates": [{"text": "old win", "rank_score": 0.7}],
                "outcome": {"got_reply": True, "reply_delay_seconds": 20, "spectator_engagement": 6, "result_label": "bite"},
            },
            {
                "id": 102,
                "created_at": "2026-03-14T11:00:00Z",
                "platform": "reddit",
                "persona": "dry_midwit_savant",
                "plan": {"selected_objective": "audience_win", "selected_tactic": "reverse_interrogation", "exit_state": "hold"},
                "candidates": [{"text": "old win 2", "rank_score": 0.68}],
                "outcome": {"got_reply": True, "reply_delay_seconds": 40, "spectator_engagement": 5, "result_label": "bite"},
            },
            {
                "id": 103,
                "created_at": "2026-03-14T12:00:00Z",
                "platform": "reddit",
                "persona": "dry_midwit_savant",
                "plan": {"selected_objective": "tilt", "selected_tactic": "calm_reduction", "exit_state": "hold"},
                "candidates": [{"text": "old loss", "rank_score": 0.9}],
                "outcome": {"got_reply": False, "reply_delay_seconds": None, "spectator_engagement": 0, "result_label": "miss"},
            },
        ]

        panel = build_preview_panel(
            run,
            thread_id="t3_deadbeef",
            reply_to_id="t1_abc",
            author_handle="grave",
            history_runs=history_runs,
            panel_reviews=[
                {
                    "run_id": 200,
                    "created_at": "2026-03-14T13:00:00Z",
                    "platform": "reddit",
                    "persona": "dry_midwit_savant",
                    "candidate_objective": "audience_win",
                    "candidate_tactic": "reverse_interrogation",
                    "selection_preset": "audience",
                    "selection_strategy": "rank",
                    "disposition": "promote",
                }
            ],
            review_bridge={
                "program": "python3",
                "argv_prefix": ["-m", "bait_engine.cli.main"],
                "cwd": "/tmp",
                "db_path": "/tmp/bait.db",
            },
        )

        self.assertEqual(panel["envelope"]["metadata"]["candidate_objective"], "audience_win")
        self.assertEqual(panel["variants"][0]["envelope"]["metadata"]["candidate_objective"], "audience_win")
        self.assertEqual(panel["outcome_overlay"]["confidence"], "low")
        self.assertIn("engagement rate", panel["primary_variant"]["history_rationale"])
        self.assertGreaterEqual(len(panel["primary_variant"]["recent_match_lines"]), 1)
        self.assertIn("comparison_to_runner_up", panel["primary_variant"])
        self.assertIn("summary", panel["primary_variant"]["comparison_to_runner_up"])
        self.assertIn("metrics", panel["primary_variant"]["comparison_to_runner_up"])
        self.assertIn("dominant_advantage", panel["primary_variant"]["comparison_to_runner_up"])
        self.assertIn("review_rationale", panel["primary_variant"])
        self.assertIn("review_action_templates", panel["primary_variant"])
        self.assertIn("promote", panel["variants"][0]["review_action_templates"])
        self.assertIn("shell_command", panel["variants"][0]["review_action_templates"]["promote"])
        self.assertGreater(panel["outcome_overlay"]["historical_score"], panel["variants"][-1]["outcome_overlay"]["historical_score"])
        self.assertGreater(panel["review_overlay"]["operator_score"], 0.0)
        self.assertEqual(panel["variant_generation"]["ranking_basis"], "historical_score, operator_score, engagement_rate, matching_runs, candidate_rank_score")
        self.assertEqual(panel["variant_generation"]["dedupe_basis"], "body, candidate_tactic, candidate_objective, platform")
        self.assertEqual(panel["variant_generation"]["comparison_basis"], "primary vs runner-up deltas on historical score, operator score, engagement rate, matching runs, candidate rank score")
        self.assertEqual(panel["body_alignment"]["selected_candidate_text"], panel["envelope"]["metadata"]["selected_candidate_text"])

    def test_build_preview_panel_dedupes_equivalent_envelopes(self) -> None:
        run = {
            "id": 10,
            "platform": "reddit",
            "persona": "dry_midwit_savant",
            "plan": {
                "selected_objective": "tilt",
                "selected_tactic": "calm_reduction",
                "exit_state": "one_more_spike",
            },
            "candidates": [
                {
                    "text": "same line",
                    "rank_index": 1,
                    "objective": "tilt",
                    "tactic": "calm_reduction",
                    "rank_score": 0.8,
                    "estimated_bite_score": 0.7,
                    "estimated_audience_score": 0.2,
                    "critic_penalty": 0.1,
                }
            ],
        }

        panel = build_preview_panel(
            run,
            thread_id="t3_deadbeef",
            reply_to_id="t1_abc",
            author_handle="grave",
            include_all_presets=True,
            include_strategy_variants=True,
        )

        dedupe_keys = {
            (
                variant["envelope"]["body"],
                variant["envelope"]["metadata"].get("candidate_tactic"),
                variant["envelope"]["metadata"].get("candidate_objective"),
                variant["envelope"]["platform"],
            )
            for variant in panel["variants"]
        }
        self.assertEqual(len(dedupe_keys), len(panel["variants"]))

    def test_build_preview_panel_strategy_variants_keep_composition_modes_explicit(self) -> None:
        run = {
            "id": 12,
            "platform": "reddit",
            "persona": "dry_midwit_savant",
            "plan": {
                "selected_objective": "tilt",
                "selected_tactic": "calm_reduction",
                "exit_state": "one_more_spike",
            },
            "selected_objective": "tilt",
            "selected_tactic": "calm_reduction",
            "exit_state": "one_more_spike",
            "candidates": [
                {
                    "text": "That framing is lazy and ignores basic funding mechanics.",
                    "rank_index": 1,
                    "objective": "tilt",
                    "tactic": "calm_reduction",
                    "rank_score": 0.91,
                    "estimated_bite_score": 0.52,
                    "estimated_audience_score": 0.44,
                    "critic_penalty": 0.08,
                },
                {
                    "text": "Private donations do not replace public priorities.",
                    "rank_index": 2,
                    "objective": "tilt",
                    "tactic": "calm_reduction",
                    "rank_score": 0.88,
                    "estimated_bite_score": 0.49,
                    "estimated_audience_score": 0.41,
                    "critic_penalty": 0.09,
                },
                {
                    "text": "Waving 'pay for it yourself' as a fix is rhetorical cosplay.",
                    "rank_index": 3,
                    "objective": "tilt",
                    "tactic": "calm_reduction",
                    "rank_score": 0.86,
                    "estimated_bite_score": 0.53,
                    "estimated_audience_score": 0.39,
                    "critic_penalty": 0.11,
                },
            ],
        }

        panel = build_preview_panel(
            run,
            thread_id="t3_deadbeef",
            reply_to_id="t1_abc",
            author_handle="grave",
            include_all_presets=False,
            include_strategy_variants=True,
            include_filter_variants=False,
        )

        mega_bait_variant = next(variant for variant in panel["variants"] if variant["name"] == "strategy:mega_bait")
        self.assertEqual(mega_bait_variant["envelope"]["metadata"]["composition_strategy"], "mega_bait")
        self.assertEqual(mega_bait_variant["envelope"]["metadata"]["combined_candidate_rank_indexes"], [1, 2, 3])
        self.assertTrue(mega_bait_variant["body_alignment"]["combined_top_candidates"])
        self.assertTrue(mega_bait_variant["body_alignment"]["emitted_body_differs_from_selected_candidate"])
        blend_top3_variant = next((variant for variant in panel["variants"] if variant["name"] == "strategy:blend_top3"), None)
        if blend_top3_variant is not None:
            self.assertTrue(blend_top3_variant["body_alignment"]["combined_top_candidates"])

    def test_build_preview_panel_auto_best_variant_exposes_rationale(self) -> None:
        run = {
            "id": 11,
            "platform": "reddit",
            "persona": "dry_midwit_savant",
            "plan": {
                "selected_objective": "tilt",
                "selected_tactic": "calm_reduction",
                "exit_state": "one_more_spike",
            },
            "candidates": [
                {
                    "text": "high-rate-low-sample",
                    "rank_index": 1,
                    "objective": "tilt",
                    "tactic": "calm_reduction",
                    "rank_score": 0.82,
                    "estimated_bite_score": 0.91,
                    "estimated_audience_score": 0.25,
                    "critic_penalty": 0.08,
                },
                {
                    "text": "verified-consistent",
                    "rank_index": 2,
                    "objective": "tilt",
                    "tactic": "reverse_interrogation",
                    "rank_score": 0.74,
                    "estimated_bite_score": 0.66,
                    "estimated_audience_score": 0.58,
                    "critic_penalty": 0.14,
                },
            ],
        }
        reputation = {
            "reply_rate": 0.44,
            "total_runs": 5,
            "delivery_confidence": 0.7,
            "tactic_performance": {
                "calm_reduction": {"count": 2, "replies": 2, "rate": 1.0, "delivery_confidence": 0.0},
                "reverse_interrogation": {"count": 14, "replies": 6, "rate": 0.43, "delivery_confidence": 0.86},
            },
        }

        panel = build_preview_panel(
            run,
            thread_id="t3_deadbeef",
            reply_to_id="t1_abc",
            author_handle="grave",
            include_all_presets=False,
            include_strategy_variants=True,
            include_filter_variants=False,
            reputation_data=reputation,
        )

        auto_best_variant = next(variant for variant in panel["variants"] if variant["name"] == "strategy:auto_best")
        self.assertEqual(auto_best_variant["selection"]["strategy"], "auto_best")
        self.assertEqual(auto_best_variant["selection"]["tactic"], "reverse_interrogation")
        self.assertIsNotNone(auto_best_variant["selection"]["auto_best_rationale"])
        self.assertEqual(
            auto_best_variant["selection"]["auto_best_rationale"]["tactic"],
            auto_best_variant["selection"]["tactic"],
        )

    def test_build_reply_envelope_rejects_unknown_platform(self) -> None:
        run = {
            "id": 1,
            "platform": "myspace_lich",
            "persona": "dry_midwit_savant",
            "selected_objective": "tilt",
            "selected_tactic": "calm_reduction",
            "exit_state": "one_more_spike",
            "candidates": [{"text": "test", "rank_index": 1, "objective": "tilt", "rank_score": 0.5}],
        }
        with self.assertRaises(KeyError):
            build_reply_envelope(run, candidate_rank_index=1)

    def test_build_reply_envelope_raises_for_missing_candidate(self) -> None:
        run = {
            "id": 1,
            "platform": "reddit",
            "persona": "dry_midwit_savant",
            "selected_objective": "tilt",
            "selected_tactic": "calm_reduction",
            "exit_state": "one_more_spike",
            "candidates": [],
        }
        with self.assertRaises(KeyError):
            build_reply_envelope(run, candidate_rank_index=1)


if __name__ == "__main__":
    unittest.main()
