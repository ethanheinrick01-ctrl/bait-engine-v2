from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from bait_engine.adapters.inbound import InboundThreadContext
from bait_engine.adapters.presets import resolve_selection_preset


_HOSTILE_MARKERS = (
    "idiot",
    "moron",
    "stupid",
    "dumb",
    "cope",
    "lol",
    "lmao",
    "wtf",
    "schizo",
    "delusional",
)

_ENGAGEMENT_METADATA_KEYS = (
    "like_count",
    "likes",
    "favorite_count",
    "reaction_count",
    "reactions",
    "reply_count",
    "replies",
    "quote_count",
    "retweet_count",
    "repost_count",
    "upvote_count",
    "score",
    "view_count",
    "impression_count",
)

_PARTICIPANT_METADATA_KEYS = (
    "participant_count",
    "participants",
    "unique_author_count",
    "spectator_count",
)


def _as_int(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            return int(stripped)
    return 0


def _sum_metadata_keys(mapping: dict[str, Any], keys: Iterable[str]) -> int:
    return sum(_as_int(mapping.get(key)) for key in keys)


def _collect_recommendation_metrics(context: InboundThreadContext) -> dict[str, int | bool | None]:
    messages = context.messages
    message_count = len(messages)
    recent_messages = messages[-5:]
    recent_bodies = [message.body.lower() for message in recent_messages]
    joined = " ".join(recent_bodies)
    hostility_hits = sum(1 for marker in _HOSTILE_MARKERS if marker in joined)

    authors = {message.author_handle.lower() for message in messages if message.author_handle}
    unique_authors = len(authors)
    root_author_present = bool(
        context.root_author_handle
        and context.root_author_handle.lower() in authors
    )

    thread_engagement = _sum_metadata_keys(context.metadata, _ENGAGEMENT_METADATA_KEYS)
    message_engagement = sum(_sum_metadata_keys(message.metadata, _ENGAGEMENT_METADATA_KEYS) for message in messages)
    audience_signal = thread_engagement + message_engagement
    participant_signal = max(unique_authors, _sum_metadata_keys(context.metadata, _PARTICIPANT_METADATA_KEYS))

    return {
        "message_count": message_count,
        "hostility_hits": hostility_hits,
        "unique_authors": unique_authors,
        "participant_signal": participant_signal,
        "audience_signal": audience_signal,
        "root_author_present": root_author_present,
        "has_subject": bool(context.subject),
    }


def recommend_selection_preset(
    platform: str, 
    context: InboundThreadContext | None = None,
    reputation_data: dict[str, Any] | None = None,
) -> dict[str, str | int | bool | None | dict[str, int | bool | None]]:
    if context is None:
        preset = resolve_selection_preset(platform)
        metrics = {
            "message_count": 0,
            "hostility_hits": 0,
            "unique_authors": 0,
            "participant_signal": 0,
            "audience_signal": 0,
            "root_author_present": False,
            "has_subject": False,
        }

        preset_name = preset["name"]
        strategy = str(preset["strategy"])
        reason = "adapter default preset"

        runs = int((reputation_data or {}).get("total_runs") or 0)
        reply_rate = float((reputation_data or {}).get("reply_rate") or 0.0)
        if runs >= 8 and reply_rate >= 0.2:
            strategy = "auto_best"
            reason = "strong persona reputation with sufficient sample; enabling auto_best"

        return {
            "name": str(preset_name),
            "strategy": strategy,
            "tactic": preset["tactic"],
            "objective": preset["objective"],
            "dispatch_driver": preset.get("dispatch_driver"),
            "reason": reason,
            "message_count": 0,
            "hostility_hits": 0,
            "metrics": metrics,
        }

    metrics = _collect_recommendation_metrics(context)
    message_count = int(metrics["message_count"])
    hostility_hits = int(metrics["hostility_hits"])
    unique_authors = int(metrics["unique_authors"])
    participant_signal = int(metrics["participant_signal"])
    audience_signal = int(metrics["audience_signal"])

    if platform == "x":
        if hostility_hits >= 3 and audience_signal < 3:
            preset_name = "safe"
            reason = "x thread looks heated without much audience payoff"
        elif audience_signal >= 5 or participant_signal >= 3 or message_count >= 4:
            preset_name = "audience"
            reason = "x thread has enough spectators or velocity to optimize for onlookers"
        else:
            preset_name = "engage"
            reason = "x thread is still compact enough to lean into direct engagement"
    elif platform == "reddit":
        if hostility_hits >= 2:
            preset_name = "safe"
            reason = "reddit thread looks hostile; lower-penalty reply preferred"
        elif audience_signal >= 6 or participant_signal >= 4:
            preset_name = "audience"
            reason = "reddit thread has enough visible audience signal to favor spectator wins"
        else:
            preset_name = "engage"
            reason = "reddit thread still rewards bite-driven engagement"
    elif platform == "discord":
        if hostility_hits >= 2:
            preset_name = "safe"
            reason = "discord room is heated; prefer low-penalty replies"
        elif participant_signal >= 4 or audience_signal >= 4:
            preset_name = "audience"
            reason = "discord room is crowded enough to optimize for the room, not just the target"
        elif message_count >= 4:
            preset_name = "safe"
            reason = "discord room is active enough that durable replies beat spike-chasing"
        else:
            preset_name = "engage"
            reason = "discord thread is still sparse enough for an engagement push"
    else:
        preset_name = "safe" if hostility_hits >= 1 else None
        reason = "generic web thread looks heated" if hostility_hits >= 1 else "fall back to adapter default preset"

    preset = resolve_selection_preset(platform, preset_name)
    strategy = str(preset["strategy"])
    runs = int((reputation_data or {}).get("total_runs") or 0)
    reply_rate = float((reputation_data or {}).get("reply_rate") or 0.0)
    if runs >= 6 and reply_rate > 0.15:
        strategy = "auto_best"
        reason = f"{reason} + reputation auto_best"

    return {
        "name": str(preset["name"]),
        "strategy": strategy,
        "tactic": preset["tactic"],
        "objective": preset["objective"],
        "dispatch_driver": preset.get("dispatch_driver"),
        "reason": reason,
        "message_count": message_count,
        "hostility_hits": hostility_hits,
        "metrics": metrics,
    }
