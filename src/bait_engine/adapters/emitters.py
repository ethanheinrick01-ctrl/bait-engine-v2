from __future__ import annotations

from typing import Any


def _sanitize_reply_text(text: str) -> str:
    normalized = str(text or "").replace("\r\n", "\n").strip()
    if normalized.startswith("```") and normalized.endswith("```"):
        lines = normalized.splitlines()
        if len(lines) >= 2:
            normalized = "\n".join(lines[1:-1]).strip()
    normalized = "\n".join(line.lstrip(" \t") for line in normalized.splitlines())
    return normalized.strip()


def build_emit_request(envelope: dict[str, Any]) -> dict[str, Any]:
    platform = envelope["platform"]
    target = envelope["target"]
    body = _sanitize_reply_text(str(envelope.get("body") or ""))

    common = {
        "platform": platform,
        "action": envelope["action"],
        "body": body,
        "metadata": {
            "run_id": envelope["run_id"],
            "candidate_rank_index": envelope["candidate_rank_index"],
            "selection_strategy": envelope.get("metadata", {}).get("selection_strategy"),
            "selection_preset": envelope.get("metadata", {}).get("selection_preset"),
            "preferred_dispatch_driver": envelope.get("metadata", {}).get("preferred_dispatch_driver"),
        },
    }

    if platform == "reddit":
        return {
            **common,
            "transport": "reddit.comment.reply",
            "request": {
                "thing_id": target.get("reply_to_id") or target.get("thread_id"),
                "text": body,
            },
        }
    if platform == "x":
        return {
            **common,
            "transport": "x.post.reply",
            "request": {
                "in_reply_to_tweet_id": target.get("reply_to_id") or target.get("thread_id"),
                "text": body,
            },
        }
    if platform == "discord":
        return {
            **common,
            "transport": "discord.message.reply",
            "request": {
                "channel_or_thread_id": target.get("thread_id"),
                "message_reference_id": target.get("reply_to_id"),
                "content": body,
                "allowed_mentions": {"parse": []},
            },
        }
    if platform == "web":
        return {
            **common,
            "transport": "web.reply",
            "request": {
                "thread_id": target.get("thread_id"),
                "reply_to_id": target.get("reply_to_id"),
                "body": body,
            },
        }

    raise KeyError(f"emitter for adapter '{platform}' not found")
