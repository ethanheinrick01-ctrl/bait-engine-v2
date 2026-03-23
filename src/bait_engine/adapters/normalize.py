from __future__ import annotations

from bait_engine.adapters.contracts import AdapterTarget
from bait_engine.adapters.registry import DEFAULT_ADAPTERS


def _clean(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def _normalize_handle(platform: str, author_handle: str | None) -> str | None:
    handle = _clean(author_handle)
    if handle is None:
        return None
    if platform == "reddit":
        return handle.removeprefix("u/").removeprefix("/u/")
    if platform == "x":
        return handle.removeprefix("@")
    if platform == "discord":
        return handle.lower()
    return handle


def normalize_target(
    platform: str,
    thread_id: str | None = None,
    reply_to_id: str | None = None,
    author_handle: str | None = None,
) -> AdapterTarget:
    if platform not in DEFAULT_ADAPTERS:
        raise KeyError(f"adapter '{platform}' not found")

    normalized_thread_id = _clean(thread_id)
    normalized_reply_to_id = _clean(reply_to_id)
    normalized_author_handle = _normalize_handle(platform, author_handle)

    if platform in {"reddit", "x", "discord"} and normalized_reply_to_id is not None and normalized_thread_id is None:
        normalized_thread_id = normalized_reply_to_id

    return AdapterTarget(
        platform=platform,
        thread_id=normalized_thread_id,
        reply_to_id=normalized_reply_to_id,
        author_handle=normalized_author_handle,
    )
