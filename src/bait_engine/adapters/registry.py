from __future__ import annotations

from bait_engine.adapters.contracts import AdapterCapabilitySet, AdapterDescriptor, AdapterSelectionPreset


DEFAULT_ADAPTERS: dict[str, AdapterDescriptor] = {
    "reddit": AdapterDescriptor(
        name="reddit",
        platform="reddit",
        capabilities=AdapterCapabilitySet(
            can_reply=True,
            can_create_thread=True,
            supports_editing=True,
            supports_deletion=True,
            supports_media=False,
            supports_thread_lookup=True,
        ),
        default_selection_preset="engage",
        selection_presets=[
            AdapterSelectionPreset(name="default", strategy="top_score", dispatch_driver="reddit_api", notes=["General balanced selection."]),
            AdapterSelectionPreset(name="engage", strategy="highest_bite", objective="tilt", dispatch_driver="reddit_api", notes=["Prefer replies likely to provoke engagement."]),
            AdapterSelectionPreset(name="audience", strategy="highest_audience", objective="audience_win", dispatch_driver="reddit_api", notes=["Prefer replies that play well to spectators."]),
            AdapterSelectionPreset(name="safe", strategy="lowest_penalty", dispatch_driver="reddit_api", notes=["Prefer low-penalty replies for durable threads."]),
        ],
        notes=[
            "Thread = submission or comment chain context.",
            "Reply targets typically map to comment IDs.",
        ],
    ),
    "x": AdapterDescriptor(
        name="x",
        platform="x",
        capabilities=AdapterCapabilitySet(
            can_reply=True,
            can_create_thread=True,
            supports_editing=False,
            supports_deletion=True,
            supports_media=True,
            supports_thread_lookup=True,
        ),
        default_selection_preset="audience",
        selection_presets=[
            AdapterSelectionPreset(name="default", strategy="top_score", dispatch_driver="x_api", notes=["General balanced selection."]),
            AdapterSelectionPreset(name="audience", strategy="highest_audience", objective="audience_win", dispatch_driver="x_api", notes=["Prefer replies optimized for public spectators."]),
            AdapterSelectionPreset(name="engage", strategy="highest_bite", objective="tilt", dispatch_driver="x_api", notes=["Prefer replies that trigger continued posting."]),
            AdapterSelectionPreset(name="safe", strategy="lowest_penalty", dispatch_driver="x_api", notes=["Prefer low-penalty replies where portability matters."]),
        ],
        notes=[
            "Thread creation may involve chained posts.",
            "Edit support is intentionally false for portability.",
        ],
    ),
    "discord": AdapterDescriptor(
        name="discord",
        platform="discord",
        capabilities=AdapterCapabilitySet(
            can_reply=True,
            can_create_thread=True,
            supports_editing=True,
            supports_deletion=True,
            supports_media=True,
            supports_thread_lookup=True,
        ),
        default_selection_preset="safe",
        selection_presets=[
            AdapterSelectionPreset(name="default", strategy="top_score", notes=["General balanced selection."]),
            AdapterSelectionPreset(name="safe", strategy="lowest_penalty", notes=["Prefer low-penalty replies in conversational rooms."]),
            AdapterSelectionPreset(name="engage", strategy="highest_bite", objective="tilt", notes=["Prefer punchier replies for active threads."]),
            AdapterSelectionPreset(name="audience", strategy="highest_audience", objective="audience_win", notes=["Prefer spectator-friendly replies when rooms are crowded."]),
        ],
        notes=[
            "Thread targets may be channel threads or reply references.",
        ],
    ),
    "web": AdapterDescriptor(
        name="web",
        platform="web",
        capabilities=AdapterCapabilitySet(
            can_reply=True,
            can_create_thread=False,
            supports_editing=False,
            supports_deletion=False,
            supports_media=False,
            supports_thread_lookup=False,
        ),
        default_selection_preset="default",
        selection_presets=[
            AdapterSelectionPreset(name="default", strategy="top_score", notes=["General balanced selection for generic surfaces."]),
            AdapterSelectionPreset(name="safe", strategy="lowest_penalty", notes=["Prefer durable low-penalty replies."]),
        ],
        notes=[
            "Minimal generic web surface adapter.",
        ],
    ),
}


def list_adapters() -> list[dict]:
    return [descriptor.model_dump(mode="json") for descriptor in DEFAULT_ADAPTERS.values()]


def get_adapter(platform: str) -> dict:
    try:
        return DEFAULT_ADAPTERS[platform].model_dump(mode="json")
    except KeyError as exc:
        raise KeyError(f"adapter '{platform}' not found") from exc
