from __future__ import annotations

from bait_engine.adapters.registry import DEFAULT_ADAPTERS
from bait_engine.adapters.select import SelectionStrategy


def resolve_selection_preset(
    platform: str,
    preset_name: str | None = None,
) -> dict[str, str | SelectionStrategy | None]:
    try:
        descriptor = DEFAULT_ADAPTERS[platform]
    except KeyError as exc:
        raise KeyError(f"adapter '{platform}' not found") from exc

    wanted = preset_name or descriptor.default_selection_preset
    preset = next((item for item in descriptor.selection_presets if item.name == wanted), None)
    if preset is None:
        raise KeyError(f"selection preset '{wanted}' not found for adapter '{platform}'")

    return {
        "name": preset.name,
        "strategy": preset.strategy,
        "tactic": preset.tactic,
        "objective": preset.objective,
        "dispatch_driver": preset.dispatch_driver,
    }
