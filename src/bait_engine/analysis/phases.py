from __future__ import annotations

from bait_engine.core.types import ThreadPhase


def estimate_phase(
    turn_count: int | None = None,
    avg_reply_length: int | None = None,
    emotional_intensity: float | None = None,
    outside_participants: int | None = None,
    frame_adoption: float | None = None,
) -> ThreadPhase:
    turns = turn_count or 1
    avg_len = avg_reply_length or 0
    emotion = emotional_intensity or 0.0
    spectators = outside_participants or 0
    frame = frame_adoption or 0.0

    if turns <= 1:
        return ThreadPhase.HOOK
    if turns <= 3 and avg_len < 80 and emotion < 0.45:
        return ThreadPhase.INFLATION
    if frame > 0.55 and (avg_len >= 80 or spectators > 0):
        return ThreadPhase.EXPOSURE
    if emotion >= 0.7:
        return ThreadPhase.COMBUSTION
    if turns >= 4 and emotion < 0.35 and frame > 0.45:
        return ThreadPhase.CLOSURE
    return ThreadPhase.INFLATION
