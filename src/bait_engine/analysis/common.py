from __future__ import annotations


def clamp_unit(value: float) -> float:
    return max(0.0, min(1.0, float(value)))
