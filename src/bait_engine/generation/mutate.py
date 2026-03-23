from __future__ import annotations

import difflib
import re
from typing import Any


_WHITESPACE_RE = re.compile(r"\s+")
_WORD_RE = re.compile(r"[a-z0-9']+")

AGREEMENT_OPENERS = (
    "yeah",
    "yep",
    "exactly",
    "true",
    "fair",
    "facts",
    "right",
    "valid point",
    "agreed",
    "correct",
)

OBJECTIVE_DELTA_BOUNDS: dict[str, tuple[float, float]] = {
    "hook": (0.08, 0.42),
    "resurrect": (0.08, 0.42),
    "stall": (0.08, 0.42),
    "branch_split": (0.08, 0.45),
    "collapse": (0.12, 0.5),
    "audience_win": (0.12, 0.5),
    "exit_on_top": (0.12, 0.5),
    "inflate": (0.14, 0.58),
    "tilt": (0.14, 0.58),
    "misframe": (0.14, 0.58),
}

OBJECTIVE_MIN_QUESTION: set[str] = {"hook", "resurrect", "stall", "branch_split"}
OBJECTIVE_NO_QUESTION: set[str] = {"collapse", "audience_win", "exit_on_top"}


def _normalize(text: str) -> str:
    return _WHITESPACE_RE.sub(" ", text).strip()


def _tokens(text: str) -> list[str]:
    return _WORD_RE.findall(text.lower())


def _delta_ratio(base_text: str, variant_text: str) -> float:
    return 1.0 - difflib.SequenceMatcher(a=base_text.lower(), b=variant_text.lower()).ratio()


def _novelty_ratio(base_text: str, variant_text: str) -> float:
    base = set(_tokens(base_text))
    if not base:
        return 0.0
    variant = _tokens(variant_text)
    if not variant:
        return 0.0
    new_tokens = [token for token in variant if token not in base]
    return len(new_tokens) / max(len(variant), 1)


def _compress(text: str) -> str:
    parts = [chunk.strip() for chunk in re.split(r"[.!?;]+", text) if chunk.strip()]
    if not parts:
        return text.strip()
    shortest = min(parts, key=lambda item: len(item.split()))
    return shortest.rstrip(".?!")


def _sharpen(text: str) -> str:
    trimmed = text.strip().rstrip(".?!")
    if not trimmed:
        return text
    return f"{trimmed} — pick one claim and defend it."


def _soften_surface_preserve_sting(text: str) -> str:
    trimmed = text.strip().rstrip(".?!")
    if not trimmed:
        return text
    return f"Serious question: {trimmed.lower()}?"


def _vary_cadence(text: str) -> str:
    cleaned = text.strip().rstrip(".?!")
    if not cleaned:
        return text
    words = cleaned.split()
    if len(words) < 8:
        return f"{cleaned}. Why?"
    pivot = max(3, min(len(words) - 3, len(words) // 2))
    first = " ".join(words[:pivot]).rstrip(",")
    second = " ".join(words[pivot:]).lstrip(",")
    return f"{first}, then {second}."


def _invert_confidence_posture(text: str) -> str:
    cleaned = text.strip().rstrip(".?!")
    if not cleaned:
        return text
    return f"Maybe I'm missing it, but {cleaned.lower()}."


def _starts_with_agreement(text: str) -> bool:
    return text.lower().lstrip().startswith(AGREEMENT_OPENERS)


def _fits_objective_shape(text: str, objective: str | None) -> bool:
    objective_key = (objective or "").strip().lower()
    if not objective_key:
        return True
    if objective_key == "do_not_engage":
        return False
    has_question = "?" in text
    if objective_key in OBJECTIVE_MIN_QUESTION and not has_question:
        return False
    if objective_key in OBJECTIVE_NO_QUESTION and has_question:
        return False
    return True


def _delta_bounds_for_objective(objective: str | None) -> tuple[float, float]:
    key = (objective or "").strip().lower()
    return OBJECTIVE_DELTA_BOUNDS.get(key, (0.1, 0.55))


def _within_mutation_guardrails(base_text: str, candidate_text: str, objective: str | None) -> tuple[bool, dict[str, float]]:
    delta_ratio = _delta_ratio(base_text, candidate_text)
    novelty_ratio = _novelty_ratio(base_text, candidate_text)
    min_delta, max_delta = _delta_bounds_for_objective(objective)
    ok = (
        min_delta <= delta_ratio <= max_delta
        and 0.03 <= novelty_ratio <= 0.5
        and not _starts_with_agreement(candidate_text)
    )
    return ok, {
        "delta_ratio": round(delta_ratio, 4),
        "novelty_ratio": round(novelty_ratio, 4),
        "min_delta": round(min_delta, 4),
        "max_delta": round(max_delta, 4),
    }


def generate_controlled_variants(
    winner: dict[str, Any],
    *,
    max_variants: int = 5,
    transform_policy: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Generate deterministic winner mutations with bounded deltas and objective constraints."""
    base_text = _normalize(str(winner.get("candidate_text") or ""))
    if not base_text:
        return []

    objective = str(winner.get("candidate_objective") or "").strip().lower() or None
    if objective == "do_not_engage":
        return []

    transforms: list[tuple[str, callable]] = [
        ("compress", _compress),
        ("sharpen", _sharpen),
        ("soften_surface_preserve_sting", _soften_surface_preserve_sting),
        ("vary_cadence", _vary_cadence),
        ("invert_confidence_posture", _invert_confidence_posture),
    ]
    if transform_policy:
        policy_rank = {str(name).strip().lower(): idx for idx, name in enumerate(transform_policy) if str(name).strip()}
        transforms = sorted(
            transforms,
            key=lambda item: (
                policy_rank.get(item[0].lower(), len(policy_rank) + 999),
                item[0],
            ),
        )

    variants: list[dict[str, Any]] = []
    seen = {_normalize(base_text).lower()}
    for transform_name, transform in transforms:
        candidate_text = _normalize(transform(base_text))
        if not candidate_text:
            continue
        key = candidate_text.lower()
        if key in seen:
            continue
        seen.add(key)
        if not _fits_objective_shape(candidate_text, objective):
            continue
        guard_ok, metrics = _within_mutation_guardrails(base_text, candidate_text, objective)
        if not guard_ok:
            continue

        variants.append(
            {
                "transform": transform_name,
                "text": candidate_text,
                "lineage": {
                    "run_id": winner.get("run_id"),
                    "candidate_id": winner.get("candidate_id"),
                    "candidate_rank_index": winner.get("candidate_rank_index"),
                    "winner_score": winner.get("winner_score"),
                    "winner_source": winner.get("winner_source"),
                    "mutation_metrics": metrics,
                },
            }
        )
        if len(variants) >= max_variants:
            break

    return variants
