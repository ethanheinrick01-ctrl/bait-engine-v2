from __future__ import annotations

from bait_engine.generation.contracts import DraftRequest, MutationSeed


def _serialize_mutation_seed(seed: MutationSeed) -> dict:
    payload = {
        "text": seed.text,
        "transform": seed.transform,
        "tactic": seed.tactic,
        "objective": seed.objective,
        "winner_score": seed.winner_score,
        "delta_ratio": seed.delta_ratio,
        "novelty_ratio": seed.novelty_ratio,
    }
    return {key: value for key, value in payload.items() if value is not None}


def build_prompt_payload(request: DraftRequest) -> dict:
    plan = request.plan
    writer_rules = [
        "Keep it short.",
        "Do not explain the whole argument.",
        "Prefer slight unfinishedness over polished completeness.",
        "Avoid sounding like an AI assistant or debate essayist.",
        "Disagree with the source claim; do not open by agreeing.",
        "Ban agreement openers like: yeah, exactly, true, fair, facts, valid point, right.",
    ]
    mutation_seeds = [_serialize_mutation_seed(seed) for seed in request.mutation_seeds if seed.text.strip()]
    if mutation_seeds:
        writer_rules.extend(
            [
                "Use mutation seeds as pressure/cadence anchors when helpful.",
                "Do not copy mutation seeds verbatim; mutate or recombine them.",
            ]
        )
    if request.avoid_patterns:
        writer_rules.append(f"Avoid these stale patterns: {', '.join(request.avoid_patterns)}")
    return {
        "task": "Write short human-sounding comment replies inside the given tactical constraints.",
        "source_text": request.source_text,
        "persona": {
            "name": request.persona.name,
            "length_band_words": request.persona.length_band_words,
            "tone_tags": request.persona.tone_tags,
            "jargon_ceiling": request.persona.jargon_ceiling,
            "absurdity_tolerance": request.persona.absurdity_tolerance,
            "calmness_preference": request.persona.calmness_preference,
            "pressure_profile": request.persona.pressure_profile,
            "escalation_cues": request.persona.escalation_cues,
        },
        "plan": {
            "objective": plan.selected_objective.value,
            "tactic": plan.selected_tactic.value if plan.selected_tactic else None,
            "alternates": [item.value for item in plan.alternates],
            "length_band_words": list(plan.length_band_words),
            "tone_constraints": plan.tone_constraints,
            "exit_state": plan.exit_state,
        },
        "mutation_seeds": mutation_seeds,
        "mutation_context": request.mutation_context,
        "winner_anchors": request.winner_anchors,
        "avoid_patterns": request.avoid_patterns,
        "writer_rules": writer_rules,
    }
