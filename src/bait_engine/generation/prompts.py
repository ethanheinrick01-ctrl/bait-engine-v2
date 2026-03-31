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


def _register_rules(target_register: float) -> list[str]:
    """Return diction/register rules based on the target's detected lexical level."""
    if target_register < 0.35:
        return [
            "Target uses simple vocabulary and short sentences — match that register.",
            "Prefer short common words over technical or Latinate alternatives.",
            "Loose or imperfect grammar is fine if it sounds more natural than correct grammar.",
            "Do not introduce jargon or academic phrasing the target didn't use.",
            "You can join two thoughts with casual connectors like 'cuz', 'tho', 'but like', 'cause' instead of a period.",
            "Trailing tags like 'lol', 'ngl', 'fr', 'tbh' are acceptable and sound more human at this register.",
            "Avoid aphoristic or overly polished phrasing — type it, don't write it.",
        ]
    if target_register < 0.65:
        return [
            "Target uses mid-register language — conversational but literate.",
            "Match their vocabulary level without dumbing down or academizing.",
            "Imperfect grammar is acceptable where it sounds more natural.",
            "Occasional casual connectors like 'though', 'but', 'honestly' between clauses read as human.",
            "A trailing 'lol' or 'ngl' is fine if it fits the tone.",
        ]
    return [
        "Target uses elevated vocabulary and complex syntax — match or slightly exceed their register.",
        "Precise terminology is appropriate here — don't simplify what doesn't need simplifying.",
        "Grammatical precision is expected at this register, but stilted formality is still wrong.",
        "Do not use casual connectors like 'cuz' or trailing tags like 'lol' — they would undercut the register.",
    ]


def build_prompt_payload(request: DraftRequest) -> dict:
    plan = request.plan
    writer_rules = [
        "Keep it short.",
        "Do not explain the whole argument.",
        "Prefer slight unfinishedness over polished completeness.",
        "Avoid sounding like an AI assistant or debate essayist.",
        "Disagree with the source claim; do not open by agreeing.",
        "Ban agreement openers like: yeah, exactly, true, fair, facts, valid point, right.",
        "Never use colons or semicolons.",
        "Imperfect grammar is acceptable when it sounds more natural than correct grammar.",
    ]
    writer_rules.extend(_register_rules(request.target_register))
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
        "target_register": round(request.target_register, 2),
        "mutation_seeds": mutation_seeds,
        "mutation_context": request.mutation_context,
        "winner_anchors": request.winner_anchors,
        "avoid_patterns": request.avoid_patterns,
        "writer_rules": writer_rules,
    }
