from __future__ import annotations

import re

from bait_engine.generation.contracts import CandidateReply, DraftRequest, MutationSeed
from bait_engine.generation.prompts import build_prompt_payload
from bait_engine.providers.base import TextGenerationProvider

LINE_PREFIX_RE = re.compile(r"^\s*(?:[-*]|\d+[.)])\s*")


def _render_mutation_seed(seed: MutationSeed | dict) -> str:
    if isinstance(seed, MutationSeed):
        text = seed.text
        transform = seed.transform
        tactic = seed.tactic
        objective = seed.objective
        winner_score = seed.winner_score
    else:
        text = str(seed.get("text") or "")
        transform = seed.get("transform")
        tactic = seed.get("tactic")
        objective = seed.get("objective")
        winner_score = seed.get("winner_score")
    meta: list[str] = []
    if transform:
        meta.append(f"transform={transform}")
    if tactic:
        meta.append(f"tactic={tactic}")
    if objective:
        meta.append(f"objective={objective}")
    if winner_score is not None:
        meta.append(f"winner_score={winner_score}")
    meta_prefix = f"[{', '.join(meta)}] " if meta else ""
    return f"- {meta_prefix}{text}".strip()


def build_provider_prompts(request: DraftRequest) -> tuple[str, str]:
    payload = build_prompt_payload(request)
    system_prompt = (
        "You write short, human-sounding social media replies. "
        "No assistant tone. No explanations. No policy chatter. "
        "Every candidate must challenge or contradict the source claim. "
        "Never open with agreement language (yeah/exactly/true/facts/right/valid point). "
        "Output only candidate lines, one per line."
    )
    mutation_block = ""
    mutation_seeds = payload.get("mutation_seeds") or []
    if mutation_seeds:
        rendered = "\n".join(_render_mutation_seed(seed) for seed in mutation_seeds)
        mutation_block = (
            "Mutation seeds (reuse cadence/pressure when useful; do not copy them verbatim):\n"
            f"{rendered}\n"
        )
    mutation_context = payload.get("mutation_context")
    if mutation_context:
        mutation_block += f"Mutation context: {mutation_context}\\n"
    winner_anchors = payload.get("winner_anchors") or []
    if winner_anchors:
        mutation_block += f"Winner anchors: {winner_anchors}\\n"
    avoid_patterns = payload.get("avoid_patterns") or []
    if avoid_patterns:
        mutation_block += f"Avoid patterns: {avoid_patterns}\\n"
    pressure_profile = payload.get("persona", {}).get("pressure_profile")
    escalation_cues = payload.get("persona", {}).get("escalation_cues")
    pressure_line = ""
    if pressure_profile:
        pressure_line = f"Pressure profile: {pressure_profile} (cues={escalation_cues})\\n"
    user_prompt = (
        f"Source text: {payload['source_text']}\n"
        f"Persona: {payload['persona']}\n"
        f"Plan: {payload['plan']}\n"
        f"{pressure_line}"
        f"{mutation_block}"
        f"Rules: {payload['writer_rules']}\n"
        f"Return exactly {request.candidate_count} distinct candidates."
    )
    return system_prompt, user_prompt


def parse_candidate_lines(raw: str, candidate_count: int) -> list[str]:
    seen: set[str] = set()
    cleaned: list[str] = []
    for line in raw.splitlines():
        line = LINE_PREFIX_RE.sub("", line).strip()
        if not line:
            continue
        key = re.sub(r"\s+", " ", line.lower())
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(line)
        if len(cleaned) >= candidate_count:
            break
    return cleaned


def generate_candidates_via_provider(request: DraftRequest, provider: TextGenerationProvider) -> list[CandidateReply]:
    system_prompt, user_prompt = build_provider_prompts(request)
    raw = provider.generate_candidates(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        candidate_count=request.candidate_count,
    )
    lines = parse_candidate_lines(raw, request.candidate_count)
    candidates: list[CandidateReply] = []
    for line in lines:
        candidates.append(
            CandidateReply(
                text=line,
                tactic=request.plan.selected_tactic,
                objective=request.plan.selected_objective.value,
                persona=request.persona.name,
                estimated_bite_score=0.62,
                estimated_audience_score=0.58,
            )
        )
    return candidates
