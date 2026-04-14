from __future__ import annotations

import json
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
        "Keep every line grounded in the source text. "
        "Output either plain candidate lines or a JSON array of candidate strings."
    )
    mutation_lines: list[str] = []
    mutation_seeds = payload.get("mutation_seeds") or []
    if mutation_seeds:
        rendered = "\n".join(_render_mutation_seed(seed) for seed in mutation_seeds)
        mutation_lines.extend(
            [
                "Mutation seeds (reuse cadence/pressure when useful; do not copy them verbatim):",
                rendered,
            ]
        )
    mutation_context = payload.get("mutation_context")
    if mutation_context:
        mutation_lines.append(f"Mutation context: {mutation_context}")
    winner_anchors = payload.get("winner_anchors") or []
    if winner_anchors:
        mutation_lines.append(f"Winner anchors: {', '.join(str(item) for item in winner_anchors)}")
    avoid_patterns = payload.get("avoid_patterns") or []
    if avoid_patterns:
        mutation_lines.append(f"Avoid patterns: {', '.join(str(item) for item in avoid_patterns)}")
    pressure_profile = payload.get("persona", {}).get("pressure_profile")
    escalation_cues = payload.get("persona", {}).get("escalation_cues")
    sections = [
        "Source text:",
        str(payload["source_text"]),
        "",
        "Persona:",
        f"- name: {payload['persona'].get('name')}",
        f"- length_band_words: {payload['persona'].get('length_band_words')}",
        f"- tone_tags: {', '.join(payload['persona'].get('tone_tags') or [])}",
        f"- pressure_profile: {pressure_profile}",
        f"- escalation_cues: {', '.join(str(item) for item in (escalation_cues or []))}",
        "",
        "Plan:",
        f"- objective: {payload['plan'].get('objective')}",
        f"- tactic: {payload['plan'].get('tactic')}",
        f"- alternates: {', '.join(str(item) for item in (payload['plan'].get('alternates') or [])) or 'none'}",
        f"- exit_state: {payload['plan'].get('exit_state')}",
        f"- target_register: {payload.get('target_register')}",
        "",
        "Rules:",
        *[f"- {rule}" for rule in payload["writer_rules"]],
    ]
    if request.candidate_count >= 3:
        sections.extend(
            [
                "",
                "Weave guidance:",
                "- Make the first three candidates complementary enough to fuse into one reply.",
                "- Candidate 1 should be the lead contradiction.",
                "- Candidate 2 should add a secondary pressure point.",
                "- Candidate 3 should land a sting or closing label.",
            ]
        )
    if mutation_lines:
        sections.extend(["", *mutation_lines])
    sections.extend(
        [
            "",
            "Return format:",
            f"- Return exactly {request.candidate_count} distinct candidates.",
            "- Each candidate must be short, source-grounded, and usable as-is.",
            "- Output either one candidate per line or a JSON array of candidate strings.",
        ]
    )
    user_prompt = "\n".join(sections)
    return system_prompt, user_prompt


def parse_candidate_lines(raw: str, candidate_count: int) -> list[str]:
    stripped = raw.strip()
    if stripped.startswith("[") and stripped.endswith("]"):
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError:
            payload = None
        if isinstance(payload, list):
            raw = "\n".join(str(item) for item in payload)

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
    if len(lines) < request.candidate_count:
        raise RuntimeError(f"provider returned only {len(lines)} valid candidates")
    candidates: list[CandidateReply] = []
    weave_roles = ("lead", "support", "sting")
    for idx, line in enumerate(lines):
        candidates.append(
            CandidateReply(
                text=line,
                tactic=request.plan.selected_tactic,
                objective=request.plan.selected_objective.value,
                persona=request.persona.name,
                weave_role=weave_roles[idx] if idx < len(weave_roles) else None,
                generation_source="provider",
                estimated_bite_score=0.62,
                estimated_audience_score=0.58,
            )
        )
    return candidates
