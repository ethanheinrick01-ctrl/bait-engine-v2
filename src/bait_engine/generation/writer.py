from __future__ import annotations

from collections import deque
import hashlib
import math
from typing import TypedDict

from bait_engine.core.types import TacticFamily
from bait_engine.generation.contracts import CandidateReply, DraftRequest


TACTIC_TEMPLATES: dict[TacticFamily, list[str]] = {
    TacticFamily.ESSAY_COLLAPSE: [
        "that's a lot of words to just restate the same mistake",
        "cool so we're back to the same category error",
        "you turned one confusion into a paragraph",
        "more words did not improve the underlying error",
        "three paragraphs, one bad premise, impressive",
        "this is the same thing you said but slower",
        "you stretched a bad take into an essay",
        "length is not the same as argument",
    ],
    TacticFamily.BURDEN_REVERSAL: [
        "why are you treating that like it proves more than it does",
        "what exactly is doing the actual work in that claim",
        "you slipped the premise in and now you're acting like it was established",
        "that's still your job to prove, not mine to disprove",
        "saying it confidently doesn't move the burden back",
        "where's the part where this becomes my problem",
        "you asserted it, so you own the proof",
        "i don't have to disprove what you never proved",
    ],
    TacticFamily.AGREE_AND_ACCELERATE: [
        "exactly and by that logic forks are authoritarian now",
        "yeah sure and traffic laws are metaphysical oppression",
        "fully agreed, which is why gravity is basically a hate crime",
        "right and by that standard chairs are morally suspect",
        "totally, which means the alphabet has blood on its hands",
        "agreed, so clouds should also face consequences",
        "yes and honestly sand should apologize for existing",
        "correct, this means numbers are a form of violence",
    ],
    TacticFamily.CALM_REDUCTION: [
        "interesting amount of emotion for one comment",
        "you seem weirdly animated about this",
        "that's a lot of feeling packed into not much thought",
        "you're pretty invested in a claim this shaky",
        "the volume doesn't fix the logic",
        "that's a lot of heat for a pretty cold argument",
        "all caps won't promote the premise",
        "the feeling is noted, the argument still isn't there",
    ],
    TacticFamily.FAKE_CLARIFICATION: [
        "just to be clear, your point is basically x because vibes, right",
        "so your position is just the sharpened bad version of what you said",
        "wait so you're saying utility equals truth now",
        "to confirm, your whole case rests on that one unproven part",
        "so you're arguing the thing is true because you prefer it to be",
        "just checking, you mean the weaker version of that claim",
        "so the argument is basically trust me on the hard part",
        "to restate this fairly, you need step two to be assumed",
    ],
    TacticFamily.ABSURDIST_DERAIL: [
        "none of this addresses the moon's liability here",
        "counterpoint, spiritually this is losing badly",
        "interesting but the forks remain unconvinced",
        "the staplers have filed a formal objection",
        "meanwhile the concept of Tuesday has a point",
        "i'm going to need a comment from the ceiling",
        "the argument left, the vibes stayed",
        "honestly the wallpaper makes a stronger case",
    ],
    TacticFamily.SCHOLAR_HEX: [
        "that's instrumentalism with extra steps",
        "you're mixing mechanism with necessity again",
        "this is underdetermined and weirdly confident",
        "that's a conflation of correlation with determination",
        "you're treating a defeasible claim like it's entailed",
        "this collapses the distinction between type and token",
        "you've got a strong intuition wearing a weak argument's clothes",
        "that's the fallacy of misplaced concreteness, look it up",
    ],
    TacticFamily.LABEL_AND_LEAVE: [
        "cool, so just cope with punctuation",
        "got it. restatement dressed as rebuttal",
        "nice, a category error in public",
        "noted, a goalpost migration in real time",
        "classic, that was the motte and bailey combo",
        "solid appeal to vibe, no argument detected",
        "right, an ad hominem where a premise should be",
        "understood, the retreat into unfalsifiability",
    ],
    TacticFamily.REVERSE_INTERROGATION: [
        "which part of that did you think actually established anything",
        "why are all your questions doing the work your argument didn't",
        "what answer would even satisfy you there",
        "what would you accept as evidence against your own claim",
        "which of your premises are you actually willing to defend",
        "why does your rebuttal consist entirely of new questions",
        "what's the testable version of what you just said",
        "can you name one thing that would change your position here",
    ],
    TacticFamily.CONCESSION_MAGNIFIER: [
        "right so you basically conceded the frame already",
        "yeah that little concession kind of ends it",
        "appreciate you quietly giving up the important part there",
        "that caveat you slipped in there was the whole argument",
        "you just handed me the load-bearing piece",
        "the qualifier you buried does a lot of work against you",
        "that one admission is going to be expensive",
        "you gave away the key premise and kept going, bold",
    ],
}


class FlavorPack(TypedDict):
    openers: list[str]
    suffixes: list[str]
    closers: list[str]


def _cross_product(parts_a: list[str], parts_b: list[str]) -> list[str]:
    out: list[str] = []
    for left in parts_a:
        for right in parts_b:
            value = " ".join(piece for piece in (left, right) if piece).strip()
            if value:
                out.append(value)
    return out


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        normalized = item.strip()
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(normalized)
    return out


def _build_suffixes(lead_ins: list[str], payloads: list[str], extras: list[str]) -> list[str]:
    variants = _cross_product(lead_ins, payloads)
    variants.extend(extras)
    return _dedupe(variants)


def _build_dry_midwit_suffixes() -> list[str]:
    lead_ins = [
        "just saying",
        "for the record",
        "small note",
        "quick reminder",
        "if we're being precise",
        "to be painfully clear",
        "tiny correction",
        "friendly correction",
        "in plain english",
        "briefly",
    ]
    payloads = [
        "that's not what your evidence does",
        "that still doesn't establish causality",
        "you're substituting confidence for proof",
        "this only works if the premise is true",
        "you're answering a different question",
        "that leap skipped the hard part",
        "the conclusion outruns the support",
        "you smuggled in the key assumption",
    ]
    extras = [
        "lol",
        "not remotely equivalent",
        "same error, longer sentence",
        "saying it confidently isn't the same as proving it",
        "you can be loud or correct, pick one",
        "the math still says no",
        "you'd need one more premise for this to land",
        "same category error, different wording",
        "if this is your strong version, that's rough",
        "you built a frame with nothing inside it",
        "vibes aren't a mechanism",
        "it sounds technical, it isn't",
    ]
    return _build_suffixes(lead_ins, payloads, extras)


def _build_smug_oracle_suffixes() -> list[str]:
    lead_ins = [
        "bro",
        "be serious",
        "come on",
        "you know this",
        "let's not do this",
        "lmao",
        "chief",
        "buddy",
        "respectfully",
        "my guy",
    ]
    payloads = [
        "that ain't proof",
        "you cooked this premise too hard",
        "that's not how burden works",
        "you forgot step one",
        "you argued vibes not logic",
        "you've got confidence and no bridge",
        "that claim has no legs",
        "you're bluffing with big words",
    ]
    extras = [
        "internet confidence isn't evidence",
        "that sounded cool for three seconds",
        "you did a victory lap before the race",
        "you literally proved the opposite",
        "this is cope in bullet points",
        "you got the tone right and the logic wrong",
    ]
    return _build_suffixes(lead_ins, payloads, extras)


def _build_calm_ghoul_suffixes() -> list[str]:
    lead_ins = [
        "hm",
        "anyway",
        "quietly",
        "flatly",
        "to simplify",
        "in short",
        "without drama",
        "plainly",
        "mechanically",
        "cold read",
    ]
    payloads = [
        "that premise fails",
        "the support is missing",
        "this isn't responsive",
        "you changed the claim",
        "that was never established",
        "the mechanism is unproven",
        "your frame collapses",
        "this is overclaimed",
    ]
    extras = [
        "no heat needed",
        "same output either way",
        "short answer, no",
        "the line still breaks here",
        "this does not survive inspection",
    ]
    return _build_suffixes(lead_ins, payloads, extras)


def _build_fake_sincere_suffixes() -> list[str]:
    lead_ins = [
        "genuinely asking",
        "honestly",
        "serious question",
        "curious here",
        "help me understand",
        "just checking",
        "quick question",
        "trying to follow",
        "good-faith question",
        "walk me through this",
    ]
    payloads = [
        "where is the evidence step",
        "which premise carries your conclusion",
        "how does that follow exactly",
        "what would falsify this for you",
        "why does this beat the alternative",
        "what's the mechanism you trust",
        "which claim are we testing",
        "are we redefining terms midstream",
    ]
    extras = [
        "what answer would change your mind",
        "is this claim measurable or just rhetorical",
        "could you steelman the opposite once",
        "what did you rule out first",
        "did you test this or prefer it",
        "what's your strongest verifiable premise",
    ]
    return _build_suffixes(lead_ins, payloads, extras)


def _build_absurdist_suffixes() -> list[str]:
    lead_ins = [
        "incredible scenes",
        "respectfully",
        "lmao",
        "breaking news",
        "plot twist",
        "meanwhile",
        "counterpoint",
        "live update",
        "wild development",
        "spiritually",
    ]
    payloads = [
        "the forks remain unconvinced",
        "the moon rejected jurisdiction",
        "your premise slipped on a banana peel",
        "we're in season four now",
        "the thesis left the group chat",
        "your evidence entered witness protection",
        "gravity filed a complaint",
        "the timeline is offended",
    ]
    extras = [
        "this argument now has a soundtrack",
        "none of this survived daylight",
        "you just speedran a category error",
        "the crowd gasped, the logic didn't",
        "statistically hilarious",
        "this belongs in a museum of bad pivots",
    ]
    return _build_suffixes(lead_ins, payloads, extras)


PERSONA_STYLE_PACKS: dict[str, FlavorPack] = {
    "dry_midwit_savant": {
        "openers": [
            "small correction",
            "premise check",
            "quick calibration",
            "translation",
            "let's tighten this",
            "version that survives contact",
            "if we're scoring rigor",
            "mechanically",
            "clean room pass",
            "diagnosis",
            "reality check",
            "in one line",
            "plain terms",
        ],
        "suffixes": _build_dry_midwit_suffixes(),
        "closers": [
            "that's the whole trick.",
            "that's the gap.",
            "that's the miss.",
            "try again with an actual bridge.",
            "you can fix this with one real premise.",
            "same energy, less drift next pass.",
            "still waiting on the evidence part.",
            "this is salvageable, but not like this.",
            "we're done once that step is proven.",
            "clean this up and it might hold.",
            "plug the premise hole and revisit.",
            "the support just needs to exist.",
        ],
    },
    "smug_moron_oracle": {
        "openers": [
            "bro",
            "be serious",
            "look",
            "quick one",
            "status report",
            "newsflash",
            "translation",
            "anyway",
            "chief",
            "listen",
        ],
        "suffixes": _build_smug_oracle_suffixes(),
        "closers": [
            "we're not doing fantasy accounting.",
            "say it slower next round.",
            "you can do better than this.",
            "that's game.",
            "crowd saw that.",
            "that was not a winning hand.",
            "run it back with evidence.",
            "nice try though.",
            "you had the tone, not the argument.",
            "next time bring the receipts.",
            "the confidence was there, the rest wasn't.",
            "we'll be here when you're ready.",
        ],
    },
    "calm_unbothered_ghoul": {
        "openers": [
            "briefly",
            "plainly",
            "cold read",
            "without heat",
            "simple",
            "minimal version",
            "quiet note",
            "flat answer",
            "mechanics only",
            "short form",
        ],
        "suffixes": _build_calm_ghoul_suffixes(),
        "closers": [
            "that's enough.",
            "nothing else to add.",
            "this is resolved.",
            "same result every time.",
            "no further drama required.",
            "that's the endpoint.",
            "done.",
            "end of line.",
            "the outcome is the same regardless.",
            "no heat needed, it still fails.",
            "noted and filed.",
            "no adjustment necessary.",
        ],
    },
    "fake_sincere_questioner": {
        "openers": [
            "help me map this",
            "genuine question",
            "could you clarify",
            "quick check",
            "walk me through this",
            "i might be missing it",
            "sanity check",
            "for precision",
            "trying to follow",
            "honest fork",
        ],
        "suffixes": _build_fake_sincere_suffixes(),
        "closers": [
            "what am i missing?",
            "where does that step happen?",
            "which part is testable?",
            "can you pin that to evidence?",
            "what would disconfirm this?",
            "which claim should we inspect first?",
            "is that fair?",
            "does that seem right to you?",
            "how does that part get established?",
            "what does the strong version look like?",
            "help me see where i'm wrong here.",
            "can you show the mechanism once?",
        ],
    },
    "absurdist_accelerator": {
        "openers": [
            "live from the timeline",
            "breaking",
            "new patch notes",
            "cinematic cut",
            "plot update",
            "field report",
            "narrator voice",
            "counterpoint from orbit",
            "spectator cam",
            "latest arc",
        ],
        "suffixes": _build_absurdist_suffixes(),
        "closers": [
            "roll credits.",
            "season finale behavior.",
            "the choir of forks has spoken.",
            "this patch is cursed.",
            "we are off the rails now.",
            "someone clip that.",
            "history will not forgive this pivot.",
            "respectfully, chaos wins.",
        ],
    },
}

# Backward compatibility for older references.
PERSONA_FLAVOR = {name: pack["suffixes"] for name, pack in PERSONA_STYLE_PACKS.items()}

# Keep a tiny rolling memory to suppress immediate reuse across adjacent generations.
_RECENT_STYLE_MEMORY: dict[str, dict[str, deque[str]]] = {}


def reset_style_memory() -> None:
    _RECENT_STYLE_MEMORY.clear()


def _component_memory(persona: str, component: str) -> deque[str]:
    persona_memory = _RECENT_STYLE_MEMORY.setdefault(persona, {})
    return persona_memory.setdefault(component, deque(maxlen=6))


def _stable_seed(request: DraftRequest) -> int:
    selected_tactic = request.plan.selected_tactic.value if request.plan.selected_tactic else "none"
    material = "|".join(
        [
            request.persona.name,
            selected_tactic,
            request.source_text,
            request.mutation_context or "",
            "|".join(request.winner_anchors),
            "|".join(request.avoid_patterns),
            str(request.candidate_count),
        ]
    )
    digest = hashlib.blake2b(material.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big")


def _permuted(items: list[str], seed: int, salt: int) -> list[str]:
    if not items:
        return [""]
    unique = _dedupe(items)
    if len(unique) <= 1:
        return unique

    shift = (seed + salt) % len(unique)
    stride = (seed // (salt + 1)) % len(unique)
    stride = max(1, stride)
    while math.gcd(stride, len(unique)) != 1:
        stride += 1

    return [unique[(shift + idx * stride) % len(unique)] for idx in range(len(unique))]


def _select_with_suppression(
    options: list[str],
    index: int,
    recent_local: deque[str],
    recent_global: deque[str],
) -> str:
    if not options:
        return ""

    width = min(len(options), 9)
    for step in range(width):
        candidate = options[(index + step) % len(options)]
        if candidate and (candidate in recent_local or candidate in recent_global):
            continue
        chosen = candidate
        break
    else:
        chosen = options[index % len(options)]

    if chosen:
        recent_local.append(chosen)
        recent_global.append(chosen)
    return chosen


def _strip_avoid_patterns(text: str, avoid_patterns: list[str]) -> str:
    lowered = text.lower()
    if "soft hedge language" in avoid_patterns:
        for hedge in ("maybe ", "probably ", "kind of ", "i think "):
            if hedge in lowered:
                text = text.replace(hedge, "")
                lowered = text.lower()
    return " ".join(text.split())


def _inject_anchor_hint(text: str, anchors: list[str], idx: int) -> str:
    if not anchors or idx % 2 == 1:
        return text
    anchor = anchors[idx % len(anchors)].strip()
    if not anchor:
        return text
    prefix = anchor.split(" ")[0].strip(".,!?;:")
    if not prefix:
        return text
    if prefix.lower() in text.lower():
        return text
    return f"{prefix}, {text}"


def _apply_pressure_profile(text: str, profile: str, idx: int) -> str:
    # Use a hash of (idx, len(text)) to break fingerprint patterns from simple modulo.
    _h = int(hashlib.blake2b(f"{idx}:{len(text)}".encode(), digest_size=4).hexdigest(), 16)
    if profile == "surgical_pinch":
        return f"premise first, {text}" if _h % 3 == 0 else text
    if profile == "taunt_escalator":
        return f"{text} keep pretending" if _h % 2 == 0 else f"{text} lol"
    if profile == "ice_pick":
        return text.replace("?", "").replace("!", "").strip()
    if profile == "velvet_snare":
        return f"quick question, {text}" if _h % 2 == 0 else text
    if profile == "chaos_ramp":
        return f"{text} and somehow this gets weirder"
    return text


def _strip_bot_punctuation(text: str) -> str:
    return " ".join(text.replace(":", " ").replace(";", ",").split())


def _trim_to_band(text: str, min_words: int, max_words: int) -> str:
    words = text.split()
    if len(words) > max_words:
        return " ".join(words[:max_words])
    return text


def _strip_trailing_period(text: str, punctuation_style: str) -> str:
    """Strip a trailing period when punctuation_style is loose or minimal."""
    if punctuation_style in ("loose", "minimal"):
        if text.endswith("."):
            return text[:-1]
    return text


def _lowercase_i(text: str, target_register: float) -> str:
    """Lowercase standalone 'I' and 'I'' contractions when register is low (< 0.35)."""
    import re
    if target_register >= 0.35:
        return text
    # Replace I'm, I've, I'd, I'll, I'm etc. (I followed by apostrophe)
    text = re.sub(r"\bI'", "i'", text)
    # Replace standalone I (not followed by apostrophe)
    text = re.sub(r"\bI\b", "i", text)
    return text


def _apply_contractions(text: str) -> str:
    """Replace common un-contracted forms with contractions (whole-word matches)."""
    import re
    replacements = [
        (r"\byou are\b", "you're"),
        (r"\bdo not\b", "don't"),
        (r"\bit is\b", "it's"),
        (r"\bthat is\b", "that's"),
        (r"\bthey are\b", "they're"),
        (r"\bwe are\b", "we're"),
        (r"\bI am\b", "I'm"),
    ]
    for pattern, contraction in replacements:
        text = re.sub(pattern, contraction, text, flags=re.IGNORECASE)
    return text


# Informal conjunctions and trailing tags, keyed by register tier.
# Low  = target uses simple diction (register < 0.35)
# Mid  = mid-register (0.35–0.65)
# High = elevated vocabulary (> 0.65) — keep formal, no tags
_CONNECTORS: dict[str, list[str]] = {
    "low":  ["cuz", "cause", "tho", "but like", "so like", "and like"],
    "mid":  ["though", "but", "which", "honestly", "and"],
    "high": [],
}
_TRAILING_TAGS: dict[str, list[str]] = {
    "low":  ["lol", "lmao", "ngl", "tbh", "fr", "imo", "lulz"],
    "mid":  ["lol", "ngl", "honestly"],
    "high": [],
}


def _register_tier(target_register: float) -> str:
    if target_register < 0.35:
        return "low"
    if target_register < 0.65:
        return "mid"
    return "high"


def _stitch_clauses(
    opener: str,
    template: str,
    suffix: str,
    closer: str,
    *,
    target_register: float,
    seed: int,
    idx: int,
) -> str:
    """Assemble components using informal conjunctions when appropriate.

    Low-register targets get casual connectors (cuz, tho) and optional
    trailing tags (lol, ngl). High-register targets get normal space joining.
    """
    tier = _register_tier(target_register)
    connectors = _CONNECTORS[tier]
    tags = _TRAILING_TAGS[tier]

    # Use a mix of seed + idx so each candidate in a batch varies.
    rng = (seed + idx * 7919) % (2 ** 31)

    parts = [p for p in (opener, template, suffix, closer) if p]
    if not parts:
        return ""

    if not connectors:
        # High register — plain join
        return " ".join(parts)

    # Decide whether to use a connector between template and suffix.
    use_connector = bool(template and suffix and (rng % 3 != 0))
    # Decide whether to add a trailing tag at the end.
    use_tag = bool(tags and (rng % 4 == 0))
    # Occasionally drop opener entirely for casual brevity (low register).
    drop_opener = tier == "low" and bool(rng % 5 == 0)

    assembled_parts: list[str] = []
    for part in parts:
        if part == opener and drop_opener:
            continue
        assembled_parts.append(part)

    if use_connector and len(assembled_parts) >= 2:
        # Find the junction between template and suffix in assembled_parts
        try:
            t_idx = assembled_parts.index(template)
            if t_idx + 1 < len(assembled_parts) and assembled_parts[t_idx + 1] == suffix:
                connector = connectors[(rng // 3) % len(connectors)]
                assembled_parts[t_idx] = f"{template} {connector}"
        except ValueError:
            pass

    result = " ".join(assembled_parts)

    if use_tag:
        tag = tags[(rng // 5) % len(tags)]
        result = f"{result} {tag}"

    return result.strip()


def _mutation_templates(request: DraftRequest) -> list[str]:
    seen: set[str] = set()
    templates: list[str] = []
    for seed in request.mutation_seeds:
        text = str(seed.text or "").strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        templates.append(text)
    return templates


def generate_candidates(request: DraftRequest) -> list[CandidateReply]:
    plan = request.plan
    if not plan.selected_tactic:
        return []

    mutation_pool = _mutation_templates(request)
    base = TACTIC_TEMPLATES.get(plan.selected_tactic, ["that's not doing what you think it's doing"])
    alternates = []
    for tactic in plan.alternates:
        alternates.extend(TACTIC_TEMPLATES.get(tactic, []))

    pool = mutation_pool + base + alternates

    style_pack = PERSONA_STYLE_PACKS.get(request.persona.name, {"openers": [""], "suffixes": [""], "closers": [""]})
    seed = _stable_seed(request)
    openers = _permuted(style_pack.get("openers", [""]), seed, 17)
    suffixes = _permuted(style_pack.get("suffixes", [""]), seed, 29)
    closers = _permuted(style_pack.get("closers", [""]), seed, 43)

    local_recent_openers: deque[str] = deque(maxlen=4)
    local_recent_suffixes: deque[str] = deque(maxlen=6)
    local_recent_closers: deque[str] = deque(maxlen=4)

    global_recent_openers = _component_memory(request.persona.name, "openers")
    global_recent_suffixes = _component_memory(request.persona.name, "suffixes")
    global_recent_closers = _component_memory(request.persona.name, "closers")

    results: list[CandidateReply] = []
    min_words, max_words = request.persona.length_band_words

    for idx, template in enumerate(pool[: request.candidate_count]):
        use_seed = idx < len(mutation_pool)

        text = template
        if not use_seed:
            opener = _select_with_suppression(openers, idx, local_recent_openers, global_recent_openers)
            suffix = _select_with_suppression(suffixes, idx, local_recent_suffixes, global_recent_suffixes)
            closer = _select_with_suppression(closers, idx, local_recent_closers, global_recent_closers)
            text = _stitch_clauses(
                opener, template, suffix, closer,
                target_register=request.target_register,
                seed=seed,
                idx=idx,
            )

        text = _apply_pressure_profile(text, request.persona.pressure_profile, idx)
        text = _inject_anchor_hint(text, request.winner_anchors, idx)
        text = _strip_avoid_patterns(text, request.avoid_patterns)
        text = _strip_bot_punctuation(text)
        text = _trim_to_band(text, min_words, max_words)
        text = _apply_contractions(text)
        text = _lowercase_i(text, request.target_register)
        text = _strip_trailing_period(text, request.persona.punctuation_style)

        if use_seed:
            tactic = plan.selected_tactic
            estimated_bite = min(1.0, 0.58 + idx * 0.025)
            estimated_audience = min(1.0, 0.54 + idx * 0.02)
        else:
            base_offset = idx - len(mutation_pool)
            tactic = (
                plan.selected_tactic
                if base_offset < len(base)
                else (plan.alternates[(base_offset - len(base)) % max(len(plan.alternates), 1)] if plan.alternates else plan.selected_tactic)
            )
            estimated_bite = min(1.0, 0.45 + base_offset * 0.03)
            estimated_audience = min(1.0, 0.42 + base_offset * 0.025)

        results.append(
            CandidateReply(
                text=text,
                tactic=tactic,
                objective=plan.selected_objective.value,
                persona=request.persona.name,
                estimated_bite_score=estimated_bite,
                estimated_audience_score=estimated_audience,
            )
        )
    return results
