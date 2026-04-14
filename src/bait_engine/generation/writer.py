from __future__ import annotations

from collections import deque
import hashlib
import math
import re
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
_SOURCE_WORD_RE = re.compile(r"[a-z0-9']+")
_SOURCE_STOPWORDS = {
    "about", "after", "again", "also", "arguing", "because", "before", "being",
    "claim", "comment", "conclusion", "confusing", "does", "doing", "dont",
    "enough", "exactly", "follow", "from", "here", "just", "like", "make",
    "meant", "more", "only", "part", "really", "same", "should", "still",
    "that", "their", "there", "these", "thing", "this", "those", "what",
    "when", "where", "which", "while", "with", "would", "your", "youre",
    "keep", "saying", "worked", "work", "have", "open", "until", "usually",
    "average", "another", "person", "hours", "hour", "them", "into", "been",
    "added", "tipping", "sales", "there",
}
_OBJECTIVE_REQUIRES_QUESTION = {"hook", "resurrect", "stall", "branch_split"}
_OBJECTIVE_REJECTS_QUESTION = {"collapse", "audience_win", "exit_on_top"}
_CONFUSION_RE = re.compile(r"\b(?:you(?:'re| are)\s+)?confusing\s+(?P<left>[^,.!?;]+?)\s+with\s+(?P<right>[^,.!?;]+)", re.IGNORECASE)
_DOES_NOT_MAKE_RE = re.compile(r"\b(?P<left>[^,.!?;]+?)\s+(?:does not|doesn't)\s+make\s+(?P<right>[^,.!?;]+)", re.IGNORECASE)
_IS_NOT_RE = re.compile(r"\b(?P<left>[^,.!?;]+?)\s+(?:is not|isn't|are not|aren't)\s+(?P<right>[^,.!?;]+)", re.IGNORECASE)
_LEADING_FILLER_RE = re.compile(r"^(?:that|this|these|those|your|you're|you are)\s+", re.IGNORECASE)
_HARD_OPENERS = {"translation", "diagnosis", "mechanically", "version that survives contact"}
_GENERIC_WEAK_TOKENS = {
    "keep", "saying", "worked", "work", "would", "have", "there", "been",
    "open", "until", "usually", "average", "another", "person", "hours", "hour",
    "i", "you", "we", "they", "he", "she", "it", "a", "an", "the", "and",
    "or", "but", "in", "on", "to", "for", "of", "with", "at", "by",
    "that", "this", "these", "those",
    "will", "would", "can", "could", "do", "does", "did", "is", "are", "am",
    "was", "were", "why", "what", "how", "when", "where", "who", "ever",
    "really", "cant", "can't", "myself", "yourself", "himself", "herself", "ourselves", "themselves", "see",
}
_AUXILIARY_TAIL_TOKENS = {"is", "are", "was", "were", "be", "being", "been", "would", "have", "has", "had"}
_QUESTION_LEAD_TOKENS = {
    "will", "would", "can", "could", "do", "does", "did", "is", "are", "am",
    "was", "were", "should", "why", "what", "how", "when", "where", "who",
}


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
        return text
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
    transforms: list[str] = []
    for seed in request.mutation_seeds:
        transform = str(seed.transform or "").strip().lower()
        if transform and transform not in transforms:
            transforms.append(transform)
    return transforms


def _tokenize_source(text: str) -> list[str]:
    return _SOURCE_WORD_RE.findall((text or "").lower())


def _source_terms(text: str) -> list[str]:
    tokens = [
        token
        for token in _tokenize_source(text)
        if len(token) >= 4 and token not in _SOURCE_STOPWORDS
    ]
    return list(dict.fromkeys(tokens))


def _source_fragments(text: str) -> list[str]:
    fragments: list[str] = []
    seen: set[str] = set()
    for block in re.split(r"[.?!\n;]+", text):
        block = " ".join(block.strip().split())
        if not block:
            continue
        pieces = [block]
        if len(block.split()) > 10:
            pieces = [chunk.strip(" ,") for chunk in re.split(r",| but | and | because ", block) if chunk.strip(" ,")]
        for piece in pieces:
            normalized = " ".join(piece.split())
            if len(normalized.split()) < 3:
                continue
            key = normalized.lower()
            if key in seen:
                continue
            seen.add(key)
            fragments.append(normalized)
    return fragments[:6]


def _short_fragment(fragment: str, limit: int = 7) -> str:
    return " ".join(fragment.split()[:limit]).rstrip(",")


def _clean_focus_part(value: str) -> str:
    cleaned = " ".join((value or "").strip().split())
    cleaned = re.sub(r"\byou(?:'d| would)\s+have\s+to\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bwould\s+have\s+to\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.strip(" ,.-")
    cleaned = _LEADING_FILLER_RE.sub("", cleaned)
    words = cleaned.split()
    while words and words[-1].lower() in _AUXILIARY_TAIL_TOKENS:
        words.pop()
    cleaned = " ".join(words)
    return cleaned.strip(" ,.-")


def _generic_subject(fragment: str) -> str:
    normalized = _clean_focus_part(fragment)
    if not normalized:
        return "that claim"

    words = normalized.split()
    strong_words = [word for word in words if word.lower() not in _GENERIC_WEAK_TOKENS]
    candidate_words = strong_words if strong_words else words
    candidate = " ".join(candidate_words[:6]).strip(" ,.-")
    if candidate:
        return candidate

    tokens = [token for token in _source_terms(fragment) if token not in _GENERIC_WEAK_TOKENS]
    if tokens:
        return " ".join(tokens[:4]).strip(" ,.-")
    return "that claim"


def _extract_focus_frame(fragment: str) -> dict[str, str]:
    lead_token = (fragment or "").strip().split()
    question_like = bool(lead_token and lead_token[0].lower().strip(" ,.-") in _QUESTION_LEAD_TOKENS)
    normalized = _clean_focus_part(fragment)
    for pattern, kind in (
        (_CONFUSION_RE, "confusion"),
        (_DOES_NOT_MAKE_RE, "does_not_make"),
        (_IS_NOT_RE, "is_not"),
    ):
        match = pattern.search(fragment)
        if not match:
            continue
        left = _clean_focus_part(match.group("left"))
        right = _clean_focus_part(match.group("right"))
        if left and right:
            return {"kind": kind, "left": left, "right": right, "question_like": "yes" if question_like else "no"}
    subject = _short_fragment(_generic_subject(fragment) or normalized, limit=7)
    return {"kind": "generic", "subject": subject or "that claim", "question_like": "yes" if question_like else "no"}


def _focus_signature(focus: dict[str, str]) -> str:
    kind = focus.get("kind") or "generic"
    if kind == "generic":
        return f"{kind}:{focus.get('subject', '')}:{focus.get('question_like', 'no')}"
    return f"{kind}:{focus.get('left', '')}|{focus.get('right', '')}"


def _generic_anchor(subject: str, *, fragment: str = "") -> str:
    cleaned = " ".join((subject or "").split()).strip(" ,.-")
    if not cleaned:
        return "that angle"
    if "?" in fragment:
        return "that question angle"
    if _looks_like_question_subject(cleaned):
        return "that question angle"

    words = cleaned.split()
    if words and words[0].lower().endswith("ing"):
        return "that angle"
    verbal_tokens = {"is", "are", "was", "were", "be", "being", "been", "looks", "look", "seems", "seem", "feels", "feel", "has", "have", "had"}
    topical = [
        word
        for word in words
        if word.lower() not in verbal_tokens and word.lower() not in _GENERIC_WEAK_TOKENS
    ]
    topical_lower = [word.lower() for word in topical if word]
    math_tokens = {"math", "percent", "menu", "price", "prices", "wage", "wages", "sales", "tips", "hour", "hours"}
    if any(token in math_tokens for token in topical_lower):
        return "that wage math angle"

    return "that angle"


def _looks_like_question_subject(subject: str) -> bool:
    normalized = " ".join((subject or "").strip().split())
    if not normalized:
        return False
    lowered = normalized.lower().strip(" ?")
    if not lowered:
        return False
    lead = lowered.split()[0]
    return lead in _QUESTION_LEAD_TOKENS or normalized.endswith("?")


def _subject_topic(subject: str, *, fragment: str = "") -> str:
    cleaned = " ".join((subject or "").strip().split()).strip(" ,.-?")
    if not cleaned:
        return "that claim"
    if "?" in fragment:
        return "that question"
    if _looks_like_question_subject(cleaned):
        return "that question"
    return cleaned


def _frame_from_fragment(fragment: str, request: DraftRequest, idx: int, role: str = "lead") -> str:
    tactic = request.plan.selected_tactic
    objective = request.plan.selected_objective.value
    focus = _extract_focus_frame(fragment)
    kind = focus.get("kind")

    if kind == "confusion":
        left = focus["left"]
        right = focus["right"]
        if role == "support":
            if tactic == TacticFamily.BURDEN_REVERSAL:
                frame = f"you still need to show {left} becomes {right}"
            elif tactic == TacticFamily.AGREE_AND_ACCELERATE:
                frame = f"treating {left} and {right} as interchangeable is doing all the work"
            elif tactic == TacticFamily.FAKE_CLARIFICATION:
                frame = f"you're still sliding from {left} to {right}"
            elif tactic == TacticFamily.LABEL_AND_LEAVE:
                frame = f"that's just relabeling {left} as {right}"
            elif tactic == TacticFamily.REVERSE_INTERROGATION:
                frame = f"what step turns {left} into {right}"
            elif tactic == TacticFamily.CONCESSION_MAGNIFIER:
                frame = f"you need {left} to count as {right} or this dies"
            else:
                frame = f"{left} still isn't {right}, and that's load-bearing"
        elif role == "sting":
            if tactic == TacticFamily.LABEL_AND_LEAVE:
                frame = f"{left} isn't {right}, that's label swapping"
            elif tactic == TacticFamily.BURDEN_REVERSAL:
                frame = f"{left} still doesn't become {right}, that's what you never proved"
            elif tactic == TacticFamily.REVERSE_INTERROGATION:
                frame = f"{left} isn't {right}, that's the missing step"
            else:
                frame = f"{left} isn't {right}, that's the trick you're hiding"
        elif tactic == TacticFamily.BURDEN_REVERSAL:
            frame = f"where do you show {left} becomes {right}"
        elif tactic == TacticFamily.AGREE_AND_ACCELERATE:
            frame = f"if {left} equals {right}, then words mean nothing"
        elif tactic == TacticFamily.CALM_REDUCTION:
            frame = f"{left} still isn't {right}"
        elif tactic == TacticFamily.FAKE_CLARIFICATION:
            frame = f"so {left} is supposed to equal {right} now"
        elif tactic == TacticFamily.ABSURDIST_DERAIL:
            frame = f"making {left} and {right} identical is still cartoon logic"
        elif tactic == TacticFamily.SCHOLAR_HEX:
            frame = f"{left} isn't {right}, that's the gap"
        elif tactic == TacticFamily.LABEL_AND_LEAVE:
            frame = f"calling {left} {right} is just label swapping"
        elif tactic == TacticFamily.REVERSE_INTERROGATION:
            frame = f"what makes {left} equivalent to {right}"
        elif tactic == TacticFamily.CONCESSION_MAGNIFIER:
            frame = f"you keep needing {left} to count as {right}"
        else:
            frame = f"{left} isn't {right}, that's still the miss"
    elif kind in {"does_not_make", "is_not"}:
        left = focus["left"]
        right = focus["right"]
        relation = "doesn't make" if kind == "does_not_make" else "isn't"
        if role == "support":
            if tactic == TacticFamily.BURDEN_REVERSAL:
                frame = f"you still need to show how {left} gets you {right}"
            elif tactic == TacticFamily.AGREE_AND_ACCELERATE:
                frame = f"if {left} gets you {right}, then anything does"
            elif tactic == TacticFamily.FAKE_CLARIFICATION:
                frame = f"you're still acting like {left} gets you {right}"
            elif tactic == TacticFamily.LABEL_AND_LEAVE:
                frame = f"treating {left} like {right} is still a label swap"
            elif tactic == TacticFamily.REVERSE_INTERROGATION:
                frame = f"what makes {left} enough for {right}"
            elif tactic == TacticFamily.CONCESSION_MAGNIFIER:
                frame = f"you still need {left} to get you {right}"
            else:
                frame = f"{left} still {relation} {right}, and that's the problem"
        elif role == "sting":
            if tactic == TacticFamily.LABEL_AND_LEAVE:
                frame = f"{left} {relation} {right}, that's just a label swap"
            elif tactic == TacticFamily.BURDEN_REVERSAL:
                frame = f"{left} {relation} {right}, that's what you never proved"
            elif tactic == TacticFamily.CONCESSION_MAGNIFIER:
                frame = f"{left} still doesn't get you {right}, that's load-bearing"
            else:
                frame = f"{left} {relation} {right}, that's the leap you're hiding"
        elif tactic == TacticFamily.BURDEN_REVERSAL:
            frame = f"where do you show {left} becomes {right}"
        elif tactic == TacticFamily.AGREE_AND_ACCELERATE:
            frame = f"if {left} gets you {right}, then literally anything does"
        elif tactic == TacticFamily.CALM_REDUCTION:
            frame = f"{left} still {relation} {right}"
        elif tactic == TacticFamily.FAKE_CLARIFICATION:
            frame = f"so {left} is supposed to get you {right}"
        elif tactic == TacticFamily.ABSURDIST_DERAIL:
            frame = f"somehow {left} is doing backflips into {right} now"
        elif tactic == TacticFamily.SCHOLAR_HEX:
            if kind == "does_not_make":
                frame = f"{left} doesn't make {right}, that's the whole gap"
            else:
                frame = f"{left} isn't {right}, that's the whole gap"
        elif tactic == TacticFamily.LABEL_AND_LEAVE:
            frame = f"treating {left} like {right} is a label swap"
        elif tactic == TacticFamily.REVERSE_INTERROGATION:
            frame = f"what makes {left} enough for {right}"
        elif tactic == TacticFamily.CONCESSION_MAGNIFIER:
            frame = f"you still need {left} to get you {right}"
        else:
            frame = f"{left} doesn't get you {right}"
    else:
        subject = focus.get("subject") or "that claim"
        question_like = focus.get("question_like") == "yes"
        topic = _subject_topic(subject, fragment="?" if question_like else fragment)
        generic_anchor = _generic_anchor(subject, fragment="?" if question_like else fragment)
        if role == "support":
            if tactic == TacticFamily.BURDEN_REVERSAL:
                frame = f"you still never proved {topic}"
            elif tactic == TacticFamily.AGREE_AND_ACCELERATE:
                frame = f"you're still asking {topic} to carry way more than it can"
            elif tactic == TacticFamily.FAKE_CLARIFICATION:
                frame = f"you're still asking {topic} to do all the work here"
            elif tactic == TacticFamily.LABEL_AND_LEAVE:
                frame = f"{generic_anchor} is still relabeling, not proof"
            elif tactic == TacticFamily.REVERSE_INTERROGATION:
                if topic == "that question":
                    frame = "what claim is that question supposed to settle"
                else:
                    frame = f"what evidence is supposed to make {topic} true"
            else:
                frame = f"{topic} is still doing all the work for no reason"
        elif role == "sting":
            if tactic == TacticFamily.LABEL_AND_LEAVE:
                frame = f"{generic_anchor} is still just labeling, not proof"
            elif tactic == TacticFamily.BURDEN_REVERSAL:
                frame = f"{topic} is still the part you never proved"
            else:
                frame = f"{topic} is still the trick you're hiding"
        elif tactic == TacticFamily.ESSAY_COLLAPSE:
            frame = f"{topic} is still one bad premise"
        elif tactic == TacticFamily.BURDEN_REVERSAL:
            frame = f"where do you actually prove {topic}"
        elif tactic == TacticFamily.AGREE_AND_ACCELERATE:
            frame = f"if {topic} counts, then literally anything counts"
        elif tactic == TacticFamily.CALM_REDUCTION:
            frame = f"{topic} is still unsupported"
        elif tactic == TacticFamily.FAKE_CLARIFICATION:
            frame = f"so {topic} is supposed to do all the work here"
        elif tactic == TacticFamily.ABSURDIST_DERAIL:
            frame = f"{topic} is doing cartwheels and still proving nothing"
        elif tactic == TacticFamily.SCHOLAR_HEX:
            frame = f"{topic} is still underdetermined"
        elif tactic == TacticFamily.LABEL_AND_LEAVE:
            frame = f"{generic_anchor} is confidence without proof"
        elif tactic == TacticFamily.REVERSE_INTERROGATION:
            if topic == "that question":
                frame = "what claim is that question supposed to prove"
            else:
                frame = f"what evidence is meant to make {topic} true"
        elif tactic == TacticFamily.CONCESSION_MAGNIFIER:
            frame = f"that caveat around {topic} is the whole problem"
        else:
            frame = f"{topic} is not doing the work you think"

    if objective in _OBJECTIVE_REQUIRES_QUESTION and not frame.endswith("?"):
        return f"{frame}?"
    if objective in _OBJECTIVE_REJECTS_QUESTION:
        return frame.rstrip("?")
    return frame if idx % 2 == 0 else frame.rstrip("?")


def _render_source_frame(frame: str, request: DraftRequest, idx: int, seed: int, mutation_hints: list[str]) -> str:
    style_pack = PERSONA_STYLE_PACKS.get(request.persona.name, {"openers": [""], "suffixes": [""], "closers": [""]})
    openers = _permuted(style_pack.get("openers", [""]), seed, 17)
    closers = _permuted(style_pack.get("closers", [""]), seed, 43)
    opener = openers[idx % len(openers)] if openers else ""
    closer = closers[idx % len(closers)] if closers else ""
    text = frame
    _, max_words = request.persona.length_band_words

    can_prefix = len(text.split()) <= max_words - 4 and opener and len(opener.split()) <= 1 and opener.lower() not in _HARD_OPENERS and idx % 2 == 0
    if can_prefix:
        text = f"{opener}, {text}"
    if mutation_hints:
        hint = mutation_hints[idx % len(mutation_hints)]
        if hint == "compress":
            text = text.replace("actually ", "").replace("literally ", "")
        elif hint == "sharpen" and not text.endswith("prove it") and len(text.split()) <= max_words - 2:
            text = f"{text.rstrip('?')}, prove it"
        elif hint == "invert_confidence_posture" and len(text.split()) <= max_words - 4:
            text = f"maybe i'm missing it, {text[0].lower() + text[1:]}" if text else text
        elif hint == "soften_surface_preserve_sting" and not text.endswith("?") and len(text.split()) <= max_words - 3:
            text = f"serious question, {text[0].lower() + text[1:]}?"
        elif hint == "vary_cadence" and "," not in text:
            text = text.replace(" is ", ", is ", 1)

    if closer and idx % 3 == 1 and len(text.split()) + len(closer.split()) <= max_words:
        text = f"{text} {closer}"
    return text


def generate_candidates(request: DraftRequest) -> list[CandidateReply]:
    plan = request.plan
    if not plan.selected_tactic:
        return []

    mutation_hints = _mutation_templates(request)
    seed = _stable_seed(request)
    fragments = _source_fragments(request.source_text)
    if not fragments:
        salient = _source_terms(request.source_text)
        fragments = [" ".join(salient[:6])] if salient else ["that claim"]

    results: list[CandidateReply] = []
    min_words, max_words = request.persona.length_band_words
    recent_frames: set[str] = set()
    weave_roles = ("lead", "support", "sting")

    unique_fragments: list[str] = []
    seen_focus: set[str] = set()
    for fragment in fragments:
        signature = _focus_signature(_extract_focus_frame(fragment))
        if signature in seen_focus:
            continue
        seen_focus.add(signature)
        unique_fragments.append(fragment)
        if len(unique_fragments) >= 3:
            break
    if not unique_fragments:
        unique_fragments = fragments[:1]

    seeded_specs: list[tuple[str, str]] = []
    primary_fragment = unique_fragments[0]
    secondary_fragment = unique_fragments[1] if len(unique_fragments) > 1 else primary_fragment
    seeded_specs.append((primary_fragment, "lead"))
    if request.candidate_count >= 2:
        seeded_specs.append((secondary_fragment, "support"))
    if request.candidate_count >= 3:
        seeded_specs.append((primary_fragment, "sting"))

    def _append_candidate(fragment: str, role: str, idx: int) -> None:
        nonlocal results
        if len(results) >= request.candidate_count:
            return
        frame = _frame_from_fragment(fragment, request, idx, role=role)
        frame_key = frame.lower()
        if frame_key in recent_frames:
            return
        recent_frames.add(frame_key)
        text = _render_source_frame(frame, request, idx, seed, mutation_hints)
        text = _apply_pressure_profile(text, request.persona.pressure_profile, idx)
        text = _inject_anchor_hint(text, request.winner_anchors, idx)
        text = _strip_avoid_patterns(text, request.avoid_patterns)
        text = _strip_bot_punctuation(text)
        text = _trim_to_band(text, min_words, max_words)
        text = _apply_contractions(text)
        text = _lowercase_i(text, request.target_register)
        text = _strip_trailing_period(text, request.persona.punctuation_style)
        if len(text.split()) < min_words:
            return
        results.append(
            CandidateReply(
                text=text,
                tactic=plan.selected_tactic,
                objective=plan.selected_objective.value,
                persona=request.persona.name,
                weave_role=role if len(results) < 3 else None,
                generation_source="heuristic",
                estimated_bite_score=min(1.0, 0.52 + len(results) * 0.04),
                estimated_audience_score=min(1.0, 0.48 + len(results) * 0.03),
            )
        )

    for idx, (fragment, role) in enumerate(seeded_specs):
        _append_candidate(fragment, role, idx)

    for idx in range(request.candidate_count * 3):
        if len(results) >= request.candidate_count:
            break
        fragment = fragments[idx % len(fragments)]
        role = weave_roles[len(results)] if len(results) < min(3, request.candidate_count) else weave_roles[(idx + len(seeded_specs)) % len(weave_roles)]
        _append_candidate(fragment, role, idx + len(seeded_specs))
    return results
