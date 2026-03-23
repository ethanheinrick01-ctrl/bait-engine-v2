from __future__ import annotations

from dataclasses import dataclass, field
import re


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, value))


POSITIVE_TONE_MARKERS = {
    "brilliant",
    "genius",
    "amazing",
    "foolproof",
    "great job",
    "perfect",
    "sure, because",
    "totally",
}

MOCKING_MARKERS = {
    "lol",
    "lmao",
    "sure",
    "yeah right",
    "as if",
    "champ",
    "buddy",
    "righttt",
    "obviously",
}

REVERSAL_MARKERS = {
    "and then",
    "except",
    "yet",
    "but",
    "somehow",
    "meanwhile",
}

EMOJI_MOCK_RE = re.compile(r"(?:😂|🤣|🙄|😬|😒|😏)")
QUOTE_BLOCK_RE = re.compile(r'"[^"]+"|\'[^^\']+\'')
RHETORICAL_SURE_RE = re.compile(r"\bsure[, ]+because\b", re.IGNORECASE)
POSITIVE_LEAD_RE = re.compile(r"\b(?:oh\s+)?(?:great|brilliant|amazing|perfect)\b", re.IGNORECASE)


@dataclass(slots=True)
class SemanticReport:
    sarcasm_probability: float
    irony_probability: float
    polarity_inversion_probability: float
    quoted_text_ratio: float
    literal_confidence: float
    reasons: list[str] = field(default_factory=list)


def infer_semantics(text: str) -> SemanticReport:
    raw = text or ""
    lowered = raw.lower()
    reasons: list[str] = []

    token_count = max(len(re.findall(r"\b\w+(?:['’-]\w+)?\b", lowered)), 1)
    quote_spans = QUOTE_BLOCK_RE.findall(raw)
    quote_chars = sum(len(span) for span in quote_spans)
    quoted_text_ratio = _clamp(quote_chars / max(len(raw), 1))

    sarcasm = 0.0
    irony = 0.0

    mocking_hits = sum(1 for marker in MOCKING_MARKERS if marker in lowered)
    positive_hits = sum(1 for marker in POSITIVE_TONE_MARKERS if marker in lowered)
    reversal_hits = sum(1 for marker in REVERSAL_MARKERS if marker in lowered)

    if mocking_hits:
        sarcasm += min(0.45, 0.12 * mocking_hits)
        reasons.append(f"mocking_markers:{mocking_hits}")
    if positive_hits and mocking_hits:
        sarcasm += min(0.28, 0.10 * positive_hits)
        reasons.append("positive+mocking_mismatch")
    if POSITIVE_LEAD_RE.search(raw) and ("lol" in lowered or "lmao" in lowered):
        sarcasm += 0.22
        reasons.append("sarcastic_praise_pattern")
    if RHETORICAL_SURE_RE.search(raw):
        irony += 0.28
        reasons.append("rhetorical_sure_because")
    if reversal_hits and quote_spans:
        irony += min(0.22, 0.07 * reversal_hits)
        reasons.append("quoted_reversal_pattern")

    emoji_hits = len(EMOJI_MOCK_RE.findall(raw))
    if emoji_hits and positive_hits:
        sarcasm += min(0.22, 0.08 * emoji_hits)
        reasons.append("emoji_polarity_mismatch")

    if quote_spans:
        irony += min(0.3, 0.35 * quoted_text_ratio)
        reasons.append(f"quoted_ratio:{round(quoted_text_ratio, 3)}")

    if token_count <= 5 and mocking_hits:
        sarcasm += 0.08
        reasons.append("short_mocking_jab")

    sarcasm_probability = _clamp(sarcasm)
    irony_probability = _clamp(irony + (0.2 * sarcasm_probability if sarcasm_probability > 0.35 else 0.0))

    polarity_inversion_probability = _clamp(
        0.55 * sarcasm_probability
        + 0.35 * irony_probability
        + (0.15 if (positive_hits and mocking_hits) else 0.0)
    )

    literal_confidence = _clamp(1.0 - (0.62 * polarity_inversion_probability) - (0.28 * quoted_text_ratio))

    return SemanticReport(
        sarcasm_probability=round(sarcasm_probability, 4),
        irony_probability=round(irony_probability, 4),
        polarity_inversion_probability=round(polarity_inversion_probability, 4),
        quoted_text_ratio=round(quoted_text_ratio, 4),
        literal_confidence=round(literal_confidence, 4),
        reasons=reasons,
    )
