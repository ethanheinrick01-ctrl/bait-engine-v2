from __future__ import annotations

from bait_engine.generation.contracts import CandidateReply
from bait_engine.planning.personas import PersonaProfile


BOTLIKE_MARKERS = {
    "however",
    "therefore",
    "moreover",
    "in conclusion",
    "it is important to note",
    "fundamentally",
}

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


def starts_with_agreement_language(text: str) -> bool:
    return text.lower().lstrip().startswith(AGREEMENT_OPENERS)


def critique_candidate(candidate: CandidateReply, persona: PersonaProfile) -> CandidateReply:
    notes: list[str] = []
    penalty = 0.0
    text_lower = candidate.text.lower()
    words = candidate.text.split()

    if len(words) > persona.length_band_words[1]:
        penalty += 0.18
        notes.append("too long for persona band")
    if len(words) < persona.length_band_words[0]:
        penalty += 0.08
        notes.append("too short for persona band")
    if any(marker in text_lower for marker in BOTLIKE_MARKERS):
        penalty += 0.25
        notes.append("contains polished connectives")
    if starts_with_agreement_language(candidate.text):
        penalty += 0.55
        notes.append("opens with agreement language")
    if candidate.text.count(",") >= 3:
        penalty += 0.16
        notes.append("too syntactically balanced")
    if candidate.text.endswith(".") and persona.punctuation_style in {"minimal", "loose"}:
        penalty += 0.05
        notes.append("too tidy for persona punctuation style")
    if "because" in text_lower and len(words) > 12:
        penalty += 0.14
        notes.append("drifting toward explanation")
    if candidate.text == candidate.text.lower() and "clean" == persona.punctuation_style:
        notes.append("lowercase acceptable but watch persona polish")

    return candidate.model_copy(update={"critic_penalty": min(1.0, penalty), "critic_notes": notes})
