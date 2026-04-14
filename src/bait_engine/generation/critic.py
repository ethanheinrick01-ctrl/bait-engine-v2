from __future__ import annotations

import re

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

OBJECTIVE_REQUIRES_QUESTION = {"hook", "resurrect", "stall", "branch_split"}
OBJECTIVE_REJECTS_QUESTION = {"collapse", "audience_win", "exit_on_top"}
GENERIC_CHALLENGE_MARKERS = {
    "not an argument",
    "still unsupported",
    "prove that",
    "where do you prove",
    "doing all the work",
    "confidence cosplay",
}
SOURCE_TOKEN_RE = re.compile(r"[a-z0-9']+")
GROUNDING_STOPWORDS = {
    "about",
    "actually",
    "after",
    "again",
    "against",
    "almost",
    "also",
    "because",
    "before",
    "being",
    "between",
    "claim",
    "comment",
    "conclusion",
    "could",
    "does",
    "doing",
    "dont",
    "enough",
    "exactly",
    "follow",
    "from",
    "have",
    "here",
    "just",
    "like",
    "line",
    "logic",
    "make",
    "meant",
    "more",
    "need",
    "only",
    "over",
    "part",
    "point",
    "prove",
    "question",
    "really",
    "same",
    "seems",
    "should",
    "still",
    "than",
    "that",
    "their",
    "there",
    "these",
    "thing",
    "this",
    "what",
    "when",
    "which",
    "while",
    "with",
    "would",
    "your",
    "youre",
}


def starts_with_agreement_language(text: str) -> bool:
    return text.lower().lstrip().startswith(AGREEMENT_OPENERS)


def _tokenize(text: str) -> list[str]:
    return SOURCE_TOKEN_RE.findall((text or "").lower())


def grounding_score(text: str, source_text: str) -> float:
    source_tokens = [
        token
        for token in _tokenize(source_text)
        if len(token) >= 4 and token not in GROUNDING_STOPWORDS
    ]
    if not source_tokens:
        return 0.5

    candidate_text = (text or "").lower()
    source_vocab = list(dict.fromkeys(source_tokens))
    overlap = [token for token in source_vocab if token in candidate_text]
    overlap_score = len(overlap) / max(len(source_vocab), 1)

    phrase_bonus = 0.0
    for idx in range(len(source_tokens) - 1):
        phrase = f"{source_tokens[idx]} {source_tokens[idx + 1]}"
        if phrase in candidate_text:
            phrase_bonus = 0.25
            break

    return round(min(1.0, overlap_score + phrase_bonus), 4)


def objective_shape_ok(text: str, objective: str) -> bool:
    objective_key = str(objective or "").strip().lower()
    if not objective_key or objective_key == "do_not_engage":
        return True
    has_question = "?" in text
    if objective_key in OBJECTIVE_REQUIRES_QUESTION:
        return has_question
    if objective_key in OBJECTIVE_REJECTS_QUESTION:
        return not has_question
    return True


def critique_candidate(
    candidate: CandidateReply,
    persona: PersonaProfile,
    *,
    source_text: str = "",
    objective: str = "",
) -> CandidateReply:
    notes: list[str] = []
    penalty = 0.0
    text_lower = candidate.text.lower()
    words = candidate.text.split()
    candidate_grounding = grounding_score(candidate.text, source_text)

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
    if ":" in candidate.text:
        penalty += 0.20
        notes.append("contains colon — remove")
    if ";" in candidate.text:
        penalty += 0.20
        notes.append("contains semicolon — remove")
    if candidate.text.endswith(".") and persona.punctuation_style in {"minimal", "loose"}:
        penalty += 0.05
        notes.append("too tidy for persona punctuation style")
    if "because" in text_lower and len(words) > 12:
        penalty += 0.14
        notes.append("drifting toward explanation")
    if candidate.text == candidate.text.lower() and "clean" == persona.punctuation_style:
        notes.append("lowercase acceptable but watch persona polish")
    if candidate_grounding < 0.18:
        penalty += 0.42
        notes.append("weak source grounding")
    elif candidate_grounding < 0.32:
        penalty += 0.16
        notes.append("light source grounding")
    if not objective_shape_ok(candidate.text, objective):
        penalty += 0.34
        notes.append("objective shape mismatch")
    if any(marker in text_lower for marker in GENERIC_CHALLENGE_MARKERS) and candidate_grounding < 0.22:
        penalty += 0.18
        notes.append("generic challenge line")

    return candidate.model_copy(
        update={
            "grounding_score": candidate_grounding,
            "critic_penalty": min(1.0, penalty),
            "critic_notes": notes,
        }
    )
