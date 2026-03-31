from __future__ import annotations

from dataclasses import dataclass, field
import re

TOKEN_RE = re.compile(r"\b\w+(?:['’-]\w+)?\b")
QUESTION_RE = re.compile(r"\?")
ALL_CAPS_RE = re.compile(r"\b[A-Z]{3,}\b")
QUOTE_RE = re.compile(r'"[^"]+"|\b(?:you said|so you\'re saying|you mean)\b', re.IGNORECASE)

HEDGES = {
    "maybe",
    "perhaps",
    "possibly",
    "probably",
    "seems",
    "appears",
    "might",
    "could",
    "i think",
    "i guess",
    "sort of",
    "kind of",
}

CERTAINTY_MARKERS = {
    "obviously",
    "clearly",
    "definitely",
    "undeniably",
    "everyone knows",
    "literally",
    "objectively",
    "plainly",
    "certainly",
    "always",
    "never",
}

MORALIZING_MARKERS = {
    "should",
    "ought",
    "moral",
    "immoral",
    "evil",
    "good person",
    "bad person",
    "wrong",
    "harmful",
    "problematic",
    "responsibility",
}

JARGON_MARKERS = {
    "ontology",
    "epistemic",
    "epistemology",
    "metaphysical",
    "heuristic",
    "instrumental",
    "instrumentalism",
    "normative",
    "descriptive",
    "correlation",
    "causation",
    "framework",
    "axiomatic",
    "underdetermined",
    "commitment",
    "ontological",
}

ABSOLUTIST_MARKERS = {
    "always",
    "never",
    "everyone",
    "nobody",
    "all",
    "none",
    "completely",
    "entirely",
    "every",
    "impossible",
}

INSULT_MARKERS = {
    "idiot",
    "moron",
    "stupid",
    "dumb",
    "retard",
    "delusional",
    "pathetic",
    "insane",
    "loser",
    "clown",
}

CONCESSION_MARKERS = {
    "fair",
    "granted",
    "maybe",
    "sure",
    "i agree",
    "to be fair",
    "you may be right",
}

EVIDENCE_MARKERS = {
    "because",
    "since",
    "for example",
    "evidence",
    "data",
    "study",
    "source",
    "citation",
    "shown",
    "demonstrates",
}

QUESTION_OPENERS = {"why", "how", "what", "where", "when", "who"}


@dataclass(slots=True)
class SignalReport:
    text: str
    tokens: list[str]
    token_count: int
    sentence_count: int
    question_count: int
    exclamation_count: int
    all_caps_count: int
    quote_hint_count: int
    hedge_hits: dict[str, int] = field(default_factory=dict)
    certainty_hits: dict[str, int] = field(default_factory=dict)
    moralizing_hits: dict[str, int] = field(default_factory=dict)
    jargon_hits: dict[str, int] = field(default_factory=dict)
    absolutist_hits: dict[str, int] = field(default_factory=dict)
    insult_hits: dict[str, int] = field(default_factory=dict)
    concession_hits: dict[str, int] = field(default_factory=dict)
    evidence_hits: dict[str, int] = field(default_factory=dict)
    second_person_count: int = 0
    first_person_count: int = 0
    question_opener_count: int = 0
    line_count: int = 1
    avg_token_length: float = 0.0

    @property
    def question_density(self) -> float:
        return self.question_count / max(self.sentence_count, 1)

    @property
    def insult_density(self) -> float:
        return sum(self.insult_hits.values()) / max(self.token_count, 1)

    @property
    def hedge_density(self) -> float:
        return sum(self.hedge_hits.values()) / max(self.token_count, 1)

    @property
    def certainty_density(self) -> float:
        return sum(self.certainty_hits.values()) / max(self.token_count, 1)

    @property
    def jargon_density(self) -> float:
        return sum(self.jargon_hits.values()) / max(self.token_count, 1)

    @property
    def moralizing_density(self) -> float:
        return sum(self.moralizing_hits.values()) / max(self.token_count, 1)

    @property
    def absolutist_density(self) -> float:
        return sum(self.absolutist_hits.values()) / max(self.token_count, 1)

    @property
    def concession_density(self) -> float:
        return sum(self.concession_hits.values()) / max(self.token_count, 1)

    @property
    def evidence_density(self) -> float:
        return sum(self.evidence_hits.values()) / max(self.token_count, 1)

    @property
    def lexical_register(self) -> float:
        """0.0 = very simple diction, 1.0 = high vocabulary / academic register.

        Derived from:
        - avg_token_length: primary proxy for word complexity (5-char avg ~ mid, 7+ ~ high)
        - jargon_density: academic/philosophical markers push register up
        - evidence_density: citation-style language signals intellectual engagement
        - avg sentence length (tokens per sentence): longer sentences → more complex syntax
        """
        # avg_token_length: clamp to 3–8 range, normalise to 0–1
        token_len_score = max(0.0, min(1.0, (self.avg_token_length - 3.0) / 5.0))
        # jargon: each jargon hit is already rare, so scale aggressively
        jargon_score = min(1.0, self.jargon_density * 40.0)
        # evidence markers signal structured argument
        evidence_score = min(1.0, self.evidence_density * 20.0)
        # avg sentence length: clamp 5–25 words
        avg_sentence_len = self.token_count / max(self.sentence_count, 1)
        sentence_score = max(0.0, min(1.0, (avg_sentence_len - 5.0) / 20.0))
        return round(
            0.45 * token_len_score
            + 0.30 * jargon_score
            + 0.15 * evidence_score
            + 0.10 * sentence_score,
            4,
        )


def _count_phrase_hits(text_lower: str, phrases: set[str]) -> dict[str, int]:
    hits: dict[str, int] = {}
    for phrase in phrases:
        count = text_lower.count(phrase)
        if count:
            hits[phrase] = count
    return hits


def extract_signals(text: str) -> SignalReport:
    raw = text or ""
    text_lower = raw.lower()
    tokens = TOKEN_RE.findall(raw)
    token_count = len(tokens)
    sentence_count = max(1, len(re.findall(r"[.!?]+", raw)) or (1 if raw.strip() else 0))
    line_count = max(1, raw.count("\n") + 1)
    question_count = len(QUESTION_RE.findall(raw))
    exclamation_count = raw.count("!")
    all_caps_count = len(ALL_CAPS_RE.findall(raw))
    quote_hint_count = len(QUOTE_RE.findall(raw))
    second_person_count = len(re.findall(r"\b(?:you|your|you're|youre|u)\b", text_lower))
    first_person_count = len(re.findall(r"\b(?:i|me|my|mine)\b", text_lower))
    question_opener_count = len(re.findall(r"\b(?:why|how|what|where|when|who)\b", text_lower))
    avg_token_length = sum(len(t) for t in tokens) / max(token_count, 1)

    return SignalReport(
        text=raw,
        tokens=tokens,
        token_count=token_count,
        sentence_count=sentence_count,
        question_count=question_count,
        exclamation_count=exclamation_count,
        all_caps_count=all_caps_count,
        quote_hint_count=quote_hint_count,
        hedge_hits=_count_phrase_hits(text_lower, HEDGES),
        certainty_hits=_count_phrase_hits(text_lower, CERTAINTY_MARKERS),
        moralizing_hits=_count_phrase_hits(text_lower, MORALIZING_MARKERS),
        jargon_hits=_count_phrase_hits(text_lower, JARGON_MARKERS),
        absolutist_hits=_count_phrase_hits(text_lower, ABSOLUTIST_MARKERS),
        insult_hits=_count_phrase_hits(text_lower, INSULT_MARKERS),
        concession_hits=_count_phrase_hits(text_lower, CONCESSION_MARKERS),
        evidence_hits=_count_phrase_hits(text_lower, EVIDENCE_MARKERS),
        second_person_count=second_person_count,
        first_person_count=first_person_count,
        question_opener_count=question_opener_count,
        line_count=line_count,
        avg_token_length=avg_token_length,
    )
