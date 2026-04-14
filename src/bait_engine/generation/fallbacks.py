from __future__ import annotations

import re

from bait_engine.generation.contracts import DraftRequest


_TOKEN_RE = re.compile(r"[a-z0-9']+")
_STOPWORDS = {
    "about", "actually", "after", "again", "against", "almost", "also", "because", "before",
    "being", "between", "claim", "comment", "conclusion", "could", "does", "doing", "dont",
    "enough", "exactly", "follow", "from", "have", "here", "just", "like", "line", "logic",
    "make", "meant", "more", "need", "only", "over", "part", "point", "prove", "question",
    "really", "same", "seems", "should", "still", "than", "that", "their", "there", "these",
    "thing", "this", "what", "when", "which", "while", "with", "would", "your", "youre",
    "lol", "lmao", "working", "hard", "please", "patient", "ready", "promise", "prom",
}
_LOW_SIGNAL = {"lol", "lmao", "xd", "working", "hard", "please", "patient", "ready", "promise", "prom"}
_HIGH_SIGNAL = {
    "compatibility", "opencode", "claude", "evidence", "premise", "mechanism", "necessity",
    "minimum", "wage", "wages", "prices", "argument", "proof", "claim",
}
_QUESTION_OBJECTIVES = {"hook", "resurrect", "stall", "branch_split"}


def _source_anchor(source_text: str) -> str:
    tokens = [token for token in _TOKEN_RE.findall((source_text or "").lower()) if len(token) >= 4 and token not in _STOPWORDS]
    if not tokens:
        return "that claim"
    unique = list(dict.fromkeys(tokens))
    if not unique:
        return "that claim"
    frequency: dict[str, int] = {}
    for token in tokens:
        frequency[token] = frequency.get(token, 0) + 1

    scored = []
    for idx, token in enumerate(unique):
        score = 0
        score += frequency.get(token, 0) * 2
        if len(token) >= 8:
            score += 1
        if token in _HIGH_SIGNAL:
            score += 3
        if token in _LOW_SIGNAL:
            score -= 4
        if token in {"downvotes", "you'll", "already", "something", "show", "going", "once"}:
            score -= 3
        scored.append((score, -idx, token))

    scored.sort(reverse=True)
    picked = [token for score, _, token in scored if score > 0][:2]
    if not picked:
        picked = [token for _, _, token in scored[:2] if token]
    if not picked:
        return "that claim"
    if len(picked) == 1:
        return picked[0]
    return f"{picked[0]} {picked[1]}"


def build_disagreement_fallbacks(request: DraftRequest) -> list[str]:
    objective = request.plan.selected_objective.value
    tactic = request.plan.selected_tactic.value if request.plan.selected_tactic is not None else ""
    anchor = _source_anchor(request.source_text)
    anchor_label = f"'{anchor}'" if anchor != "that claim" else "that claim"
    if objective in _QUESTION_OBJECTIVES:
        if tactic == "essay_collapse":
            if anchor == "that claim":
                return [
                    "where is the evidence beyond the promise?",
                    "what test makes that conclusion hold up?",
                    "how does that claim survive one hard check?",
                ]
            return [
                f"how does {anchor_label} prove quality instead of just integration?",
                f"where is the evidence beyond {anchor_label}?",
                f"what test shows {anchor_label} means the claim is true?",
            ]
        if anchor == "that claim":
            return [
                "where is the missing step between premise and conclusion?",
                "what proof is supposed to carry that leap?",
                "how does that line establish the actual claim?",
            ]
        return [
            f"where does {anchor_label} actually prove the conclusion?",
            f"what step turns {anchor_label} into a real argument?",
            f"how is {anchor_label} supposed to establish the claim?",
        ]
    if anchor == "that claim":
        return [
            "that leap still doesn't prove your claim",
            "you're skipping the step that actually matters",
            "useful isn't the same as true, that's the gap",
        ]
    return [
        f"{anchor_label} still doesn't prove the point",
        f"you're treating {anchor_label} like proof when it isn't",
        f"{anchor_label} is framing, not evidence",
    ]
