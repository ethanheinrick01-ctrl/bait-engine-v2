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


def _anchor_piece_score(token: str, frequency: dict[str, int]) -> int:
    score = frequency.get(token, 0) * 2
    if len(token) >= 8:
        score += 1
    if token in _HIGH_SIGNAL:
        score += 3
    if token in _LOW_SIGNAL:
        score -= 4
    if token in {"downvotes", "you'll", "already", "something", "show", "going", "once"}:
        score -= 3
    return score


def _source_fragments(source_text: str) -> list[str]:
    fragments: list[str] = []
    seen: set[str] = set()
    for block in re.split(r"[.?!\n;]+", source_text or ""):
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
    return fragments[:8]


def _source_anchor(source_text: str) -> str:
    source = (source_text or "").strip()
    if not source:
        return "that claim"

    fragments = _source_fragments(source)
    if not fragments:
        fragments = [" ".join(source.split())]

    best_fragment = ""
    best_score = 0
    for fragment in fragments:
        tokens = [match.group(0).lower() for match in _TOKEN_RE.finditer(fragment)]
        frequency: dict[str, int] = {}
        for token in tokens:
            if len(token) >= 4 and token not in _STOPWORDS:
                frequency[token] = frequency.get(token, 0) + 1

        fragment_score = sum(_anchor_piece_score(token, frequency) for token in frequency)
        if fragment_score > best_score:
            best_score = fragment_score
            best_fragment = fragment

    if not best_fragment:
        return "that claim"

    matches = [
        (idx, match.group(0).lower(), match.start(), match.end())
        for idx, match in enumerate(_TOKEN_RE.finditer(best_fragment))
        if len(match.group(0)) >= 4 and match.group(0).lower() not in _STOPWORDS
    ]
    if not matches:
        return "that claim"

    frequency = {token: sum(1 for _, other, _, _ in matches if other == token) for _, token, _, _ in matches}
    best_single: tuple[int, int, str] | None = None
    best_pair: tuple[int, int, int, int] | None = None

    for idx, token, _start, _end in matches:
        score = _anchor_piece_score(token, frequency)
        if best_single is None or score > best_single[0] or (score == best_single[0] and idx < best_single[1]):
            best_single = (score, idx, token)

    for left_idx, (_, left_token, _, _) in enumerate(matches):
        for right_offset in range(1, min(5, len(matches) - left_idx)):
            right_idx = left_idx + right_offset
            right = matches[right_idx]
            right_token = right[1]
            span_words = right[0] - matches[left_idx][0] + 1
            if span_words > 6:
                continue
            pair_score = _anchor_piece_score(left_token, frequency) + _anchor_piece_score(right_token, frequency)
            pair_score += max(0, 3 - span_words)
            if best_pair is None or pair_score > best_pair[0] or (
                pair_score == best_pair[0] and span_words < best_pair[3]
            ):
                best_pair = (pair_score, left_idx, right_idx, span_words)

    if best_single is None:
        return "that claim"

    use_pair = best_pair is not None and best_pair[0] >= best_single[0] + 1
    if use_pair:
        left_match = matches[best_pair[1]]
        right_match = matches[best_pair[2]]
        start = left_match[2]
        end = right_match[3]
        anchor = best_fragment[start:end]
    else:
        anchor = best_single[2]

    anchor = " ".join(anchor.split()).strip(" ,.-")
    return anchor or "that claim"


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
