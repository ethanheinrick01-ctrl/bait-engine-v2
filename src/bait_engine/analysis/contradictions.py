from __future__ import annotations

import re

from bait_engine.analysis.common import clamp_unit
from bait_engine.analysis.signals import SignalReport
from bait_engine.core.types import ContradictionRecord, ContradictionType


WORD_RE = re.compile(r"[a-z]+(?:'[a-z]+)?")
SENTENCE_SPLIT_RE = re.compile(r"[.!?]+")
DESCRIPTION_ANCHORS = {
    "natural",
    "normal",
    "common",
    "traditional",
    "traditionally",
    "historical",
    "historically",
    "biological",
    "biologically",
    "objective",
    "real",
    "reality",
    "factual",
    "fact",
    "facts",
    "exists",
    "exist",
    "happens",
    "happen",
    "works",
    "work",
}
NORMATIVE_ANCHORS = {
    "should",
    "ought",
    "moral",
    "morally",
    "immoral",
    "immorally",
    "wrong",
    "right",
    "good",
    "bad",
    "acceptable",
    "unacceptable",
    "justified",
    "unjustified",
}
BRIDGE_TOKENS = {"therefore", "thus", "hence", "so"}
BRIDGE_PHRASES = ("which means", "that means", "this means", "that's why", "thats why")
NEGATED_BRIDGE_PHRASES = ("doesn't mean", "doesnt mean", "does not mean", "not a reason", "not grounds")
BRIDGE_WINDOW = 12


def _record(
    kind: ContradictionType,
    severity: float,
    exploitability: float,
    evidence_spans: list[str],
    label: str | None,
) -> ContradictionRecord:
    return ContradictionRecord(
        type=kind,
        severity=clamp_unit(severity),
        exploitability=clamp_unit(exploitability),
        evidence_spans=evidence_spans,
        recommended_label=label,
    )


def _ordered_bridge_match(desc_positions: list[int], bridge_positions: list[int], norm_positions: list[int]) -> bool:
    for bridge in bridge_positions:
        has_description = any(0 < bridge - desc <= BRIDGE_WINDOW for desc in desc_positions)
        has_normativity = any(0 < norm - bridge <= BRIDGE_WINDOW for norm in norm_positions)
        if has_description and has_normativity:
            return True
    return False


def _has_description_vs_normativity_signal(text: str) -> bool:
    for sentence in SENTENCE_SPLIT_RE.split(text):
        lowered = sentence.strip().lower()
        if not lowered or any(phrase in lowered for phrase in NEGATED_BRIDGE_PHRASES):
            continue

        tokens = WORD_RE.findall(lowered)
        if not tokens:
            continue
        token_set = set(tokens)
        if not (token_set & DESCRIPTION_ANCHORS):
            continue
        if not (token_set & NORMATIVE_ANCHORS):
            continue
        if any(phrase in lowered for phrase in BRIDGE_PHRASES):
            return True

        desc_positions = [idx for idx, token in enumerate(tokens) if token in DESCRIPTION_ANCHORS]
        norm_positions = [idx for idx, token in enumerate(tokens) if token in NORMATIVE_ANCHORS]
        bridge_positions = [idx for idx, token in enumerate(tokens) if token in BRIDGE_TOKENS]
        if _ordered_bridge_match(desc_positions, bridge_positions, norm_positions):
            return True

    return False


def mine_contradictions(report: SignalReport) -> list[ContradictionRecord]:
    text = report.text.lower()
    found: list[ContradictionRecord] = []

    if any(term in text for term in ["useful", "works", "practical"]) and any(term in text for term in ["true", "real", "objective"]):
        found.append(
            _record(
                ContradictionType.UTILITY_VS_TRUTH,
                severity=0.78,
                exploitability=0.9,
                evidence_spans=["pragmatic language mixed with truth claims"],
                label="instrumentalism",
            )
        )

    if any(term in text for term in ["because it works", "that explains", "the mechanism", "explains function", "mechanism explains"]) and any(term in text for term in ["therefore it must", "necessarily", "has to be", "necessary", "necessity"]):
        found.append(
            _record(
                ContradictionType.MECHANISM_VS_NECESSITY,
                severity=0.74,
                exploitability=0.82,
                evidence_spans=["mechanism explanation being treated as necessity"],
                label="mechanism_not_necessity",
            )
        )

    if _has_description_vs_normativity_signal(text):
        found.append(
            _record(
                ContradictionType.DESCRIPTION_VS_NORMATIVITY,
                severity=0.67,
                exploitability=0.76,
                evidence_spans=["descriptive premise explicitly bridged into a normative claim"],
                label="is_ought_slip",
            )
        )

    if sum(report.certainty_hits.values()) >= 2 and report.evidence_density < 0.01:
        found.append(
            _record(
                ContradictionType.CONFIDENCE_EVIDENCE_MISMATCH,
                severity=0.71,
                exploitability=0.84,
                evidence_spans=["strong certainty without evidence markers"],
                label="confidence_without_support",
            )
        )

    if report.question_count >= 2 and any(term in text for term in ["define", "what do you mean", "exactly what"]):
        found.append(
            _record(
                ContradictionType.DEFINITION_EVASION,
                severity=0.52,
                exploitability=0.66,
                evidence_spans=["definition pressure present in thread posture"],
                label="definition_drift",
            )
        )

    if any(term in text for term in ["actually", "now", "in this context", "that's not what i said"]) and report.quote_hint_count > 0:
        found.append(
            _record(
                ContradictionType.FRAME_DRIFT,
                severity=0.58,
                exploitability=0.72,
                evidence_spans=["restatement markers plus frame-shift language"],
                label="frame_shift",
            )
        )

    if report.quote_hint_count > 0 and any(term in text for term in ["you said", "then did", "did exactly that", "right after"]):
        found.append(
            _record(
                ContradictionType.HIDDEN_PREMISE_DEPENDENCY,
                severity=0.64,
                exploitability=0.78,
                evidence_spans=["quoted claim contrasted with claimed behavior"],
                label="quote_behavior_mismatch",
            )
        )

    if any(term in text for term in ["all", "every", "none", "never"]) and any(term in text for term in ["except", "sometimes", "in some cases"]):
        found.append(
            _record(
                ContradictionType.SCOPE_SHIFT,
                severity=0.61,
                exploitability=0.73,
                evidence_spans=["absolute claims softened by scope qualifiers"],
                label="scope_creep",
            )
        )

    unique: dict[ContradictionType, ContradictionRecord] = {}
    for record in found:
        existing = unique.get(record.type)
        if existing is None or record.exploitability > existing.exploitability:
            unique[record.type] = record
    return list(unique.values())
