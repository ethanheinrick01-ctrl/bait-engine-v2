from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, Field


Score = float


class RhetoricalAxis(StrEnum):
    EGO_FRAGILITY = "ego_fragility"
    VERBOSITY = "verbosity"
    CERTAINTY = "certainty"
    AGGRESSION = "aggression"
    CURIOSITY = "curiosity"
    SELF_AWARENESS = "self_awareness"
    AUDIENCE_CONSCIOUSNESS = "audience_consciousness"
    JARGON_FLUENCY = "jargon_fluency"
    CONTRADICTION_SUSCEPTIBILITY = "contradiction_susceptibility"
    MORALIZING_TENDENCY = "moralizing_tendency"
    BAIT_HUNGER = "bait_hunger"
    REPLY_STAMINA = "reply_stamina"


class Archetype(StrEnum):
    SPECTRAL = "spectral"
    CONFIDENT_IDIOT = "confident_idiot"
    AGGRESSIVE_POSTER = "aggressive_poster"
    CONSPIRACY_COUNTERPARTY = "conspiracy_counterparty"
    SEALION = "sealion"
    NAIVE_LITERALIST = "naive_literalist"
    OVEREXTENDER = "overextender"


class ContradictionType(StrEnum):
    MECHANISM_VS_NECESSITY = "mechanism_vs_necessity"
    UTILITY_VS_TRUTH = "utility_vs_truth"
    DESCRIPTION_VS_NORMATIVITY = "description_vs_normativity"
    CORRELATION_VS_CAUSATION = "correlation_vs_causation"
    SCOPE_SHIFT = "scope_shift"
    HIDDEN_PREMISE_DEPENDENCY = "hidden_premise_dependency"
    DEFINITION_EVASION = "definition_evasion"
    CONFIDENCE_EVIDENCE_MISMATCH = "confidence_evidence_mismatch"
    EQUIVOCATION = "equivocation"
    FRAME_DRIFT = "frame_drift"


class ThreadPhase(StrEnum):
    HOOK = "hook"
    INFLATION = "inflation"
    EXPOSURE = "exposure"
    COMBUSTION = "combustion"
    CLOSURE = "closure"
    DEAD = "dead"


class TacticalObjective(StrEnum):
    HOOK = "hook"
    INFLATE = "inflate"
    TILT = "tilt"
    MISFRAME = "misframe"
    COLLAPSE = "collapse"
    AUDIENCE_WIN = "audience_win"
    STALL = "stall"
    EXIT_ON_TOP = "exit_on_top"
    RESURRECT = "resurrect"
    BRANCH_SPLIT = "branch_split"
    DO_NOT_ENGAGE = "do_not_engage"


class TacticFamily(StrEnum):
    ESSAY_COLLAPSE = "essay_collapse"
    BURDEN_REVERSAL = "burden_reversal"
    AGREE_AND_ACCELERATE = "agree_and_accelerate"
    CALM_REDUCTION = "calm_reduction"
    FAKE_CLARIFICATION = "fake_clarification"
    ABSURDIST_DERAIL = "absurdist_derail"
    SCHOLAR_HEX = "scholar_hex"
    LABEL_AND_LEAVE = "label_and_leave"
    REVERSE_INTERROGATION = "reverse_interrogation"
    CONCESSION_MAGNIFIER = "concession_magnifier"


class BranchClass(StrEnum):
    DENIAL = "denial"
    CLARIFICATION = "clarification"
    ANGER = "anger"
    SARCASM = "sarcasm"
    CONCESSION = "concession"
    DEFLECTION = "deflection"
    QUESTION_SPAM = "question_spam"
    ESSAY_DEFENSE = "essay_defense"
    SILENCE = "silence"


class AxisScore(BaseModel):
    axis: RhetoricalAxis
    score: Score = Field(ge=0.0, le=1.0)
    confidence: Score = Field(ge=0.0, le=1.0)
    reasons: list[str] = Field(default_factory=list)


class ContradictionRecord(BaseModel):
    type: ContradictionType
    severity: Score = Field(ge=0.0, le=1.0)
    exploitability: Score = Field(ge=0.0, le=1.0)
    evidence_spans: list[str] = Field(default_factory=list)
    recommended_label: str | None = None


class OpportunityScores(BaseModel):
    engagement_value: Score = Field(ge=0.0, le=1.0)
    reply_probability: Score = Field(ge=0.0, le=1.0)
    essay_probability: Score = Field(ge=0.0, le=1.0)
    audience_value: Score = Field(ge=0.0, le=1.0)
    human_plausibility_window: Score = Field(ge=0.0, le=1.0)
    risk_of_boringness: Score = Field(ge=0.0, le=1.0)
    overplay_risk: Score = Field(ge=0.0, le=1.0)


class AnalysisResult(BaseModel):
    source_text: str
    platform: str = "unknown"
    axes: list[AxisScore] = Field(default_factory=list)
    archetype_blend: dict[Archetype, Score] = Field(default_factory=dict)
    contradictions: list[ContradictionRecord] = Field(default_factory=list)
    phase: ThreadPhase = ThreadPhase.HOOK
    opportunity: OpportunityScores
    vulnerabilities: list[str] = Field(default_factory=list)
    recommended_objectives: list[TacticalObjective] = Field(default_factory=list)
    recommended_tactics: list[TacticFamily] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)
    target_register: Score = Field(ge=0.0, le=1.0, default=0.5)


class BranchForecast(BaseModel):
    branch: BranchClass
    probability: Score = Field(ge=0.0, le=1.0)
    follow_up_objective: TacticalObjective
    follow_up_tactic: TacticFamily | None = None
    disengage: bool = False


class PersonaRouterDecision(BaseModel):
    selected_persona: str
    persona_scores: dict[str, Score] = Field(default_factory=dict)
    confidence: Score = Field(ge=0.0, le=1.0, default=0.0)
    why_selected: list[str] = Field(default_factory=list)
    calibration_version: str | None = None
    calibration_timestamp: str | None = None
    segment_confidence: Score = Field(ge=0.0, le=1.0, default=0.0)
    segment_key: str | None = None


class DecisionPlan(BaseModel):
    selected_objective: TacticalObjective
    selected_tactic: TacticFamily | None = None
    alternates: list[TacticFamily] = Field(default_factory=list)
    risk_gates: list[str] = Field(default_factory=list)
    length_band_words: tuple[int, int] = (5, 18)
    tone_constraints: list[str] = Field(default_factory=list)
    branch_forecast: list[BranchForecast] = Field(default_factory=list)
    exit_state: Literal["exit_now", "one_more_spike", "stall_lightly", "abandon"] = "abandon"
    persona_router: PersonaRouterDecision | None = None
