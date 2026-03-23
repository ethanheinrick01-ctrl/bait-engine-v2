from __future__ import annotations

from pydantic import BaseModel, Field

from bait_engine.core.types import DecisionPlan, TacticFamily
from bait_engine.planning.personas import PersonaProfile


class CandidateReply(BaseModel):
    text: str
    tactic: TacticFamily | None = None
    objective: str
    persona: str
    estimated_bite_score: float = Field(ge=0.0, le=1.0, default=0.0)
    estimated_audience_score: float = Field(ge=0.0, le=1.0, default=0.0)
    critic_penalty: float = Field(ge=0.0, le=1.0, default=0.0)
    critic_notes: list[str] = Field(default_factory=list)
    rank_score: float = Field(ge=0.0, le=1.0, default=0.0)


class MutationSeed(BaseModel):
    text: str
    variant_id: int | None = None
    family_id: int | None = None
    run_id: int | None = None
    transform: str | None = None
    persona: str | None = None
    platform: str | None = None
    tactic: str | None = None
    objective: str | None = None
    winner_score: float | None = None
    delta_ratio: float | None = None
    novelty_ratio: float | None = None


class DraftRequest(BaseModel):
    source_text: str
    plan: DecisionPlan
    persona: PersonaProfile
    candidate_count: int = Field(ge=1, le=10, default=5)
    mutation_seeds: list[MutationSeed] = Field(default_factory=list)
    mutation_context: str | None = None
    winner_anchors: list[str] = Field(default_factory=list)
    avoid_patterns: list[str] = Field(default_factory=list)


class DraftResult(BaseModel):
    request: DraftRequest
    candidates: list[CandidateReply] = Field(default_factory=list)
