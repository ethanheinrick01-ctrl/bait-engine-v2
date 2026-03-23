from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import json

from bait_engine.generation.contracts import DraftResult


@dataclass(slots=True)
class RunRecord:
    id: int | None
    created_at: str | None
    source_text: str
    platform: str
    persona: str
    selected_objective: str | None
    selected_tactic: str | None
    exit_state: str | None
    analysis_json: str
    plan_json: str


@dataclass(slots=True)
class CandidateRecord:
    id: int | None
    run_id: int
    rank_index: int
    text: str
    tactic: str | None
    objective: str
    estimated_bite_score: float
    estimated_audience_score: float
    critic_penalty: float
    rank_score: float
    critic_notes_json: str


@dataclass(slots=True)
class OutcomeRecord:
    id: int | None
    run_id: int
    got_reply: bool
    reply_delay_seconds: int | None
    reply_length: int | None
    tone_shift: str | None
    spectator_engagement: int | None
    result_label: str | None
    notes: str | None
    emit_outbox_id: int | None = None
    emit_dispatch_id: int | None = None


@dataclass(slots=True)
class PanelReviewRecord:
    id: int | None
    run_id: int
    platform: str
    persona: str
    candidate_tactic: str | None
    candidate_objective: str | None
    selection_preset: str | None
    selection_strategy: str | None
    disposition: str
    notes: str | None


@dataclass(slots=True)
class EmitOutboxRecord:
    id: int | None
    run_id: int
    platform: str
    transport: str
    selection_preset: str | None
    selection_strategy: str | None
    tactic: str | None
    objective: str | None
    status: str
    envelope_json: str
    emit_request_json: str
    notes: str | None


@dataclass(slots=True)
class EmitDispatchRecord:
    id: int | None
    emit_outbox_id: int
    run_id: int
    driver: str
    status: str
    request_json: str
    response_json: str
    notes: str | None


@dataclass(slots=True)
class IntakeTargetRecord:
    id: int | None
    source_driver: str
    source_item_id: str
    platform: str
    thread_id: str
    reply_to_id: str | None
    author_handle: str | None
    subject: str | None
    body: str
    permalink: str | None
    status: str
    score_json: str
    analysis_json: str
    context_json: str
    metadata_json: str
    promoted_run_id: int | None = None
    emit_outbox_id: int | None = None


@dataclass(slots=True)
class MutationFamilyRecord:
    id: int | None
    run_id: int
    winner_candidate_id: int | None
    winner_rank_index: int | None
    persona: str
    platform: str
    tactic: str | None
    objective: str | None
    winner_score: float
    source: str | None
    strategy: str
    notes: str | None
    lineage_json: str


@dataclass(slots=True)
class MutationVariantRecord:
    id: int | None
    family_id: int
    run_id: int
    parent_candidate_id: int | None
    transform: str
    variant_text: str
    variant_hash: str
    status: str
    score_json: str
    lineage_json: str


def to_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def candidates_from_draft(run_id: int, draft: DraftResult) -> list[CandidateRecord]:
    out: list[CandidateRecord] = []
    for idx, candidate in enumerate(draft.candidates, start=1):
        out.append(
            CandidateRecord(
                id=None,
                run_id=run_id,
                rank_index=idx,
                text=candidate.text,
                tactic=candidate.tactic.value if candidate.tactic else None,
                objective=candidate.objective,
                estimated_bite_score=candidate.estimated_bite_score,
                estimated_audience_score=candidate.estimated_audience_score,
                critic_penalty=candidate.critic_penalty,
                rank_score=candidate.rank_score,
                critic_notes_json=to_json(candidate.critic_notes),
            )
        )
    return out
