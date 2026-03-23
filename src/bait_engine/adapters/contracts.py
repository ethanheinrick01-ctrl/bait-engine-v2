from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class AdapterCapabilitySet(BaseModel):
    can_reply: bool = True
    can_create_thread: bool = False
    supports_editing: bool = False
    supports_deletion: bool = False
    supports_media: bool = False
    supports_thread_lookup: bool = False


class AdapterTarget(BaseModel):
    platform: str
    thread_id: str | None = None
    reply_to_id: str | None = None
    author_handle: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class AdapterSelectionPreset(BaseModel):
    name: str
    strategy: Literal[
        "rank",
        "top_score",
        "highest_bite",
        "highest_audience",
        "lowest_penalty",
        "auto_best",
        "blend_top3",
    ] = "rank"
    tactic: str | None = None
    objective: str | None = None
    dispatch_driver: str | None = None
    notes: list[str] = Field(default_factory=list)


class AdapterDescriptor(BaseModel):
    name: str
    platform: str
    capabilities: AdapterCapabilitySet = Field(default_factory=AdapterCapabilitySet)
    default_selection_preset: str = "default"
    selection_presets: list[AdapterSelectionPreset] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class OutboundReplyEnvelope(BaseModel):
    action: Literal["reply"] = "reply"
    run_id: int
    candidate_rank_index: int
    platform: str
    persona: str
    objective: str | None = None
    tactic: str | None = None
    exit_state: str | None = None
    body: str
    target: AdapterTarget
    metadata: dict[str, Any] = Field(default_factory=dict)
