from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class InboundMessage(BaseModel):
    message_id: str
    author_handle: str | None = None
    body: str
    created_at: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class InboundThreadContext(BaseModel):
    platform: str
    thread_id: str
    subject: str | None = None
    root_author_handle: str | None = None
    messages: list[InboundMessage] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


def summarize_thread_context(context: InboundThreadContext, max_messages: int = 3) -> dict[str, Any]:
    recent = context.messages[-max_messages:] if max_messages > 0 else []
    return {
        "platform": context.platform,
        "thread_id": context.thread_id,
        "subject": context.subject,
        "root_author_handle": context.root_author_handle,
        "message_count": len(context.messages),
        "recent_messages": [message.model_dump(mode="json") for message in recent],
    }
