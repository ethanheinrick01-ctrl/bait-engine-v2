from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class HuntTarget:
    source_driver: str
    source_item_id: str
    platform: str
    thread_id: str
    reply_to_id: str | None
    author_handle: str | None
    subject: str | None
    body: str
    permalink: str | None = None
    context: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


def compose_source_text(subject: str | None, body: str) -> str:
    clean_subject = (subject or "").strip()
    clean_body = (body or "").strip()
    if clean_subject and clean_body and clean_subject != clean_body:
        return f"{clean_subject}\n\n{clean_body}".strip()
    return clean_body or clean_subject
