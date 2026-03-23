from __future__ import annotations

from bait_engine.adapters.contracts import AdapterTarget
from bait_engine.adapters.registry import DEFAULT_ADAPTERS


def validate_target(target: AdapterTarget) -> None:
    try:
        descriptor = DEFAULT_ADAPTERS[target.platform]
    except KeyError as exc:
        raise KeyError(f"adapter '{target.platform}' not found") from exc

    capabilities = descriptor.capabilities
    if not capabilities.can_reply:
        raise ValueError(f"adapter '{target.platform}' does not support replies")

    if target.reply_to_id is None and target.thread_id is None:
        raise ValueError(f"adapter '{target.platform}' requires thread_id or reply_to_id")

    if target.thread_id is not None and not capabilities.can_create_thread and not capabilities.supports_thread_lookup:
        if target.reply_to_id is None:
            raise ValueError(
                f"adapter '{target.platform}' requires explicit reply_to_id when thread lookup/thread creation are unavailable"
            )

    if target.reply_to_id is not None and target.thread_id is None and not capabilities.supports_thread_lookup:
        raise ValueError(f"adapter '{target.platform}' cannot infer thread context from reply_to_id")
