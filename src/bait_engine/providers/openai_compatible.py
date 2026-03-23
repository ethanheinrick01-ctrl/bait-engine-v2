from __future__ import annotations

import json
import os
from urllib import request, error

from bait_engine.providers.base import TextGenerationProvider


class OpenAICompatibleProvider(TextGenerationProvider):
    def __init__(self, api_key: str | None = None, base_url: str | None = None, model: str | None = None, timeout_seconds: int = 30):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.base_url = (base_url or os.getenv("OPENAI_BASE_URL") or "https://api.openai.com/v1").rstrip("/")
        self.model = model or os.getenv("BAIT_ENGINE_MODEL") or os.getenv("OPENAI_MODEL") or "gpt-5.4"
        self.timeout_seconds = timeout_seconds

    def is_available(self) -> bool:
        return bool(self.api_key)

    def generate_candidates(self, *, system_prompt: str, user_prompt: str, candidate_count: int) -> str:
        if not self.is_available():
            raise RuntimeError("OPENAI_API_KEY is not configured")

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.9,
        }

        req = request.Request(
            url=f"{self.base_url}/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )

        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                body = json.loads(resp.read().decode("utf-8"))
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"provider HTTP error {exc.code}: {detail}") from exc
        except error.URLError as exc:
            raise RuntimeError(f"provider connection error: {exc}") from exc

        choices = body.get("choices") or []
        if not choices:
            raise RuntimeError("provider returned no choices")
        message = choices[0].get("message") or {}
        content = message.get("content")
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            return "\n".join(part.get("text", "") for part in content if isinstance(part, dict))
        raise RuntimeError("provider returned unrecognized content format")
