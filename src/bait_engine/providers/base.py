from __future__ import annotations

from abc import ABC, abstractmethod


class TextGenerationProvider(ABC):
    @abstractmethod
    def is_available(self) -> bool: ...

    @abstractmethod
    def generate_candidates(self, *, system_prompt: str, user_prompt: str, candidate_count: int) -> str: ...
