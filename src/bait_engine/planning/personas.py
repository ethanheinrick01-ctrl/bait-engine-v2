from __future__ import annotations

from pydantic import BaseModel, Field

from bait_engine.core.types import TacticFamily


class PersonaProfile(BaseModel):
    name: str
    length_band_words: tuple[int, int] = (5, 18)
    tone_tags: list[str] = Field(default_factory=list)
    jargon_ceiling: float = Field(ge=0.0, le=1.0, default=0.5)
    absurdity_tolerance: float = Field(ge=0.0, le=1.0, default=0.5)
    calmness_preference: float = Field(ge=0.0, le=1.0, default=0.5)
    punctuation_style: str = "sparse"
    pressure_profile: str = "measured_pinch"
    escalation_cues: list[str] = Field(default_factory=list)
    forbidden_tactics: list[TacticFamily] = Field(default_factory=list)


DEFAULT_PERSONAS: dict[str, PersonaProfile] = {
    "dry_midwit_savant": PersonaProfile(
        name="dry_midwit_savant",
        length_band_words=(6, 18),
        tone_tags=["dry", "casual", "slightly technical"],
        jargon_ceiling=0.85,
        absurdity_tolerance=0.35,
        calmness_preference=0.72,
        punctuation_style="sparse",
        pressure_profile="surgical_pinch",
        escalation_cues=["tight correction", "premise check", "quiet finality"],
        forbidden_tactics=[],
    ),
    "smug_moron_oracle": PersonaProfile(
        name="smug_moron_oracle",
        length_band_words=(4, 14),
        tone_tags=["smug", "casual", "half-literate danger"],
        jargon_ceiling=0.45,
        absurdity_tolerance=0.62,
        calmness_preference=0.54,
        punctuation_style="loose",
        pressure_profile="taunt_escalator",
        escalation_cues=["mock confidence", "status jab", "crowd bait"],
        forbidden_tactics=[],
    ),
    "calm_unbothered_ghoul": PersonaProfile(
        name="calm_unbothered_ghoul",
        length_band_words=(3, 12),
        tone_tags=["cold", "minimal", "calm"],
        jargon_ceiling=0.4,
        absurdity_tolerance=0.25,
        calmness_preference=0.94,
        punctuation_style="minimal",
        pressure_profile="ice_pick",
        escalation_cues=["flat affect", "minimal words", "walk-away finish"],
        forbidden_tactics=[TacticFamily.ABSURDIST_DERAIL],
    ),
    "fake_sincere_questioner": PersonaProfile(
        name="fake_sincere_questioner",
        length_band_words=(6, 20),
        tone_tags=["curious", "polite", "destabilizing"],
        jargon_ceiling=0.5,
        absurdity_tolerance=0.2,
        calmness_preference=0.78,
        punctuation_style="clean",
        pressure_profile="velvet_snare",
        escalation_cues=["polite trap", "precision question", "forced fork"],
        forbidden_tactics=[TacticFamily.CALM_REDUCTION],
    ),
    "absurdist_accelerator": PersonaProfile(
        name="absurdist_accelerator",
        length_band_words=(5, 16),
        tone_tags=["playful", "derailing", "chaotic"],
        jargon_ceiling=0.3,
        absurdity_tolerance=0.96,
        calmness_preference=0.38,
        punctuation_style="loose",
        pressure_profile="chaos_ramp",
        escalation_cues=["absurd metaphor", "rapid pivot", "spectator wink"],
        forbidden_tactics=[TacticFamily.SCHOLAR_HEX],
    ),
}


def get_persona(name: str | None = None) -> PersonaProfile:
    if not name:
        return DEFAULT_PERSONAS["dry_midwit_savant"]
    return DEFAULT_PERSONAS.get(name, DEFAULT_PERSONAS["dry_midwit_savant"])
