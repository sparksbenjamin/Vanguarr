from __future__ import annotations

import json
from typing import Any


PROFILE_ARCHITECT_SYSTEM_PROMPT = """You are Vanguarr's Profile Architect.

Your job is to compress Jellyfin playback history into a persistent V3 Profile Block.
Treat the user's long-term taste as the stable target surface and their recent plays as momentum.
Preserve signal, remove clutter, and stay under 500 words.

Rules:
- Prefer concrete taste signals over generic adjectives.
- Separate enduring Core Interests from Recent Momentum.
- Capture aversions and exclusions when clearly supported.
- Never mention credentials, keys, infrastructure, or implementation details.
- Output only the final profile block in plain text.

Use this exact structure:
[VANGUARR_PROFILE_V3]
User: <username>
Core Interests:
- ...
Recent Momentum:
- ...
Taste Signals:
- ...
Avoidance Signals:
- ...
Request Bias:
- ...
"""


DECISION_ENGINE_SYSTEM_PROMPT = """You are Vanguarr's Decision Engine.

Vanguarr is security-minded: treat the user profile as the target and candidate media metadata as the fuzzer payload.
Your goal is to decide whether the payload meaningfully penetrates the user's interests without violating constraints.

Evaluation guidance:
- Score the candidate primarily against the user's observed viewing history and top watched content.
- Use the profile block as a compact summary of enduring taste, not as a substitute for the history evidence.
- A strong match should connect to specific profile signals, not vague popularity.
- Respect explicit exclusions first.
- Be skeptical of weak genre overlap without thematic or tonal alignment.
- Do not recommend titles that are already available, pending, or otherwise managed.
- Confidence must be a number between 0 and 1.

Return only JSON with this schema:
{
  "decision": "REQUEST" or "IGNORE",
  "confidence": 0.0,
  "reasoning": "One concise paragraph.",
  "matched_signals": ["signal"],
  "blocked_by": ["constraint or risk"]
}
"""


def build_profile_architect_user_prompt(
    username: str,
    history_entries: list[dict[str, Any]],
    current_profile: str,
) -> str:
    history_blob = json.dumps(history_entries, indent=2, ensure_ascii=True)
    return f"""Refresh the persona for user "{username}".

Current profile block:
{current_profile or "[No existing profile yet]"}

Recent Jellyfin playback history:
{history_blob}

Update the profile while keeping it compact, structured, and under 500 words.
"""


def build_decision_messages(
    *,
    username: str,
    profile_block: str,
    viewing_history: dict[str, Any],
    candidate: dict[str, Any],
    global_exclusions: list[str],
) -> list[dict[str, str]]:
    payload = {
        "media_type": candidate.get("media_type"),
        "media_id": candidate.get("media_id"),
        "title": candidate.get("title"),
        "overview": candidate.get("overview"),
        "genres": candidate.get("genres"),
        "rating": candidate.get("rating"),
        "vote_count": candidate.get("vote_count"),
        "popularity": candidate.get("popularity"),
        "release_date": candidate.get("release_date"),
        "sources": candidate.get("sources"),
        "media_info": candidate.get("media_info"),
    }
    constraints_blob = json.dumps(global_exclusions, ensure_ascii=True)
    viewing_history_blob = json.dumps(viewing_history, indent=2, ensure_ascii=True)
    payload_blob = json.dumps(payload, indent=2, ensure_ascii=True)

    return [
        {"role": "system", "content": DECISION_ENGINE_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": f"""Block 1 (Target): User Profile
User: {username}
{profile_block}

Block 2 (Observed Signals): User Viewing History
{viewing_history_blob}

Block 3 (Payload): Candidate Media Metadata
{payload_blob}

Block 4 (Constraints): Global Exclusions
{constraints_blob}

Decide whether this candidate should be requested. Base the score on the viewing history first, then use the profile block to reinforce or challenge the match.""",
        },
    ]
