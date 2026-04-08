from __future__ import annotations

import json
from typing import Any


PROFILE_ENRICHMENT_SYSTEM_PROMPT = """You are Vanguarr's profile enrichment assistant.

The core viewing profile has already been built in code from actual Jellyfin history.
Your only job is to suggest a few adjacent discovery lanes that are close to the user's proven tastes.

Rules:
- Return JSON only.
- Suggest at most 3 adjacent genres or sub-genres.
- Suggest at most 2 short adjacent themes.
- Do not repeat genres already listed as primary.
- Keep labels short and mainstream.
- Do not mention infrastructure, credentials, or implementation details.

Return JSON with this schema:
{
  "adjacent_genres": ["genre"],
  "adjacent_themes": ["theme"],
  "notes": "One short sentence."
}
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


def build_profile_enrichment_messages(
    username: str,
    history_summary: dict[str, Any],
) -> list[dict[str, str]]:
    history_blob = json.dumps(history_summary, indent=2, ensure_ascii=True)
    return [
        {"role": "system", "content": PROFILE_ENRICHMENT_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": f"""Suggest adjacent discovery lanes for user "{username}".

Use this code-derived viewing summary as the only source of truth:
{history_blob}

Return compact JSON only.""",
        },
    ]


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
