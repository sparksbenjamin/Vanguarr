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
- Treat the profile manifest JSON as the canonical persisted taste model.
- Use the summary block as a compact human-readable view of enduring taste, not as a substitute for the history evidence.
- Treat the code-derived recommendation features as a strong prior; only push against them when there is a clear mismatch.
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
    profile_payload: dict[str, Any],
    viewing_history: dict[str, Any],
    candidate: dict[str, Any],
    global_exclusions: list[str],
) -> list[dict[str, str]]:
    summary_block = str(profile_payload.get("summary_block") or "").strip()
    profile_manifest = {
        key: value
        for key, value in profile_payload.items()
        if key != "summary_block"
    }
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
        "recommendation_features": candidate.get("recommendation_features"),
    }
    constraints_blob = json.dumps(global_exclusions, ensure_ascii=True)
    viewing_history_blob = json.dumps(viewing_history, indent=2, ensure_ascii=True)
    profile_blob = json.dumps(profile_manifest, indent=2, ensure_ascii=True)
    payload_blob = json.dumps(payload, indent=2, ensure_ascii=True)

    return [
        {"role": "system", "content": DECISION_ENGINE_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": f"""Block 1 (Target): Canonical User Profile JSON
User: {username}
{profile_blob}

Block 2 (Target Summary): Derived Profile Summary
{summary_block}

Block 3 (Observed Signals): User Viewing History
{viewing_history_blob}

Block 4 (Payload): Candidate Media Metadata
{payload_blob}

Block 5 (Constraints): Global Exclusions
{constraints_blob}

Decide whether this candidate should be requested. Base the score on the viewing history and code-derived recommendation features first, then use the profile manifest and summary to reinforce or challenge the match.""",
        },
    ]
