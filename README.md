# Vanguarr

AI-driven proactive media curation for the Arr stack.

## What It Does

Vanguarr is a Dockerized FastAPI service that:

- pulls Jellyfin playback history for each user
- compresses that history into a persistent LLM-generated V3 persona block
- pulls trending and recommendation candidates from Overseerr or Jellyseerr
- fuzzes each payload against the persona plus global exclusions
- requests only the media that penetrates user interest above the configured confidence threshold

The system is security-minded by design: user interests are treated as the target surface, candidate metadata is treated as the fuzzer payload, and all provider credentials are sourced from environment variables.

## Architecture

### Profile Architect

- cadence: weekly cron
- source: Jellyfin playback history
- output: `/config/profiles/{username}.txt`
- goal: keep a compact V3 profile block under 500 words

### Decision Engine

- cadence: daily cron
- source: Seer trending plus recommendation endpoints
- logic: profile block + candidate payload + global exclusions
- action: POST a Seer request when `decision == REQUEST` and confidence clears the configured threshold

## Web Interface

- `/` dashboard with live health lights, manual triggers, scheduler view, and recent decisions
- `/logs` war room with searchable reasoning history
- `/manifest` editor for raw profile block files
- `/healthz` lightweight container health endpoint
- `/api/health` cached JSON health snapshot for Jellyfin, Seer, and the active LLM provider

## Quick Start

1. Copy `.env.example` to `.env` and fill in Jellyfin, Seer, and LLM settings.
2. Build the stack:

```bash
docker compose build
```

3. Start the web app:

```bash
docker compose up -d
```

4. If you want a local ROCm-backed Ollama sidecar, enable the `local-llm` profile:

```bash
docker compose --profile local-llm up -d
```

The optional Ollama service is wired for ROCm passthrough with `/dev/kfd` and `/dev/dri`. On hosts that do not expose AMD GPU devices, leave the `local-llm` profile disabled and point Vanguarr at OpenAI, Anthropic, or another Ollama endpoint via environment variables.

## Core Environment Variables

- `JELLYFIN_BASE_URL`
- `JELLYFIN_API_KEY`
- `SEER_BASE_URL`
- `SEER_API_KEY`
- `LLM_PROVIDER`
- `LLM_MODEL`
- `OLLAMA_API_BASE`
- `OPENAI_API_KEY`
- `ANTHROPIC_API_KEY`
- `GLOBAL_EXCLUSIONS`
- `REQUEST_THRESHOLD`
- `PROFILE_CRON`
- `DECISION_CRON`

## Development

Run a quick smoke test with:

```bash
pytest
```
