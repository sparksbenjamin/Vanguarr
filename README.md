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

4. If you are using Ollama, point Vanguarr at the already-running Ollama instance:

```bash
OLLAMA_API_BASE=http://host.docker.internal:11434
```

The Docker stack does not run Ollama itself. It connects to an existing Ollama, OpenAI, Anthropic, or compatible endpoint via environment variables. The default Docker example uses `host.docker.internal` so the container can reach an Ollama process running on the host machine.

## Multi-Arch Images

The image is configured for multi-platform builds with Docker Buildx and GitHub Actions.

- supported image targets: `linux/amd64`, `linux/arm64`
- local multi-arch build command:

```bash
docker buildx build --platform linux/amd64,linux/arm64 -t vanguarr:multiarch-test --output=type=oci,dest=dist/vanguarr-multiarch.tar .
```

- CI workflow: [docker.yml](.github/workflows/docker.yml)
- Bake definition: [docker-bake.hcl](docker-bake.hcl)

If you want to publish to a different registry, override `REGISTRY_IMAGE` when invoking Buildx or update the workflow metadata image name.

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
