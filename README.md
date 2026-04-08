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

## Jellyfin Requirements

Vanguarr currently reads watched history from Jellyfin's standard item APIs.

- It uses the normal Jellyfin `/Items` endpoint with played-state filters and `DatePlayed` sorting.
- The Jellyfin Playback Reporting plugin is not required for the current implementation.
- If Vanguarr later adds an optional Playback Reporting integration, that requirement will be documented separately.

## Architecture

### Profile Architect

- cadence: weekly cron
- source: Jellyfin playback history
- output: `/app/data/profiles/{username}.txt`
- goal: keep a compact V3 profile block under 500 words

### Decision Engine

- cadence: daily cron
- source: Seer trending plus recommendation endpoints
- logic: profile block + candidate payload + global exclusions
- action: POST a Seer request when `decision == REQUEST` and confidence clears the configured threshold

## Mounted Data

Mount `./data` into the container if you want direct access to the raw runtime artifacts.

- SQLite decision history: `./data/vanguarr.db`
- user persona files: `./data/profiles/*.txt`
- app log file: `./data/logs/vanguarr.log`

Those files are safe to inspect from the host. The War Room UI reads from the SQLite database, and the Manifest Editor reads the profile text files from the same mounted data path.

## Web Interface

- `/` dashboard with live health lights, manual triggers, scheduler view, and recent decisions
- `/logs` war room with searchable reasoning history
- `/manifest` editor for raw profile block files
- `/healthz` lightweight container health endpoint
- `/api/health` cached JSON health snapshot for Jellyfin, Seer, and the active LLM provider

## Quick Start

1. Copy `.env.example` to `.env` and fill in Jellyfin, Seer, and provider values.
2. Build the stack:

```bash
docker compose build
```

3. Start the web app:

```bash
docker compose up -d
```

4. Open the dashboard:

```text
http://localhost:8000
```

## Example Docker Compose

Each example below mounts `./data` so you can inspect the raw SQLite database, profile text files, and the rotating app log from the host.

### Ollama

```yaml
services:
  vanguarr:
    image: ghcr.io/sparksbenjamin/vanguarr:latest
    container_name: vanguarr
    restart: unless-stopped
    ports:
      - "8000:8000"
    extra_hosts:
      - "host.docker.internal:host-gateway"
    volumes:
      - ./data:/app/data
    environment:
      APP_ENV: production
      TZ: America/New_York
      DATA_DIR: /app/data
      DATABASE_URL: sqlite:////app/data/vanguarr.db
      PROFILES_DIR: /app/data/profiles
      LOGS_DIR: /app/data/logs
      LOG_FILE: /app/data/logs/vanguarr.log
      JELLYFIN_BASE_URL: http://jellyfin:8096
      JELLYFIN_API_KEY: your-jellyfin-api-key
      SEER_BASE_URL: http://jellyseerr:5055
      SEER_API_KEY: your-seer-api-key
      SEER_REQUEST_USER_ID: ""
      GLOBAL_EXCLUSIONS: No Horror,No Reality TV
      REQUEST_THRESHOLD: "0.72"
      LLM_PROVIDER: ollama
      LLM_MODEL: glm-4.7-flash:latest
      LLM_TIMEOUT_SECONDS: "180"
      OLLAMA_API_BASE: http://host.docker.internal:11434
```

### OpenAI / ChatGPT

```yaml
services:
  vanguarr:
    image: ghcr.io/sparksbenjamin/vanguarr:latest
    container_name: vanguarr
    restart: unless-stopped
    ports:
      - "8000:8000"
    extra_hosts:
      - "host.docker.internal:host-gateway"
    volumes:
      - ./data:/app/data
    environment:
      APP_ENV: production
      TZ: America/New_York
      DATA_DIR: /app/data
      DATABASE_URL: sqlite:////app/data/vanguarr.db
      PROFILES_DIR: /app/data/profiles
      LOGS_DIR: /app/data/logs
      LOG_FILE: /app/data/logs/vanguarr.log
      JELLYFIN_BASE_URL: http://jellyfin:8096
      JELLYFIN_API_KEY: your-jellyfin-api-key
      SEER_BASE_URL: http://jellyseerr:5055
      SEER_API_KEY: your-seer-api-key
      SEER_REQUEST_USER_ID: ""
      GLOBAL_EXCLUSIONS: No Horror,No Reality TV
      REQUEST_THRESHOLD: "0.72"
      LLM_PROVIDER: openai
      LLM_MODEL: openai/your-openai-model
      OPENAI_API_KEY: your-openai-api-key
      OPENAI_API_BASE: https://api.openai.com/v1
```

### Claude / Anthropic

```yaml
services:
  vanguarr:
    image: ghcr.io/sparksbenjamin/vanguarr:latest
    container_name: vanguarr
    restart: unless-stopped
    ports:
      - "8000:8000"
    extra_hosts:
      - "host.docker.internal:host-gateway"
    volumes:
      - ./data:/app/data
    environment:
      APP_ENV: production
      TZ: America/New_York
      DATA_DIR: /app/data
      DATABASE_URL: sqlite:////app/data/vanguarr.db
      PROFILES_DIR: /app/data/profiles
      LOGS_DIR: /app/data/logs
      LOG_FILE: /app/data/logs/vanguarr.log
      JELLYFIN_BASE_URL: http://jellyfin:8096
      JELLYFIN_API_KEY: your-jellyfin-api-key
      SEER_BASE_URL: http://jellyseerr:5055
      SEER_API_KEY: your-seer-api-key
      SEER_REQUEST_USER_ID: ""
      GLOBAL_EXCLUSIONS: No Horror,No Reality TV
      REQUEST_THRESHOLD: "0.72"
      LLM_PROVIDER: anthropic
      LLM_MODEL: anthropic/your-claude-model
      ANTHROPIC_API_KEY: your-anthropic-api-key
      ANTHROPIC_API_BASE: https://api.anthropic.com
```

## Runtime Environment Variables

- `DATA_DIR`
- `DATABASE_URL`
- `PROFILES_DIR`
- `LOGS_DIR`
- `LOG_FILE`
- `JELLYFIN_BASE_URL`
- `JELLYFIN_API_KEY`
- `SEER_BASE_URL`
- `SEER_API_KEY`
- `SEER_REQUEST_USER_ID`
- `PROFILE_ARCHITECT_MAX_OUTPUT_TOKENS`
- `LLM_PROVIDER`
- `LLM_MODEL`
- `LLM_TIMEOUT_SECONDS`
- `OLLAMA_API_BASE`
- `OPENAI_API_KEY`
- `OPENAI_API_BASE`
- `ANTHROPIC_API_KEY`
- `ANTHROPIC_API_BASE`
- `GLOBAL_EXCLUSIONS`
- `REQUEST_THRESHOLD`
- `PROFILE_CRON`
- `DECISION_CRON`

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

## Development

Run a quick smoke test with:

```bash
python -m pytest
```

## Ollama Timeout Notes

Local Ollama models can take longer than hosted APIs to finish profile-compression and scoring calls.

- `LLM_MODEL` can be either a bare Ollama tag like `glm-4.7-flash:latest` or an explicit LiteLLM form like `ollama/glm-4.7-flash:latest`.
- If `LLM_TIMEOUT_SECONDS` is left blank, Vanguarr defaults to `180` seconds for Ollama and `45` seconds for hosted providers.
- If your Ollama model still times out, set `LLM_TIMEOUT_SECONDS=240` or `300` in your `.env` or compose file.
- Profile Architect uses a smaller dedicated output budget by default via `PROFILE_ARCHITECT_MAX_OUTPUT_TOKENS=384` to keep local summarization runs practical.
- Larger local models may also benefit from lowering `PROFILE_HISTORY_LIMIT` or switching to a faster quantization.
