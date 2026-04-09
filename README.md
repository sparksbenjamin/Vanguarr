# Vanguarr

AI-driven proactive media curation for the Arr stack.

## What It Does

Vanguarr is a Dockerized FastAPI service that:

- pulls Jellyfin playback history for each user
- builds a persistent JSON viewing manifest from grouped watch behavior, ranked genres, format bias, repeat viewing, and recent momentum
- enriches seed titles and top candidates with TMDb keywords, talent, franchise, and brand metadata
- optionally asks the LLM for a few adjacent discovery lanes instead of delegating the whole profile
- pulls blended candidate pools from Seer recommendations seeded by top, repeat-watch, recent, and genre-anchor titles plus a trending pool
- ranks those candidates in code using ranked-genre affinity, source-lane affinity, format fit, freshness, quality, explicit feedback, and diversity controls
- uses the LLM as a light final adjustment and explanation layer instead of the primary ranker
- requests only the media that clears the configured confidence threshold

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
- output: `/data/profiles/{username}.json` plus `/data/profiles/{username}.txt`
- logic: deterministic profile synthesis from watch counts, ranked genres, grouped titles, repeat viewing, recent momentum, and lightweight TMDb seed enrichment
- optional LLM use: suggest a few adjacent discovery lanes
- goal: keep a canonical JSON manifest and a compact derived summary under 500 words

### Decision Engine

- cadence: daily cron
- source: Seer recommendation endpoints plus trending
- logic: build seed lanes from top, repeat-watch, recent, and genre-anchor behavior, fetch Seer recommendations plus trending, enrich the strongest pool items with TMDb metadata, score the pool in code, diversify the shortlist, then apply a light LLM adjustment and explanation pass
- action: POST a Seer request when the hybrid confidence clears the configured threshold

## Mounted Data

Mount `./data` into `/data` in the container if you want direct access to the raw runtime artifacts.

- SQLite decision history: `./data/vanguarr.db`
- user profile manifests: `./data/profiles/*.json`
- derived profile summaries: `./data/profiles/*.txt`
- app log file: `./data/logs/vanguarr.log`

Those files are safe to inspect from the host. The War Room UI reads from the SQLite database, and the Manifest Editor reads and writes the JSON manifests while previewing the derived summary files from the same mounted data path.

## Container Runtime Notes

- The container stores mutable runtime state under `/data`, not `/app`.
- The image is prepared for arbitrary non-root UIDs that are members of group `0`, which keeps it compatible with OKD/OpenShift-style security contexts as long as `/data` is backed by a writable volume.
- The base Docker examples assume service-to-service DNS such as `http://ollama:11434`. Do not rely on `host.docker.internal` outside Docker-based runtimes.

## Web Interface

- `/` dashboard with live health lights, manual triggers, scheduler view, and recent decisions
- `/logs` war room with searchable reasoning history
- `/manifest` editor for JSON profile manifests with derived summary preview
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

Each example below mounts `./data` so you can inspect the raw SQLite database, profile JSON manifests, derived summary files, and the rotating app log from the host.

### Ollama

```yaml
services:
  vanguarr:
    image: ghcr.io/sparksbenjamin/vanguarr:latest
    container_name: vanguarr
    restart: unless-stopped
    ports:
      - "8000:8000"
    volumes:
      - ./data:/data
    environment:
      APP_ENV: production
      TZ: America/New_York
      DATA_DIR: /data
      DATABASE_URL: sqlite:////data/vanguarr.db
      PROFILES_DIR: /data/profiles
      LOGS_DIR: /data/logs
      LOG_FILE: /data/logs/vanguarr.log
      JELLYFIN_BASE_URL: http://jellyfin:8096
      JELLYFIN_API_KEY: your-jellyfin-api-key
      SEER_BASE_URL: http://jellyseerr:5055
      SEER_API_KEY: your-seer-api-key
      SEER_REQUEST_USER_ID: ""
      TMDB_BASE_URL: https://api.themoviedb.org/3
      TMDB_API_READ_ACCESS_TOKEN: your-tmdb-read-access-token
      TMDB_WATCH_REGION: US
      GLOBAL_EXCLUSIONS: No Horror,No Reality TV
      REQUEST_THRESHOLD: "0.72"
      LLM_PROVIDER: ollama
      LLM_MODEL: glm-4.7-flash:latest
      LLM_TIMEOUT_SECONDS: "180"
      OLLAMA_API_BASE: http://ollama:11434
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
    volumes:
      - ./data:/data
    environment:
      APP_ENV: production
      TZ: America/New_York
      DATA_DIR: /data
      DATABASE_URL: sqlite:////data/vanguarr.db
      PROFILES_DIR: /data/profiles
      LOGS_DIR: /data/logs
      LOG_FILE: /data/logs/vanguarr.log
      JELLYFIN_BASE_URL: http://jellyfin:8096
      JELLYFIN_API_KEY: your-jellyfin-api-key
      SEER_BASE_URL: http://jellyseerr:5055
      SEER_API_KEY: your-seer-api-key
      SEER_REQUEST_USER_ID: ""
      TMDB_BASE_URL: https://api.themoviedb.org/3
      TMDB_API_READ_ACCESS_TOKEN: your-tmdb-read-access-token
      TMDB_WATCH_REGION: US
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
    volumes:
      - ./data:/data
    environment:
      APP_ENV: production
      TZ: America/New_York
      DATA_DIR: /data
      DATABASE_URL: sqlite:////data/vanguarr.db
      PROFILES_DIR: /data/profiles
      LOGS_DIR: /data/logs
      LOG_FILE: /data/logs/vanguarr.log
      JELLYFIN_BASE_URL: http://jellyfin:8096
      JELLYFIN_API_KEY: your-jellyfin-api-key
      SEER_BASE_URL: http://jellyseerr:5055
      SEER_API_KEY: your-seer-api-key
      SEER_REQUEST_USER_ID: ""
      TMDB_BASE_URL: https://api.themoviedb.org/3
      TMDB_API_READ_ACCESS_TOKEN: your-tmdb-read-access-token
      TMDB_WATCH_REGION: US
      GLOBAL_EXCLUSIONS: No Horror,No Reality TV
      REQUEST_THRESHOLD: "0.72"
      LLM_PROVIDER: anthropic
      LLM_MODEL: anthropic/your-claude-model
      ANTHROPIC_API_KEY: your-anthropic-api-key
      ANTHROPIC_API_BASE: https://api.anthropic.com
```

### Unraid Host Ollama Override

If Ollama is running directly on the Unraid host instead of another container, add this override to your Docker Compose or template settings:

```yaml
services:
  vanguarr:
    extra_hosts:
      - "host.docker.internal:host-gateway"
    environment:
      OLLAMA_API_BASE: http://host.docker.internal:11434
```

## Unraid and OKD Notes

- Unraid: bind a host path or appdata share to `/data`. If you run Ollama on the Unraid host, use the `host.docker.internal` override above.
- OKD: mount a writable PVC at `/data` and point service URLs such as `JELLYFIN_BASE_URL`, `SEER_BASE_URL`, and `OLLAMA_API_BASE` at cluster DNS names or Routes.
- OKD: with the default SQLite database and in-process scheduler, run a single replica. If you need multiple replicas, disable the built-in scheduler in the web pods and move scheduling plus persistence to cluster-native services.
- OKD: use health probes against `/healthz`.

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
- `PROFILE_ARCHITECT_TOP_TITLES_LIMIT`
- `PROFILE_ARCHITECT_RECENT_MOMENTUM_LIMIT`
- `PROFILE_LLM_ENRICHMENT_ENABLED`
- `PROFILE_LLM_ENRICHMENT_MAX_OUTPUT_TOKENS`
- `CANDIDATE_LIMIT`
- `TRENDING_CANDIDATE_LIMIT`
- `DECISION_SHORTLIST_LIMIT`
- `RECOMMENDATION_SEED_LIMIT`
- `TMDB_SEED_ENRICHMENT_LIMIT`
- `TMDB_CANDIDATE_ENRICHMENT_LIMIT`
- `TMDB_BASE_URL`
- `TMDB_API_READ_ACCESS_TOKEN`
- `TMDB_API_KEY`
- `TMDB_LANGUAGE`
- `TMDB_WATCH_REGION`
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

Local Ollama models can take longer than hosted APIs to finish profile-enrichment and scoring calls.

- `LLM_MODEL` can be either a bare Ollama tag like `glm-4.7-flash:latest` or an explicit LiteLLM form like `ollama/glm-4.7-flash:latest`.
- If `LLM_TIMEOUT_SECONDS` is left blank, Vanguarr defaults to `180` seconds for Ollama and `45` seconds for hosted providers.
- If your Ollama model still times out, set `LLM_TIMEOUT_SECONDS=240` or `300` in your `.env` or compose file.
- Profile Architect now builds the durable profile in code, stores it as JSON, and only uses the model for lightweight adjacent-lane suggestions when enabled.
- `PROFILE_LLM_ENRICHMENT_MAX_OUTPUT_TOKENS=120` keeps that optional profile-side LLM assist small.
- Decision Engine now builds a larger blended pool with `CANDIDATE_LIMIT=160`, includes up to `TRENDING_CANDIDATE_LIMIT=100` discovery titles, and cuts it down to a diversified `DECISION_SHORTLIST_LIMIT=15` before calling the LLM.
- `RECOMMENDATION_SEED_LIMIT=6` controls how many top, repeat-watch, recent, and genre-anchor titles become Seer seed lanes per user.
- `TMDB_SEED_ENRICHMENT_LIMIT=6` controls how many watched seed titles are enriched to build persistent theme, talent, and franchise signals.
- `TMDB_CANDIDATE_ENRICHMENT_LIMIT=30` controls how many top-ranked Seer candidates get a second TMDb metadata pass before the final rerank.
- Profile Architect groups episode watches into show-level counts, emits ranked genres, and weights both durable history and recent momentum by default.
- Larger local models may also benefit from lowering `PROFILE_HISTORY_LIMIT` or switching to a faster quantization.
