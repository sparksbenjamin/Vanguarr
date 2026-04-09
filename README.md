# Vanguarr

[![Docker workflow](https://github.com/sparksbenjamin/Vanguarr/actions/workflows/docker.yml/badge.svg)](https://github.com/sparksbenjamin/Vanguarr/actions/workflows/docker.yml)
[![Tests](https://github.com/sparksbenjamin/Vanguarr/actions/workflows/tests.yml/badge.svg)](https://github.com/sparksbenjamin/Vanguarr/actions/workflows/tests.yml)
[![Latest tag](https://img.shields.io/github/v/tag/sparksbenjamin/Vanguarr?sort=semver)](https://github.com/sparksbenjamin/Vanguarr/tags)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-3776AB?logo=python&logoColor=white)](#local-development)
[![License: GPL v3](https://img.shields.io/badge/license-GPLv3-blue.svg)](LICENSE)

> Vanguarr is the scout of the ARR stack.

Vanguarr is a self-hosted media-server recommendation engine and ARR automation layer. It watches what people actually play in Jellyfin or Plex, maps that behavior into durable taste manifests, scouts recommendation territory through Seer-compatible request services such as Jellyseerr, and only sends high-confidence requests downstream.

Instead of handing the whole decision loop to an LLM, Vanguarr keeps the important parts inspectable: profiles are stored on disk, ranking happens in code, request decisions are logged, and the model stays in a narrow assist role. The result is a system that is easier to trust, easier to tune, and easier to operate than a "just ask the model" workflow.

## If You're Looking For

Vanguarr is relevant if you're searching for any of these:

- a Jellyfin or Plex recommendation engine
- Jellyseerr automation or Seer request automation
- self-hosted media request automation for the ARR stack
- watch-history-based AI recommendations for movies and TV
- an explainable alternative to opaque LLM-only media recommendation tools

## At A Glance

- Learns from real media-server watch history, not generic popularity.
- Builds persistent user manifests you can inspect, edit, and tune.
- Pulls candidates from Seer-compatible request stacks such as Jellyseerr and scores them in code.
- Can sync per-user `Suggested for You` Jellyfin playlists through the companion `Vanguarr` plugin without per-user libraries or symlink trees.
- Uses optional TMDb enrichment and optional LLM assistance without surrendering control.
- Gives operators a dashboard, a War Room log, and a manifest editor out of the box.

## Why Vanguarr

- It acts like a scout, not a black box: Vanguarr surveys the field, reports what it found, and only then asks the stack to move.
- Deterministic first: recommendation scoring happens in code and is stored for review.
- Persistent profiles: each user gets a durable JSON manifest plus a derived summary block.
- LLMs stay narrow: they suggest adjacent lanes and provide a final vote, but they do not own the pipeline.
- Operator-friendly UI: dashboard, health board, decision log, and manifest editor are built in.
- Host-visible runtime state: SQLite history, profiles, and logs live under `./data`.
- Safe deployment model: Dockerized, single-container friendly, and compatible with arbitrary non-root UIDs in group `0`.

## What It Does

Vanguarr runs two cooperating engines:

- `Profile Architect` reads Jellyfin or Plex playback history, groups repeat watches, ranks genres, infers format and release-era preferences, optionally asks the LLM for a few adjacent discovery lanes, and writes a persistent profile manifest to disk.
- `Decision Engine` builds recommendation seed lanes from top watched, repeat watched, recent, and genre-anchor titles, pulls a blended candidate pool from Seer recommendations plus trending, enriches the best items with TMDb metadata, scores them in code, applies a small LLM adjustment, and requests only titles that clear your threshold.

In plain English: Vanguarr watches, remembers, scouts, scores, explains, and only then requests.

```mermaid
flowchart LR
    A["Media server watch history"] --> B["Profile Architect"]
    B --> C["Profile JSON manifest"]
    B --> D["Summary block (.txt)"]
    A --> E["Seed lanes<br/>top / repeat / recent / genre-anchor"]
    E --> F["Seer recommendations + trending"]
    F --> G["Deterministic ranking in code"]
    C --> G
    H["TMDb enrichment (optional)"] --> G
    G --> I["Diversified shortlist"]
    I --> J["LLM final vote (optional assist)"]
    J --> K{"Confidence >= threshold?"}
    K -->|Yes| L["Create Seer request"]
    K -->|No| M["Store IGNORE decision"]
    L --> N["SQLite logs + dashboard"]
    M --> N
```

## Feature Highlights

- Positioned for Jellyfin, Plex, and Arr operators who want automation without mystery
- FastAPI web application with a built-in operator UI
- APScheduler-based recurring jobs with manual run controls
- Jellyfin integration through standard `/Users` and `/Items` APIs
- Plex integration through playback history and metadata APIs
- No Jellyfin Playback Reporting plugin required when Jellyfin is the source
- Seer discovery via recommendations and trending endpoints
- TMDb enrichment for keywords, talent, brands, franchises, certifications, and providers
- Hybrid confidence model that blends deterministic scoring with an LLM vote
- Duplicate protection against already watched, already managed, or already requested media
- Searchable decision log with stored reasoning and request outcomes
- Editable manifest workflow for operator overrides and explicit feedback
- Database-backed runtime settings with a live Settings page and prioritized LLM failover
- Optional Jellyfin companion plugin that syncs per-user `Suggested for You` playlists from Vanguarr snapshots

## Web Interface And Endpoints

| Route | Purpose |
| --- | --- |
| `/` | Main dashboard with health, manual triggers, scheduler state, recent requests, and profile cards |
| `/logs` | "War Room" decision log with search across usernames, titles, and reasoning |
| `/settings` | Runtime settings editor backed by the SQLite database |
| `/settings/save` | Saves runtime settings and applies them live without restarting the app |
| `/manifest` | Profile manifest editor with live summary preview |
| `/healthz` | Lightweight container health probe |
| `/api/health` | Cached JSON health snapshot for the active media server, Seer, TMDb, and the active LLM provider |
| `/api/jellyfin/suggestions` | Bearer-protected API the Jellyfin `Vanguarr` plugin uses to fetch per-user suggestion snapshots |
| `/api/webhooks/seer` | Bearer-protected webhook endpoint for Seer availability updates that should refresh user suggestions |
| `/actions/profile-architect` | Manual Profile Architect trigger |
| `/actions/decision-engine` | Manual Decision Engine trigger |
| `/actions/suggested-for-you` | Manual Suggested For You snapshot rebuild for Jellyfin users |
| `/manifest/save` | Saves the canonical JSON manifest and regenerates its summary block |

## Quick Start

### Prerequisites

You need these integrations configured before Vanguarr can make real request decisions:

- Jellyfin or Plex server URL and credential
- Seer-compatible request service URL and API key
- writable `./data` directory for runtime state

These are optional but strongly recommended:

- TMDb credentials for metadata enrichment
- an LLM provider (`ollama`, `openai`, or `anthropic`) for adjacent-lane suggestions and final vote assistance

### Run With Docker Compose

1. Copy [`.env.example`](.env.example) to `.env`.
2. Fill in the required media-server and Seer values.
3. Optionally add first-start seed values for TMDb, scheduling, or the initial LLM provider.
4. Start the stack:

```bash
docker compose up -d --build
```

5. Open the UI:

```text
http://localhost:8000
```

6. Open `/settings` after first boot if you want to manage runtime configuration in the UI. On startup, Vanguarr seeds missing runtime settings into the database from the environment so existing deployments continue to boot cleanly.
7. From the dashboard, run `Profile Architect` once to generate manifests, then run `Decision Engine` to score candidates immediately instead of waiting for the scheduled jobs.

The included [`docker-compose.yml`](docker-compose.yml) mounts `./data` into `/data`, which means profiles, logs, and the SQLite database stay visible on the host.

If you want the Jellyfin-side install flow for per-user playlists, follow [`docs/jellyfin-plugin.md`](docs/jellyfin-plugin.md).

If you want the shortest possible summary for sharing the repo, it is this: Vanguarr is the service that scouts what your media-server users are most likely to want next and quietly files the right requests into the ARR stack.

### Suggested For You Setup

If you want the Jellyfin `Suggested for You` experience, the shortest working path is:

1. Configure Jellyfin in Vanguarr with `JELLYFIN_BASE_URL` and an admin-capable `JELLYFIN_API_KEY`.
2. Set `SUGGESTIONS_API_KEY` and `SEER_WEBHOOK_TOKEN` in Vanguarr, or set them from the `/settings` page after first boot.
3. In Vanguarr, enable `Suggested For You` and choose a `Suggested For You Limit`.
4. In Seerr or Jellyseerr, add a webhook pointing to `http://your-vanguarr-host:8000/api/webhooks/seer` with header `Authorization: Bearer YOUR_SEER_WEBHOOK_TOKEN`.
5. In Jellyfin, add the Vanguarr plugin repository URL `https://raw.githubusercontent.com/sparksbenjamin/Vanguarr/main/jellyfin-plugin/manifest.json`.
6. Install the `Vanguarr` plugin from the Jellyfin plugin catalog and restart Jellyfin.
7. Open the plugin settings in Jellyfin and set the Vanguarr base URL plus the same `SUGGESTIONS_API_KEY`.
8. From the Vanguarr dashboard, run `Profile Architect` once and `Suggested For You` once.

After that, Vanguarr stores per-user suggestion snapshots, Seerr availability webhooks refresh them, and the Jellyfin plugin keeps each user's `Suggested for You` playlist in sync.

The full step-by-step plugin guide is in [`docs/jellyfin-plugin.md`](docs/jellyfin-plugin.md).

### Run The Published GHCR Image

```yaml
services:
  vanguarr:
    image: ghcr.io/sparksbenjamin/vanguarr:latest
    container_name: vanguarr
    restart: unless-stopped
    ports:
      - "8000:8000"
    env_file:
      - .env
    volumes:
      - ./data:/data
```

## Configuration

Vanguarr now uses a hybrid configuration model:

- bootstrap settings stay in the environment because the app needs them before it can reach the database
- runtime settings are stored in the database and edited live from `/settings`
- on startup, Vanguarr seeds any missing runtime rows from environment values so older deployments do not break during the transition

### Bootstrap Settings

These should remain environment-managed:

- `DATABASE_URL`
- `DATA_DIR`
- `PROFILES_DIR`
- `LOGS_DIR`
- `LOG_FILE`
- `APP_HOST`
- `APP_PORT`

Everything else can now live in the database-backed runtime settings store.

### Runtime Setting Seeds

The trimmed [`.env.example`](.env.example) includes the most common first-start seed values. You can still pass the older, larger environment surface if you already have it; Vanguarr continues to read those keys and seed missing database rows from them on startup.

### Core Integrations

| Variable | Required | Notes |
| --- | --- | --- |
| `MEDIA_SERVER_PROVIDER` | Yes | `jellyfin` or `plex` |
| `JELLYFIN_BASE_URL` | If using Jellyfin | Base Jellyfin URL |
| `JELLYFIN_API_KEY` | If using Jellyfin | Needed to list users and read played history |
| `PLEX_BASE_URL` | If using Plex | Base Plex Media Server URL |
| `PLEX_API_TOKEN` | If using Plex | Needed to read playback history and metadata |
| `PLEX_CLIENT_IDENTIFIER` | No | Defaults to `vanguarr` and is sent with Plex requests |
| `SEER_BASE_URL` | Yes | Base URL for a Seer-compatible request service. A trailing `/api/v1` is accepted and normalized away. |
| `SEER_API_KEY` | Yes | Needed for discovery and request creation |
| `SEER_REQUEST_USER_ID` | No | Optional request owner override |
| `SEER_WEBHOOK_TOKEN` | No | Bearer token expected on Seer webhook deliveries to `/api/webhooks/seer` |
| `SUGGESTIONS_API_KEY` | No | Bearer token the Jellyfin `Vanguarr` plugin uses to fetch per-user suggestion snapshots |

### Optional Enrichment

| Variable | Required | Notes |
| --- | --- | --- |
| `TMDB_API_READ_ACCESS_TOKEN` | No | Preferred TMDb auth method |
| `TMDB_API_KEY` | No | Alternative TMDb auth method |
| `TMDB_LANGUAGE` | No | Defaults to `en-US` |
| `TMDB_WATCH_REGION` | No | Defaults to `US` |

If no TMDb credential is set, Vanguarr still runs. It simply skips TMDb enrichment.

### LLM Providers

You can still seed a single legacy provider from the environment on first boot, but the preferred model is now the `/settings` page:

- add one or more providers
- assign a priority to each provider
- enable or disable them independently
- let Vanguarr fail over in order when the active provider is unavailable

Important behavior:

- LLM support is optional. If the LLM is unavailable, Decision Engine falls back to deterministic scoring and Profile Architect skips the profile-side enrichment step gracefully.
- Bare Ollama model names like `glm-4.7-flash:latest` are accepted and normalized to LiteLLM form.
- If a provider timeout is blank, Vanguarr defaults to `180` seconds for Ollama and `45` seconds for hosted providers.
- Startup imports a legacy `LLM_PROVIDER` and `LLM_MODEL` pair into the provider table when no provider rows exist yet.

### Scheduling And Tuning

| Variable | Default | What it controls |
| --- | --- | --- |
| `SCHEDULER_ENABLED` | `true` | Enables the built-in APScheduler jobs |
| `PROFILE_CRON` | `0 3 * * 0` | Weekly Profile Architect run in the configured timezone |
| `DECISION_CRON` | `0 4 * * *` | Daily Decision Engine run in the configured timezone |
| `SUGGESTIONS_ENABLED` | `true` | Enables per-user Suggested For You snapshot generation for Jellyfin |
| `SUGGESTIONS_LIMIT` | `20` | Number of ranked available titles stored per user for the Jellyfin plugin |
| `REQUEST_THRESHOLD` | `0.72` | Minimum hybrid confidence required to request media |
| `PROFILE_HISTORY_LIMIT` | `40` | Number of playback history items used per user |
| `CANDIDATE_LIMIT` | `160` | Maximum pre-rank candidate pool size |
| `TRENDING_CANDIDATE_LIMIT` | `100` | Max trending titles mixed into the pool |
| `DECISION_SHORTLIST_LIMIT` | `15` | Diversified shortlist size before LLM evaluation |
| `RECOMMENDATION_SEED_LIMIT` | `6` | Max watch-history seeds per user |
| `TMDB_SEED_ENRICHMENT_LIMIT` | `6` | Max watched seeds enriched for profile signals |
| `TMDB_CANDIDATE_ENRICHMENT_LIMIT` | `30` | Max ranked candidates enriched before final rerank |
| `GLOBAL_EXCLUSIONS` | `No Horror,No Reality TV` | Global guardrails applied to every decision |

Useful extra knobs from [`.env.example`](.env.example):

- `PROFILE_LLM_ENRICHMENT_ENABLED` disables the profile-side adjacent-lane suggestion step if you want a fully deterministic profile build.
- `PROFILE_LLM_ENRICHMENT_MAX_OUTPUT_TOKENS` keeps that optional profile-side LLM assist intentionally small.
- `SCHEDULER_ENABLED=false` is the right setting when you want dashboard-driven manual runs only.

## How Profiles Work

Each user profile is stored as two files under `./data/profiles`:

- `username.json`: the canonical structured manifest
- `username.txt`: the derived human-readable summary block

The JSON manifest is the source of truth. The summary block is regenerated from it whenever you save the profile in the manifest editor.

### Fields Vanguarr Derives Automatically

- `top_titles`
- `repeat_titles`
- `recent_momentum`
- `top_genres`
- `ranked_genres`
- `primary_genres`
- `secondary_genres`
- `recent_genres`
- `format_preference`
- `release_year_preference`
- `discovery_lanes`
- `top_keywords`
- `favorite_people`
- `preferred_brands`
- `favorite_collections`

### Fields Operators Can Safely Tune

- `adjacent_genres`
- `adjacent_themes`
- `explicit_feedback`
- `profile_exclusions`
- `operator_notes`

Example manifest shape:

```json
{
  "profile_version": "v5",
  "profile_state": "ready",
  "username": "alice",
  "primary_genres": ["Sci-Fi", "Thriller"],
  "adjacent_genres": ["Adventure"],
  "adjacent_themes": ["found family"],
  "explicit_feedback": {
    "liked_titles": [],
    "disliked_titles": [],
    "liked_genres": [],
    "disliked_genres": []
  },
  "profile_exclusions": ["No Horror"],
  "operator_notes": "Prefer high-conviction sci-fi with strong source affinity."
}
```

## Runtime Data

Mount `./data` to `/data` if you want direct access to all runtime artifacts.

| Path | Purpose |
| --- | --- |
| `./data/vanguarr.db` | SQLite database for decision history, requests, and task runs |
| `./data/profiles/*.json` | Canonical user manifests |
| `./data/profiles/*.txt` | Derived summary blocks |
| `./data/logs/vanguarr.log` | Application log file |

This layout is what powers the dashboard, War Room, and manifest editor.

## Deployment Notes

### Media Servers

#### Jellyfin

- Vanguarr reads watched history through Jellyfin's normal item APIs.
- It uses played-state filters and `DatePlayed` sorting on `/Items`.
- The Jellyfin Playback Reporting plugin is not required.
- The companion `Vanguarr` Jellyfin plugin syncs per-user `Suggested for You` playlists from Vanguarr's stored suggestion snapshots.
- Plugin install and webhook setup instructions live in [`docs/jellyfin-plugin.md`](docs/jellyfin-plugin.md).

#### Plex

- Vanguarr reads watch history through Plex's playback history API.
- It normalizes Plex metadata into the same internal history shape used by the scoring pipeline.
- Plex requires a valid `X-Plex-Token`, and Vanguarr sends a stable `X-Plex-Client-Identifier` with requests.

### Docker, Unraid, And OpenShift-Style Platforms

- Mutable runtime data lives under `/data`, not `/app`.
- The container is prepared for arbitrary non-root UIDs that are members of group `0`.
- For Unraid or host-based Ollama, you can add:

```yaml
extra_hosts:
  - "host.docker.internal:host-gateway"
```

and point `OLLAMA_API_BASE` at `http://host.docker.internal:11434`.

- Outside Docker-based runtimes, do not rely on `host.docker.internal`; use real service DNS or routes instead.

### Scaling

- With SQLite and the in-process scheduler, run a single replica.
- If you need multiple web replicas, disable the built-in scheduler in the web pods and move scheduling and persistence to external services.
- Use `/healthz` for container health probes.

## Local Development

The repo is small enough to run locally without Docker if you prefer.

### Setup

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

On Windows PowerShell, activate the venv with:

```powershell
.venv\Scripts\Activate.ps1
```

### Tests

```bash
python -m pytest
```

## Container Images And CI

The repo ships with multi-arch Docker build automation:

- published image: `ghcr.io/sparksbenjamin/vanguarr`
- target platforms: `linux/amd64`, `linux/arm64`
- workflow: [`.github/workflows/docker.yml`](.github/workflows/docker.yml)
- bake definition: [`docker-bake.hcl`](docker-bake.hcl)

If you want to publish to another registry, override `REGISTRY_IMAGE` in [`docker-bake.hcl`](docker-bake.hcl) or adjust the GitHub Actions metadata step.

## Design Principles

Vanguarr is intentionally opinionated about where intelligence should live:

- Watch history and profile state are durable, inspectable, and stored locally.
- Candidate ranking is deterministic first and explainable after the fact.
- TMDb adds evidence, not authority.
- The LLM is a scoped assistant, not the primary ranking engine.
- Every request decision leaves a paper trail.

That makes it much easier to understand why the system requested something, why it ignored something, and what to tune next. Vanguarr should feel like a scout reporting back to the ARR stack, not an oracle making opaque calls.

## License

This project is licensed under the GNU GPL v3. See [`LICENSE`](LICENSE).
