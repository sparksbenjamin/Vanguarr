# Configuration Reference

This is the technical companion to the main [README](../README.md). If you want the pitch, start there. If you want the knobs, schedules, and deployment details, this is the page.

## Bootstrap vs Runtime Settings

Vanguarr uses a hybrid configuration model:

* Bootstrap settings stay in the environment because the app needs them before it can reach the database.
* Runtime settings are stored in the database and edited live from `/settings`.
* On startup, Vanguarr seeds missing runtime rows from environment values so older deployments keep booting cleanly.

## Bootstrap Settings

These should remain environment-managed:

* `DATABASE_URL`
* `DATA_DIR`
* `PROFILES_DIR`
* `LOGS_DIR`
* `LOG_FILE`
* `APP_HOST`
* `APP_PORT`

Everything else can live in the runtime settings store after first boot.

## Core Integrations

| Variable | Required | Notes |
| --- | --- | --- |
| `MEDIA_SERVER_PROVIDER` | Yes | `jellyfin` or `plex` |
| `JELLYFIN_BASE_URL` | If using Jellyfin | Base Jellyfin URL |
| `JELLYFIN_API_KEY` | If using Jellyfin | Used to list users, read history, and handle plugin install flows |
| `PLEX_BASE_URL` | If using Plex | Base Plex Media Server URL |
| `PLEX_API_TOKEN` | If using Plex | Used to read playback history and metadata |
| `PLEX_CLIENT_IDENTIFIER` | No | Defaults to `vanguarr` |
| `SEER_BASE_URL` | Yes | Base URL for Jellyseerr, Overseerr, or another Seer-compatible service |
| `SEER_API_KEY` | Yes | Used for discovery and request creation |
| `SEER_REQUEST_USER_ID` | No | Optional request owner override |
| `SEER_WEBHOOK_TOKEN` | No | Bearer token for `/api/webhooks/seer` |
| `SUGGESTIONS_API_KEY` | No | Bearer token used by the Jellyfin `Vanguarr` plugin |

## Optional Enrichment

| Variable | Required | Notes |
| --- | --- | --- |
| `TMDB_API_READ_ACCESS_TOKEN` | No | Preferred TMDb auth method |
| `TMDB_API_KEY` | No | Alternative TMDb auth method |
| `TMDB_LANGUAGE` | No | Defaults to `en-US` |
| `TMDB_WATCH_REGION` | No | Defaults to `US` |

If no TMDb credential is configured, Vanguarr still runs. It just skips TMDb enrichment.

## LLM Providers

The preferred model is now the runtime Settings page:

* Add one or more providers.
* Assign a priority to each provider.
* Enable or disable them independently.
* Let Vanguarr fail over in order when the active provider is unavailable.

Important behavior:

* LLM support is optional.
* If the LLM is unavailable, Decision Engine falls back to deterministic scoring.
* Profile Architect can skip profile-side enrichment gracefully.
* Bare Ollama model names like `glm-4.7-flash:latest` are accepted.
* Blank provider timeouts default to `180` seconds for Ollama and `45` seconds for hosted providers.

## Scheduling And Tuning

| Variable | Default | What it controls |
| --- | --- | --- |
| `SCHEDULER_ENABLED` | `true` | Enables APScheduler jobs |
| `PROFILE_CRON` | `0 3 * * 0` | Weekly Profile Architect run |
| `DECISION_CRON` | `0 4 * * *` | Daily Decision Engine run |
| `LIBRARY_SYNC_ENABLED` | `true` | Enables Jellyfin library indexing |
| `LIBRARY_SYNC_CRON` | `0 */4 * * *` | Jellyfin library sync schedule |
| `SUGGESTIONS_ENABLED` | `true` | Enables per-user suggestion snapshots |
| `SUGGESTIONS_LIMIT` | `20` | Number of ranked available titles stored per user |
| `SUGGESTION_AI_THRESHOLD` | `0.58` | Minimum score required for a title to appear in Suggested For You, and the gate for AI re-ranking |
| `SUGGESTION_AI_CANDIDATE_LIMIT` | `24` | Max available titles per user that can be LLM-scored for Suggested For You |
| `SUGGESTION_RECENT_COOLDOWN_DAYS` | `14` | Days to suppress recently watched titles from Suggested For You |
| `SUGGESTION_REPEAT_WATCH_CUTOFF` | `3` | Watch count at which a title is treated as rewatch territory instead of discovery |
| `REQUEST_THRESHOLD` | `0.72` | Minimum hybrid confidence required to request |
| `PROFILE_HISTORY_LIMIT` | `40` | Watch-history items considered per user |
| `CANDIDATE_LIMIT` | `160` | Pre-rank candidate pool limit |
| `TRENDING_CANDIDATE_LIMIT` | `100` | Max trending titles mixed into the pool |
| `DECISION_SHORTLIST_LIMIT` | `15` | Diversified shortlist size before LLM review |
| `RECOMMENDATION_SEED_LIMIT` | `6` | Watch-history seeds per user |
| `TMDB_SEED_ENRICHMENT_LIMIT` | `6` | Max watched seeds enriched for profile signals |
| `TMDB_CANDIDATE_ENRICHMENT_LIMIT` | `30` | Max ranked candidates enriched before final rerank |
| `GLOBAL_EXCLUSIONS` | `No Horror,No Reality TV` | Global guardrails applied to every decision |

Useful extra knobs from [`.env.example`](../.env.example):

* `PROFILE_LLM_ENRICHMENT_ENABLED`
* `PROFILE_LLM_ENRICHMENT_MAX_OUTPUT_TOKENS`
* `SCHEDULER_ENABLED=false` for manual-only runs
* `LIBRARY_SYNC_ENABLED=true` plus a sane `LIBRARY_SYNC_CRON` to keep Jellyfin suggestions fresh

## Runtime Data Layout

Mount `./data` to `/data` if you want direct access to runtime artifacts.

| Path | Purpose |
| --- | --- |
| `./data/vanguarr.db` | SQLite database for runs, decisions, and runtime settings |
| `./data/profiles/*.json` | Canonical user manifests |
| `./data/profiles/*.txt` | Derived profile summaries |
| `./data/logs/vanguarr.log` | Application log file |

## Web Interface

Important operator routes:

| Route | Purpose |
| --- | --- |
| `/` | Dashboard with health, manual triggers, and profile cards |
| `/logs` | War Room decision log |
| `/settings` | Runtime settings editor |
| `/manifest` | Profile manifest editor |
| `/healthz` | Health probe |

## Deployment Notes

### Docker / Unraid / OpenShift-Style Platforms

* Mutable runtime data lives under `/data`, not `/app`.
* The container supports arbitrary non-root UIDs in group `0`.
* For host-based Ollama on Docker, you can use:

```yaml
extra_hosts:
  - "host.docker.internal:host-gateway"
```

Then point `OLLAMA_API_BASE` to `http://host.docker.internal:11434`.

### Scaling

* With SQLite and the in-process scheduler, run a single replica.
* If you need multiple web replicas, disable the built-in scheduler in web pods and move scheduling/persistence outward.
* Use `/healthz` for container health probes.

## Related Docs

* [Jellyfin Plugin Setup](jellyfin-plugin.md)
* [How Vanguarr Works](how-it-works.md)
