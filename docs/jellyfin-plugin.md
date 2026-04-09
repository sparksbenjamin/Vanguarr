# Jellyfin Plugin Setup

The Jellyfin plugin name is `Vanguarr`.

It does one job: it asks Vanguarr for the current user's ranked suggestions, resolves those suggestions to real Jellyfin library items, and exposes them as two native Jellyfin views: one for movies and one for shows. It does not create extra Jellyfin libraries, symlink trees, or duplicate metadata entries.

Important limitation: stock Jellyfin does not provide a native plugin hook for inventing a brand-new personalized home row in the default client. The clean native surface is a pair of user-specific browseable views that open and play through Jellyfin normally.

## What You Need

- Jellyfin `10.11.x`
- A reachable Vanguarr instance
- Jellyfin users already present in Vanguarr's source media server
- A `Suggestions API Key` configured in Vanguarr
- A `Seer Webhook Token` configured in Vanguarr if you want optional availability-driven nudges

## Add The Vanguarr Plugin Repo To Jellyfin

If your saved `JELLYFIN_API_KEY` in Vanguarr has elevated admin access, you can start this from `Vanguarr -> Settings -> Integrations -> Install Jellyfin Vanguarr Plugin`. That action adds the repository to Jellyfin and requests installation of the plugin from that repository.

If you prefer to do it directly in Jellyfin, use the manual steps below.

1. Open the Jellyfin admin dashboard.
2. Go to `Plugins` -> `Catalog` -> `Settings`.
3. Add this repository URL:

```text
https://raw.githubusercontent.com/sparksbenjamin/Vanguarr/main/jellyfin-plugin/manifest.json
```

4. Save the catalog settings.
5. Refresh the plugin catalog.
6. Search for `Vanguarr`.
7. Install the `Vanguarr` plugin.
8. Restart Jellyfin after the install finishes.

The repository URL above assumes these files are pushed to GitHub. If you are testing from a local checkout first, host [`jellyfin-plugin/manifest.json`](../jellyfin-plugin/manifest.json) and [`jellyfin-plugin/dist/vanguarr-1.2.0.0.zip`](../jellyfin-plugin/dist/vanguarr-1.2.0.0.zip) somewhere Jellyfin can reach over HTTP, or update the URLs to match your own Git hosting.

If you want to inspect or sideload the package manually, the plugin zip is published in the repo at:

```text
https://raw.githubusercontent.com/sparksbenjamin/Vanguarr/main/jellyfin-plugin/dist/vanguarr-1.2.0.0.zip
```

## Configure Vanguarr

In the Vanguarr web UI, open `/settings` and set these runtime values:

- `Suggestions API Key`: bearer token used by the Jellyfin plugin
- `Seer Webhook Token`: bearer token expected from Seerr or Jellyseerr
- `Suggested For You Enabled`: turn on per-user suggestion snapshots
- `Suggested For You Limit`: number of titles stored per user
- `Library Sync Enabled`: turns on the indexed Jellyfin catalog refresh job
- `Library Sync Cron`: controls how often Vanguarr rebuilds the indexed Jellyfin catalog

You can also seed the same values through environment variables:

- `SUGGESTIONS_API_KEY`
- `SEER_WEBHOOK_TOKEN`
- `SUGGESTIONS_ENABLED`
- `SUGGESTIONS_LIMIT`
- `LIBRARY_SYNC_ENABLED`
- `LIBRARY_SYNC_CRON`

## Configure The Jellyfin Plugin

After Jellyfin restarts:

1. Open `Dashboard` -> `Plugins` -> `My Plugins` -> `Vanguarr`.
2. Set `Vanguarr Base URL`.
3. Set `Suggestions API Key` to the same token you stored in Vanguarr.
4. Choose a `Refresh Interval`.
5. Choose a `Suggestion Limit`.
6. Keep the suggested view names as `Suggested Movies` and `Suggested Shows`, or rename them if you want different Jellyfin labels.
7. Save the plugin settings.

The plugin registers two native Jellyfin views, and each one is personalized for the currently signed-in Jellyfin user. The configuration page is shared because the plugin settings are server-wide.

That means:

- `Suggested Movies` shows ranked movie suggestions for the active Jellyfin user
- `Suggested Shows` shows ranked series suggestions for the active Jellyfin user
- the items are real Jellyfin library items, so details pages and playback stay native
- if you rename either view in plugin settings, restart Jellyfin once so the new names are registered cleanly

## Configure The Seerr Webhook

In Seerr, Jellyseerr, or another Seer-compatible request service:

1. Add a `Webhook` notification agent.
2. Set the target URL to your Vanguarr server:

```text
http://your-vanguarr-host:8000/api/webhooks/seer
```

3. Add this header:

```text
Authorization: Bearer YOUR_SEER_WEBHOOK_TOKEN
```

4. Enable the availability-focused webhook events you care about, especially the events that fire when requested media becomes available.

When Seerr sends an availability event, Vanguarr stores the delivery and can nudge suggestion refreshes, but the primary source of truth for what can actually be suggested is the indexed Jellyfin library.

## First Run

1. In Vanguarr, open `Settings` -> `Scheduling` and run `Library Sync Now` once.
2. Run `Profile Architect` once from the Vanguarr dashboard if you want to force a fresh profile build immediately.
3. Open Jellyfin and confirm the per-user `Suggested Movies` and `Suggested Shows` views appear under Jellyfin's library/media surfaces for that user.

After that:

- Vanguarr refreshes the indexed Jellyfin catalog on the configured `Library Sync Cron`.
- Vanguarr rebuilds suggestion snapshots after each successful library sync and after profile refreshes.
- Seerr availability webhooks are optional and act as nudges instead of the primary library source.
- Jellyfin refreshes the resolved suggestion cache on the configured refresh interval.
- If you want a full rebuild outside the schedule, run `Library Sync Now` or `Suggested For You` manually.

## Packaging Notes

The plugin source lives in [`jellyfin-plugin/Vanguarr`](../jellyfin-plugin/Vanguarr), and the repository manifest Jellyfin reads lives at [`jellyfin-plugin/manifest.json`](../jellyfin-plugin/manifest.json).

To rebuild the packaged zip locally, run:

```powershell
powershell -ExecutionPolicy Bypass -File .\jellyfin-plugin\package.ps1
```
