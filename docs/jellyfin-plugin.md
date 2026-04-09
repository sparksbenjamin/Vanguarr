# Jellyfin Plugin Setup

The Jellyfin plugin name is `Vanguarr`.

It does one job: it asks Vanguarr for the current user's ranked `Suggested for You` items, resolves those suggestions to real Jellyfin library items, and keeps a per-user playlist in sync. It does not create extra Jellyfin libraries, symlink trees, or duplicate metadata entries.

## What You Need

- Jellyfin `10.11.x`
- A reachable Vanguarr instance
- Jellyfin users already present in Vanguarr's source media server
- A `Suggestions API Key` configured in Vanguarr
- A `Seer Webhook Token` configured in Vanguarr if you want availability-driven refreshes

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

The repository URL above assumes these files are pushed to GitHub. If you are testing from a local checkout first, host [`jellyfin-plugin/manifest.json`](../jellyfin-plugin/manifest.json) and [`jellyfin-plugin/dist/vanguarr-1.0.0.0.zip`](../jellyfin-plugin/dist/vanguarr-1.0.0.0.zip) somewhere Jellyfin can reach over HTTP, or update the URLs to match your own Git hosting.

If you want to inspect or sideload the package manually, the plugin zip is published in the repo at:

```text
https://raw.githubusercontent.com/sparksbenjamin/Vanguarr/main/jellyfin-plugin/dist/vanguarr-1.0.0.0.zip
```

## Configure Vanguarr

In the Vanguarr web UI, open `/settings` and set these runtime values:

- `Suggestions API Key`: bearer token used by the Jellyfin plugin
- `Seer Webhook Token`: bearer token expected from Seerr or Jellyseerr
- `Suggested For You Enabled`: turn on per-user suggestion snapshots
- `Suggested For You Limit`: number of titles stored per user

You can also seed the same values through environment variables:

- `SUGGESTIONS_API_KEY`
- `SEER_WEBHOOK_TOKEN`
- `SUGGESTIONS_ENABLED`
- `SUGGESTIONS_LIMIT`

## Configure The Jellyfin Plugin

After Jellyfin restarts:

1. Open `Dashboard` -> `Plugins` -> `My Plugins` -> `Vanguarr`.
2. Set `Vanguarr Base URL`.
3. Set `Suggestions API Key` to the same token you stored in Vanguarr.
4. Choose a `Sync Interval`.
5. Choose a `Suggestion Limit`.
6. Keep the playlist name as `Suggested for You` or rename it if you want a different label in Jellyfin.
7. Save the plugin settings.

The plugin sync runs in the background and refreshes a playlist for each Jellyfin user. The playlist is user-specific even if every account sees the same plugin configuration page.

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

When Seerr sends an availability event, Vanguarr stores the delivery, refreshes the affected user's suggestion snapshot, and the Jellyfin plugin picks the changes up on its next sync cycle.

## First Run

1. Run `Profile Architect` once from the Vanguarr dashboard.
2. Run `Suggested For You` once from the same dashboard.
3. Open Jellyfin and confirm the per-user `Suggested for You` playlist appears.

After that:

- Vanguarr refreshes suggestion snapshots when Seerr availability webhooks arrive.
- Jellyfin refreshes the playlist on the plugin sync interval.
- If you want a full rebuild after profile changes, run `Suggested For You` again from the dashboard.

## Packaging Notes

The plugin source lives in [`jellyfin-plugin/Vanguarr`](../jellyfin-plugin/Vanguarr), and the repository manifest Jellyfin reads lives at [`jellyfin-plugin/manifest.json`](../jellyfin-plugin/manifest.json).

To rebuild the packaged zip locally, run:

```powershell
powershell -ExecutionPolicy Bypass -File .\jellyfin-plugin\package.ps1
```
