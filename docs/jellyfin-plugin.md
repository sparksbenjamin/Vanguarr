# Jellyfin Plugin Setup

The Jellyfin plugin name is `Vanguarr`.

This is the piece that brings Vanguarr into the Jellyfin experience.

Instead of making you manage duplicate libraries, symlink trees, or weird per-user folders, the plugin exposes two native Jellyfin views:

* `Suggested Movies`
* `Suggested Shows`

Those views are personalized per signed-in Jellyfin user, but they resolve to real library items. That means the experience still feels like Jellyfin, not a bolt-on sidecar UI.

## What You Get

Once the plugin is installed and Vanguarr is configured:

* Jellyfin users see native recommendation views inside Jellyfin
* Suggested items open into normal Jellyfin detail pages
* Playback stays fully native
* Vanguarr keeps scoring in the background while Jellyfin stays the playback source of truth

## Fast Path

If you want the shortest route from "nothing installed" to "I can browse suggestions in Jellyfin," do this:

1. Get Vanguarr running and reachable from Jellyfin.
2. Configure a `Suggestions API Key` in Vanguarr.
3. From `Vanguarr -> Settings -> Integrations`, use `Install Jellyfin Vanguarr Plugin` if your stored Jellyfin API key has admin access.
4. Restart Jellyfin.
5. Open `Dashboard -> Plugins -> My Plugins -> Vanguarr`.
6. Set the Vanguarr base URL and the same `Suggestions API Key`.
7. In Vanguarr, run `Library Sync Now`.
8. Run `Profile Architect` once if you want an immediate profile refresh.

After that, look for `Suggested Movies` and `Suggested Shows` in Jellyfin.

## Manual Install

If you prefer to add the plugin repo directly in Jellyfin:

1. Open the Jellyfin admin dashboard.
2. Go to `Plugins -> Catalog -> Settings`.
3. Add this repository URL:

```text
https://raw.githubusercontent.com/sparksbenjamin/Vanguarr/main/jellyfin-plugin/manifest.json
```

4. Save.
5. Refresh the plugin catalog.
6. Search for `Vanguarr`.
7. Install the plugin.
8. Restart Jellyfin.

## Configure The Plugin

After Jellyfin restarts:

1. Open `Dashboard -> Plugins -> My Plugins -> Vanguarr`.
2. Set `Vanguarr Base URL`.
3. Set `Suggestions API Key`.
4. Choose a `Refresh Interval`.
5. Choose a `Suggestion Limit`.
6. Leave the default view names as `Suggested Movies` and `Suggested Shows`, or rename them if you want.
7. Save.

What those settings do:

* `Vanguarr Base URL` tells Jellyfin where to fetch suggestions
* `Suggestions API Key` authorizes the plugin against Vanguarr
* `Refresh Interval` controls how long Jellyfin caches resolved suggestions
* `Suggestion Limit` caps how many items Jellyfin resolves per user

## First Run

The plugin needs Vanguarr to have actual indexed library data and user profiles to work with.

In Vanguarr:

1. Open `Settings -> Scheduling`.
2. Run `Library Sync Now`.
3. From the dashboard, run `Profile Architect`.
4. If you want to inspect what Vanguarr is about to send, use the profile preview on the Profiles page.

Then go back to Jellyfin and open the suggested views.

## What The Plugin Does Not Do

The plugin is meant to feel native, but there are still a few boundaries worth knowing:

* It does not create duplicate libraries.
* It does not build symlink trees.
* It does not replace Jellyfin's playback pipeline.
* It does not invent a custom Netflix-style homepage row in stock Jellyfin.

What it does do is give you two native Jellyfin views that behave like focused recommendation shelves backed by real Jellyfin items.

<details>
<summary><strong>Need the Vanguarr-side settings list?</strong></summary>

Set these in Vanguarr if you are using the plugin:

* `SUGGESTIONS_API_KEY`
* `SUGGESTIONS_ENABLED`
* `SUGGESTIONS_LIMIT`
* `SUGGESTION_AI_THRESHOLD`
* `SUGGESTION_AI_CANDIDATE_LIMIT`
* `SUGGESTION_RECENT_COOLDOWN_DAYS`
* `SUGGESTION_REPEAT_WATCH_CUTOFF`
* `LIBRARY_SYNC_ENABLED`
* `LIBRARY_SYNC_CRON`

Optional but useful:

* `SEER_WEBHOOK_TOKEN`

</details>

<details>
<summary><strong>Using Seerr, Jellyseerr, or Overseerr webhooks too?</strong></summary>

You can optionally add a Seer-compatible webhook to Vanguarr:

1. Create a webhook notification agent in Seerr, Jellyseerr, or Overseerr.
2. Point it to:

```text
http://your-vanguarr-host:8000/api/webhooks/seer
```

3. Add this header:

```text
Authorization: Bearer YOUR_SEER_WEBHOOK_TOKEN
```

Those webhooks act as nudges for refresh behavior, but Jellyfin's indexed library remains the source of truth for what is actually available to suggest.

</details>

<details>
<summary><strong>Need the local package or rebuild path?</strong></summary>

If you are testing from a local checkout, Jellyfin can also read a hosted copy of:

* [`jellyfin-plugin/manifest.json`](../jellyfin-plugin/manifest.json)
* [`jellyfin-plugin/dist/vanguarr-1.2.5.0.zip`](../jellyfin-plugin/dist/vanguarr-1.2.5.0.zip)

To rebuild the plugin zip locally:

```powershell
powershell -ExecutionPolicy Bypass -File .\jellyfin-plugin\package.ps1
```

</details>

<details>
<summary><strong>Troubleshooting</strong></summary>

If the suggested views show up but look empty:

* Run `Library Sync Now` in Vanguarr
* Run `Profile Architect`
* Hard refresh Jellyfin with `Ctrl+F5`
* Sign out and back in if the web UI is caching the old tiles

If the plugin does not install from Vanguarr:

* make sure the stored `JELLYFIN_API_KEY` belongs to a Jellyfin admin user
* confirm Jellyfin can reach the plugin manifest URL

If the views exist but suggestions look wrong:

* use the Vanguarr profile preview to confirm what Vanguarr thinks that user should see
* rerun `Library Sync Now` so the indexed Jellyfin catalog catches up with adds/removals

</details>

## Related Docs

* [Plugin Overview](../jellyfin-plugin/README.md)
* [Main README](../README.md)
* [Configuration Reference](configuration.md)
* [How Vanguarr Works](how-it-works.md)
