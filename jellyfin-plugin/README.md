# 🍿 Vanguarr for Jellyfin
### Personalized recommendations that still feel native.

The Jellyfin plugin name is `Vanguarr`.

This is the piece that brings the Vanguarr experience into Jellyfin.

Instead of sending users off to a sidecar app, or forcing you into duplicate libraries and symlink tricks, the Vanguarr plugin surfaces personalized recommendations directly inside Jellyfin as native views backed by real library items.

---

## ✨ Why This Plugin Exists

Vanguarr already knows what your users actually like.

The plugin is what turns that intelligence into something browseable.

* **🎬 Native Jellyfin Views:** Surfaces `Suggested Movies` and `Suggested Shows` inside Jellyfin.
* **👤 Personalized Per User:** Each signed-in Jellyfin user gets their own ranked results.
* **▶️ Real Playback:** Suggestions resolve back to real Jellyfin items, so details pages and playback stay native.
* **🧼 No Library Hacks:** No symlink forests. No duplicate per-user libraries. No metadata mess.
* **🧠 Powered By Vanguarr:** Library sync, scoring, profiles, and suggestion logic all stay in Vanguarr where they belong.

## 🧭 What It Feels Like

When it is working the way it is meant to, the experience is simple:

* Open Jellyfin
* Browse `Suggested Movies`
* Browse `Suggested Shows`
* Click into a title
* Play it like anything else already in your library

That is the whole point. It should feel like Jellyfin got smarter, not like you bolted on another app.

## 🚀 Quick Start

If you want the shortest path to seeing suggestions in Jellyfin:

1. Get Vanguarr running and reachable from Jellyfin.
2. Install the `Vanguarr` Jellyfin plugin.
3. Point the plugin back to your Vanguarr server.
4. Run `Library Sync Now` in Vanguarr.
5. Run `Profile Architect`.
6. Open Jellyfin and browse the suggested views.

For the actual install and setup steps, use the guide here:

* [Jellyfin Plugin Setup](../docs/jellyfin-plugin.md)

## 🧩 What You Get

The plugin currently gives Jellyfin two native recommendation surfaces:

* `Suggested Movies`
* `Suggested Shows`

Those views are driven by:

* Vanguarr user profiles
* Vanguarr suggestion scoring
* Jellyfin library indexing from Vanguarr
* real Jellyfin library items as the final resolved targets

## 🛡️ Why This Approach Works

There are a lot of ugly ways to fake personalized recommendations in Jellyfin.

This plugin avoids most of them.

* It does not create duplicate libraries.
* It does not depend on per-user symlink trees.
* It does not replace Jellyfin's playback flow.
* It does not force users into a separate interface just to get recommendations.

Instead, it uses Vanguarr as the recommendation brain and Jellyfin as the viewing surface.

## 📚 Documentation

Use this page for the overview. Use the docs below for the technical path.

* [Plugin Setup Guide](../docs/jellyfin-plugin.md)
* [Main Project README](../README.md)
* [Configuration Reference](../docs/configuration.md)
* [How Vanguarr Works](../docs/how-it-works.md)

<details>
<summary><strong>Need the full install path?</strong></summary>

The detailed install flow, plugin configuration steps, webhook notes, and troubleshooting live in:

* [Jellyfin Plugin Setup](../docs/jellyfin-plugin.md)

</details>

<details>
<summary><strong>Testing or packaging the plugin yourself?</strong></summary>

The plugin repo metadata and package files live under:

* [`manifest.json`](manifest.json)
* [`dist/`](dist)
* [`Vanguarr/`](Vanguarr)

To rebuild the package locally:

```powershell
powershell -ExecutionPolicy Bypass -File .\jellyfin-plugin\package.ps1
```

</details>

<details>
<summary><strong>What the plugin does not do</strong></summary>

To keep expectations clean:

* It does not create a whole second media server experience.
* It does not replace Vanguarr itself.
* It does not turn Jellyfin into Netflix overnight.

What it does do is make Vanguarr's recommendation engine feel like a real part of Jellyfin.

</details>
