# Changelog

All notable changes to Vanguarr are tracked here.

## [0.2.2] - 2026-04-14

### Highlights

* Reworked profile building into a layered pipeline that starts with personal playback history, then adds Seer recommendation-neighborhood hints, local similar-user lift, TMDb metadata enrichment, and finally LLM synthesis for tightly-bounded adjacent lanes.
* Changed profile weighting to rely on grouped titles with bounded repeat influence so one long binge is less likely to distort the durable taste model.
* Carried the richer profile signals through discovery seeds, candidate scoring, and profile summaries so request decisions and manifests reflect the same evidence stack.

## [0.2.1] - 2026-04-13

### Highlights

* Fixed profile task status matching so scheduler-driven `all users` runs now show up on the relevant profile pages instead of looking like they never ran.
* Added explicit per-profile last-run timestamps on the manifest action cards for Profile Architect, Decision Engine, and Suggested For You.
* Added the local app version to the UI so operators can see exactly which Vanguarr build is running without making any outbound version checks.

## [0.2.0] - 2026-04-10

### Highlights

* Introduced a native Jellyfin companion plugin that surfaces personalized `Suggested Movies` and `Suggested Shows` views backed by real Jellyfin library items.
* Added Jellyfin library indexing and sync inside Vanguarr so suggestions are built from what is actually available to watch.
* Reworked `Suggested For You` to use deterministic scoring first, then blended AI evaluation on a bounded shortlist for better quality and lower token burn.
* Added suggestion-specific tuning controls for AI thresholds, recent-watch cooldowns, repeat-watch filtering, and candidate caps.
* Brought suggestion activity into the War Room with live updates, paging, sorting, and request-vs-suggestion filters.
* Added no-op library sync detection and suggestion AI vote reuse so unchanged libraries do not keep reprocessing the same titles.
* Refreshed the main docs and Jellyfin plugin docs to better explain the product story, quick start, and setup path.

### Notes

* The Jellyfin plugin package line continues on its own version track for compatibility with Jellyfin updates.

## [0.1.1] - 2026-04-08

* Maintenance and documentation improvements.

## [0.1.0] - 2026-04-08

* Initial public release.
