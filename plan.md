# web-crawler plan

## Current milestone: fetch admission and bounded body reads

The current priority is to keep crawler workers from being captured by non-page resources.
The crawler is an HTML-centered WWW discovery system, not an unbounded downloader.

## Completed in this slice

- Added fetch admission before response body reads.
- Treat binary, media, archive, font, image, and oversized responses as metadata-only.
- Bound body reads by byte count and elapsed time.
- Kept metadata-only resources as successful scheduler completions.
- Made heavy scheduler diagnostics degrade instead of returning `/stats/diagnostics` 500.
- Disabled global scheduled-surface delay maintenance by default because it scans the production
  scheduled surface before cycles can start.
- Documented fetch admission in the content policy and crawler concepts.

## Acceptance

- `audio/mpeg` and other media streams do not read response bodies.
- Metadata-only resources are marked done and do not extract links.
- One streaming URL cannot hold a worker indefinitely.
- `/stats/diagnostics` remains available even when heavy scheduler diagnostics time out.
- Daemon startup does not run global scheduled-surface rank maintenance by default.
- Related tests and lint pass before deploy.

## Next checks after deploy

- Confirm `/health`, `/stats`, and `/stats/diagnostics` are healthy.
- Confirm expired `active_leases` do not accumulate.
- Confirm media-stream URLs complete quickly as metadata-only.
- Compare cycle completion time and pages/sec against the stuck `299/300` production cycle.
