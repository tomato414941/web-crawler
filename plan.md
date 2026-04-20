# web-crawler plan

## Current milestone: split precheck bottleneck timing

Cycle-local timing summaries are now deployed and show that the current crawl-cycle body cost is
mostly `fetch` and `precheck`, not lease selection. The next priority is to split `precheck` into
robots admission and host rate-limit reservation so the next speed fix can target the right subsystem.

## Completed

- Removed `url_ledger.stats()` from the daemon cycle-complete log path.
- Stopped the daemon from running a second `url_ledger.readiness()` / scheduler snapshot after each
  crawl cycle.
- Made daemon runtime scheduler views use the already-read readiness object instead of live full
  queue diagnostics.
- Stopped pre-cycle blocked-retry promotion from calling `blocked_reason_counts()` and full durable
  scheduler snapshots.
- Changed daemon cycle-start readiness to use the lightweight `host_runnable_heads` read model
  instead of the live full scheduler readiness query.
- Changed `/stats/diagnostics` to return runtime-snapshot-only degraded diagnostics.
- Added tests that fail if cycle completion calls live scheduler stats or snapshots.
- Added cycle-local timing summaries and compact cycle-complete p95 timing logs.
- Confirmed production exposes `runtime.payload.timing_summary` through `/stats`.

## Current slice

- Add a configurable `robots_fetch_timeout` with a lower default than page fetch timeout.
- Use that timeout for robots.txt fetches.
- Keep timeout failures on the current allow-unavailable path.

## Acceptance

- Robots fetch timeout defaults to 3 seconds.
- `CRAWLER_ROBOTS_FETCH_TIMEOUT` can override the timeout.
- Related tests and lint pass before deploy.

## Next checks after deploy

- Confirm `/health`, `/stats`, and `/stats/diagnostics` are healthy.
- Observe at least two production cycles and compare `robots_p95`, `precheck_p95`, and pages/s with
  the previous baseline.
- If `robots_p95` remains high, consider persisted robots bodies or unavailable/error cache policy.
- If `fetch_request_ms` remains the largest stage, move next to slow-host deprioritization or page
  fetch timeout tuning.
