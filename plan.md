# web-crawler plan

## Current milestone: measure crawl-cycle body bottlenecks

The cycle-boundary scheduler scan problem is largely resolved: production now starts the next cycle
about 9 seconds after cycle completion instead of 60-70 seconds. The current priority is to make the
remaining crawl-cycle body cost visible enough to choose the next speed fix from data, not log
inspection.

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

## Current slice

- Add cycle-local timing summaries for `lease`, `precheck`, `fetch`, `parse`, `scheduler`,
  `persist`, and queue waits.
- Persist the timing summary in the existing runtime snapshot so `/stats` can expose it without a new
  schema.
- Add compact p95 timing fields to the cycle-complete log.

## Acceptance

- Runtime snapshots include `timing_summary.samples`, outcome counts, and per-stage
  `count` / `avg` / `p50` / `p95` / `max`.
- Cycle-complete logs include compact p95 timing fields.
- Related tests and lint pass before deploy.

## Next checks after deploy

- Confirm `/health`, `/stats`, and `/stats/diagnostics` are healthy.
- Confirm `/stats` exposes `runtime.payload.timing_summary`.
- Observe at least two production cycles and identify the top p95 stage among `lease`, `precheck`,
  `fetch`, `scheduler`, `persist`, and queue waits.
- Use that measured top stage to choose the next speed fix.
