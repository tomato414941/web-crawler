# web-crawler plan

## Current milestone: make host-first execution real

The project now has a single greenfield schema baseline and no known active migration bridge.
Production telemetry showed that crawl latency is dominated by robots and HTTP request time, and
that host-first read-model reporting was too coarse to tell whether the read model was actually
serving leases. The immediate priority is to make `host_runnable_heads` the normal host-first
execution path and measure the before/after effect in production.

## Completed

- Added host latency observations: EWMA, last observed latency, observed timestamp, and sample count.
- Deployed the host latency observation fields to production.
- Reset database migrations to a single `001_schema.sql` baseline.
- Reset production `schema_migrations` to the new baseline.
- Removed archived migration history from the repo.
- Removed the one-time migration baseline bridge.
- Removed test cleanup for obsolete scheduler table names.
- Renamed the remaining internal refresh queue constant to refresh vocabulary.
- Split cycle timing enough to show that current production latency is mostly robots/precheck and
  fetch cost, not only lease selection.
- Shortened the robots fetch timeout and verified production still runs.
- Exposed host-first read-model fallback counters in runtime stats.
- Added a per-host robots fetch lock so concurrent workers share one robots.txt check.
- Deployed commit `88359ef` and verified `/health`, `/stats`, and `/stats/diagnostics`.
- Added cause-oriented telemetry for fetch, robots, lease, and pipeline waits.
- Split runtime payload into `active_cycle` and `last_completed_cycle`.
- Deployed telemetry commits `a05c657` and `5af97c3`, then verified active and completed cycle telemetry in production.
- Changed normal host-first leasing to read across the combined normal surface instead of recording
  an empty runnable-queue fallback before trying scheduled work.
- Made host-head reads recheck `host_state` dynamically and prefer warm hosts with robots/history.
- Replaced aggregate-delta lease telemetry inference with per-lease scheduler diagnostics.
- Suppressed noisy `httpx` request logs in daemon mode.

## Current slice

- Deploy the host-first read-model fix.
- Evaluate whether `read_model_hits` becomes the dominant host-first path and fallback stops being
  reported for normal scheduled work.
- Compare before/after `pages_per_second`, `robots_cache_statuses`, `fetch_p95`, `robots_p95`,
  and `scheduler_p95`.

## Acceptance

- `/stats` shows host-first leases mostly coming from read-model hits rather than fallback.
- Normal scheduled work no longer creates fallback misses only because the runnable queue is empty.
- Related tests and lint pass before the next deploy.

## Next checks after deploy

- Inspect whether `lease_fallbacks.miss` drops after per-lease diagnostics and normal-surface read.
- Inspect whether robots timeout/connect errors justify more aggressive robots policy.
- Inspect whether fetch p95 is caused by timeout/connect/http-error distribution or successful slow hosts.
- Keep changes schema-free unless runtime evidence shows a durable metrics table is needed.
