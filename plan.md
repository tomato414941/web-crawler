# web-crawler plan

## Current milestone: make crawl latency explainable

The project now has a single greenfield schema baseline and no known active migration bridge.
The immediate priority is to make production crawl latency explainable before applying more speed
optimizations.

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

## Current slice

- Use the new production telemetry to choose the next speed optimization.
- Compare fetch outcome counts, robots status counts, lease fallback misses, and DB/pipeline p95.
- Avoid new scheduler or DB abstractions until telemetry points to a concrete bottleneck.

## Acceptance

- The next optimization target is selected from `last_completed_cycle` evidence.
- The chosen change has a measurable before/after metric in `/stats`.
- Related tests and lint pass before the next deploy.

## Next checks after deploy

- Inspect whether `lease_fallbacks.miss` is idle polling or scheduler read-model drift.
- Inspect whether robots timeout/connect errors justify more aggressive robots policy.
- Inspect whether fetch p95 is caused by timeout/connect/http-error distribution or successful slow hosts.
- Keep changes schema-free unless runtime evidence shows a durable metrics table is needed.
