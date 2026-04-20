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

## Current slice

- Add cause-oriented telemetry for fetch, robots, lease, and pipeline waits.
- Split runtime payload into `active_cycle` and `last_completed_cycle`.
- Keep the external stats endpoints available while allowing the internal runtime payload to change.
- Commit, push, deploy, and evaluate whether production now explains why fetch/robots/DB/pipeline are slow.

## Acceptance

- `/stats` exposes active and completed cycle views without mixing them.
- Runtime timing includes outcome counts for fetch, robots cache/status, and lease/fallback behavior.
- Related tests, full tests, lint, and diff checks pass before deploy.

## Next checks after deploy

- Inspect production telemetry after one active cycle and one completed cycle.
- Decide the next optimization from labeled evidence, not from raw p95 stage times.
- Keep changes schema-free unless runtime evidence shows a durable metrics table is needed.
