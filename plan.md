# web-crawler plan

## Current milestone: prioritize host execution tiers

The project now has a single greenfield schema baseline and no known active migration bridge.
Production telemetry showed that normal host-first leases now come from `host_runnable_heads`, but
overall crawl latency is still dominated by robots and HTTP request time. The immediate priority is
to make the worker hot path prefer hosts that are more likely to produce useful pages, without
reintroducing dynamic global joins into lease selection.

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
- Made host-head reads recheck `host_state` dynamically without turning the lease path into a
  global warm-host sort.
- Replaced aggregate-delta lease telemetry inference with per-lease scheduler diagnostics.
- Suppressed noisy `httpx` request logs in daemon mode.
- Added an indexed `execution_tier` to `host_runnable_heads`.
- Derived tier from host runtime/history facts during read-model refresh:
  warm, probing, slow, or deferred.
- Preferred lower execution tiers in host-head reads before breadth and latency tie-breakers.
- Exposed lease execution-tier counts in runtime telemetry.
- Deployed commit `f99dc4b` and verified `/health`, active runtime stats, read-model hits, and tier
  telemetry in production.
- Backfilled existing production host-head tiers with a set-based update. A full global rebuild was
  too expensive for the current production queue size.

## Current slice

- Observe whether warm-tier leasing improves completed-cycle throughput once the active cycle
  finishes.
- Inspect why warm hosts still have high robots miss/connect-error rates.
- Decide whether explicit warm/probing worker budgets are needed next.

## Acceptance

- `/stats` continues to show host-first leases mostly coming from read-model hits rather than fallback.
- `timing_summary.counts.lease_execution_tiers` shows whether leases are warm, probing, slow, or deferred.
- Warm hosts are selected before probing hosts when both have ready normal work.
- Related tests and lint pass before deploy.

## Next checks after deploy

- Inspect whether warm tier leases dominate once enough warm hosts exist.
- If probing still consumes too much capacity, add explicit warm/probing worker budgets next.
- Inspect whether robots timeout/connect errors justify more aggressive robots policy.
- Inspect whether fetch p95 is caused by timeout/connect/http-error distribution or successful slow hosts.
- Keep future host ranking changes on stored/indexed read-model fields rather than dynamic lease joins.
