# web-crawler plan

## Current milestone: incremental host-first frontier

The current priority is to remove global scheduler scans from normal crawler startup and lease
selection. The crawler should choose work from a cheap host-first runtime cache, not rebuild host
eligibility from the full URL queue every cycle.

## Completed in this slice

- Removed the crawl-cycle-start global `host_runnable_heads` rebuild from the normal runtime path.
- Made queue insert/delete/replace update affected `(physical_queue, host)` heads incrementally.
- Made stale read-model candidate deletion refresh that host from source queue membership.
- Replaced the host-first derived-query fallback with a bounded queue scan safety fallback.
- Added host-local queue indexes and a `host_runnable_heads.head_url` index.
- Documented that `host_runnable_heads` is an incremental worker-facing cache, not scheduler truth.

## Acceptance

- Daemon startup does not log or wait for a full `host_runnable_heads` refresh.
- Leasing uses `host_runnable_heads` first and does not rebuild host heads globally on cache miss.
- Queue mutations keep the affected host head usable without requiring a cycle restart.
- Stale head rows are self-healed by host-local refresh.
- Related tests and lint pass before deploy.

## Next checks after deploy

- Confirm `/health`, `/stats`, and `/stats/diagnostics` are healthy.
- Confirm expired `active_leases` do not accumulate.
- Confirm crawler logs no longer include the previous ~24s `Refreshed host runnable-head read model`
  startup delay.
- Compare cycle completion time and pages/sec against the previous `300 pages in 145.2s`
  production baseline.
- Watch PostgreSQL CPU while the crawler is running at the current production concurrency.
