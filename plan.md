# web-crawler plan

## Current milestone: host-first execution thin slice

The current priority is to reduce the gap between the crawler concepts and runtime
behavior while also improving crawl throughput.

The target model is:

- `ledger` keeps durable URL identity and history
- `scheduler_state` is derived from queue membership, active leases, and host state
- `intent` explains why a URL should be crawled
- `active_leases` owns execution
- `host_state` owns politeness, backoff, and host-level runtime budget

## Active work

- Treat `frontline` and `deferred` as internal physical scheduler projections.
- Run normal crawl workers against the combined `normal` runnable surface.
- Lease normal work with host-first selection so deferred work does not need promotion
  to frontline before it can run.
- Keep refresh work separate with a small reserved worker budget when concurrency is high
  enough.
- Expose `normal_workers` in runtime stats while keeping legacy worker fields as
  compatibility aliases.

## Not in this slice

- No database schema migration.
- No removal of `frontline` / `deferred` physical queue tables.
- No immediate jump to 100 production concurrency.
- No host-per-inflight increase beyond the existing adaptive budget rules.

## Acceptance

- Deferred-only normal crawl work can be processed by regular workers.
- Runtime stats show `normal_workers`.
- Existing host inflight limiting still prevents one host from consuming all workers.
- Production can be tested first at about 24 concurrent workers before considering higher
  concurrency.
