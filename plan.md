# web-crawler plan

## Current milestone: host runnable-head read model v2

The current priority is to use the loose host-level read model on the normal host-first lease path.
This read model is not source of truth; every candidate is still revalidated against queue
membership, active leases, and `host_state` before it can become crawler work.

The documentation split is:

- `crawler-concepts`: abstract model and naming principles
- `scheduler-state-model`: source-of-truth boundaries and invariants
- `scheduler-execution`: runtime lease strategy, read models, and hot-path constraints
- `system-architecture`: project-wide subsystem boundaries

## Completed

- Added `host_runnable_heads` as a derived read model table.
- Added rebuild support from scheduler queue membership and `host_state`.
- Added a read API for ready host-head candidates from the read model.
- Measured production rebuild/read latency and confirmed the read-model candidate query is much
  cheaper than the current derived host-first query.

## In this slice

- Normal host-first lease reads candidate heads from `host_runnable_heads` first.
- Candidate URLs are still leased through source-of-truth revalidation.
- Stale read-model candidates are dropped when revalidation misses, so old heads do not keep
  blocking newer candidates.
- The crawler refreshes the read model once at crawl-cycle start, not on every lease.
- If the read model is empty or unavailable, host-first lease falls back to the existing derived
  query.

## Acceptance

- `host_runnable_heads` exists on new and migrated databases.
- Rebuild creates one head row per host and physical queue.
- Read model candidates respect `runnable_at <= now`, `limit`, and excluded hosts.
- Normal host-first lease uses read-model candidates when available.
- Stale read-model candidates do not permanently block the lease path.
- Existing derived host-first behavior remains as a fallback.
- Related tests and lint pass.

## Next checks after deploy

- Confirm production logs show the crawl-cycle read-model refresh.
- Compare production `lease=` timings before and after the read-model-first switch.
- Check whether stale-candidate misses still force frequent fallback.
- If fallback remains common, add a per-host or incremental head refresh instead of rebuilding the
  whole read model on the hot path.
