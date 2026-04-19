# web-crawler plan

## Current milestone: host runnable-head read model v1

The current priority is to add a loose host-level read model for measuring the next host-first
execution design. This read model is not source of truth; normal lease execution still depends on
queue membership, active leases, and `host_state`.

The documentation split is:

- `crawler-concepts`: abstract model and naming principles
- `scheduler-state-model`: source-of-truth boundaries and invariants
- `scheduler-execution`: runtime lease strategy, read models, and hot-path constraints
- `system-architecture`: project-wide subsystem boundaries

## Completed in this slice

- Added `host_runnable_heads` as a derived read model table.
- Added rebuild support from scheduler queue membership and `host_state`.
- Added a read API for ready host-head candidates from the read model.
- Kept existing `lease_next` and `lease_batch` behavior unchanged.

## Not in this slice

- No production lease-path switch to the read model.
- No strict synchronization on every queue or host-state mutation.
- No stale-candidate miss tracking yet.
- No partial per-host refresh yet.

## Acceptance

- `host_runnable_heads` exists on new and migrated databases.
- Rebuild creates one head row per host and physical queue.
- Read model candidates respect `runnable_at <= now`, `limit`, and excluded hosts.
- Existing lease behavior is unchanged.
- Related tests and lint pass.

## Next checks

- Measure full rebuild time on production data.
- Measure read-model candidate query latency with `EXPLAIN ANALYZE`.
- Compare read-model candidate latency against the current host-head query.
- If read latency is good, plan v2 cheap-miss lease selection with source-of-truth revalidation.
