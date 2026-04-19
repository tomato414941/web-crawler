# web-crawler plan

## Current milestone: host-first lease query optimization

The current priority is to reduce the cost of the existing host-first lease path before adding a new
runtime read model. The durable host ledger foundation is already in place; normal lease execution
still depends on queue membership, active leases, and `host_state`, not on `host_ledger`.

The documentation split is:

- `crawler-concepts`: abstract model and naming principles
- `scheduler-state-model`: source-of-truth boundaries and invariants
- `scheduler-execution`: runtime lease strategy, read models, and hot-path constraints
- `system-architecture`: project-wide subsystem boundaries

## Completed in this slice

- Replaced repeated correlated `host_state` lookups in readiness and latency ordering with one
  `LEFT JOIN host_state` per candidate query.
- Preserved the existing host-first behavior and ordering.
- Kept missing `host_state` rows runnable by treating host timing and latency as zero.
- Kept URL membership and active leases as the execution source of truth.

## Not in this slice

- No schema migration.
- No `host_ready` execution read model.
- No `host_ledger` dependency in the lease path.
- No rewrite of the host pending count window yet.

## Acceptance

- Host gating by `next_request_at` and `backoff_until` still works.
- Host latency ordering still works.
- The runnable host-head query no longer contains correlated `host_state` subqueries.
- Related tests and lint pass.

## Next checks

- Measure the production host-first query again after deploy.
- Evaluate disabling PostgreSQL JIT for crawler sessions if query planning/execution is still costly.
- Recheck whether `COUNT(*) OVER (PARTITION BY host)` remains the dominant cost.
