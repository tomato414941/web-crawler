# Scheduler Execution

This document describes how scheduler state becomes executable crawler work.
It sits below the abstract crawler concepts and next to the scheduler state model.

Related documents:

- [crawler-concepts.md](crawler-concepts.md) defines the abstract model.
- [scheduler-state-model.md](scheduler-state-model.md) defines scheduler source-of-truth boundaries.
- [system-architecture.md](system-architecture.md) defines the project-wide subsystem split.

## Purpose

Scheduler execution answers a narrower question:

Given scheduler membership, host state, and active leases, how should the crawler select the next
work without making the lease path too expensive?

This document is about runtime execution strategy. It is not the source of truth for durable URL
state, policy intent, or crawl pipeline output.

## Execution Layers

Execution should be understood as four related but separate layers:

1. Scheduler membership: which live scheduler surface a URL belongs to.
2. Execution strategy: how workers choose work from those surfaces.
3. Runtime-facing read models: derived views used by workers or operators.
4. Operator stats: summaries such as worker counts, active hosts, and queue depth.

The same table or query may currently serve more than one layer, but the meanings should remain
separate.

## Current Runtime Interpretation

The current runtime still has physical scheduler queues. Those queues are implementation surfaces,
not the primary runtime subject for normal crawling.

Current interpretation:

- `runnable` and `scheduled` are internal scheduler membership projections.
- `normal` is the runtime-facing runnable view for regular crawling across those projections.
- `refresh` work remains separate from regular crawling.
- active leases own execution after a URL is selected.
- host state decides whether a host may be touched and how much capacity it has.

Normal crawler workers should lease from the combined `normal` view with host-first selection.
They should not need to copy scheduled work into a separate runtime-only queue before it can run.

## Hot Path Rule

Lease selection is a hot path. It runs once per worker repeatedly, so it must avoid expensive
global derivation on every lease.

The lease path should not repeatedly derive host runnable capability by scanning, sorting, and
windowing all ready queue rows for every worker lease.

The scheduler should move toward a cheap host-first executable view:

- host eligibility should be available without repeated correlated host lookups
- host-level runnable heads should be cheap to read
- URL selection should happen after a host has been chosen
- operator-facing summaries should not make the worker lease path slower

It is acceptable for read models to be derived. It is not acceptable for every lease to rebuild an
expensive derived model from scratch when the crawler runs at high concurrency.

## Observed Lease Bottleneck

An earlier production bottleneck was in host-first lease selection.

Observed during the April 2026 production investigation:

- 24 crawler workers kept PostgreSQL near 90-95% CPU while crawler CPU stayed much lower.
- The current host-first candidate query took roughly hundreds of milliseconds to about one second.
- Disabling PostgreSQL JIT reduced one measured query from about 1035 ms to about 662 ms.
- Replacing repeated correlated `host_state` lookups with a single join reduced one measured shape to about 309 ms, and about 266 ms with larger `work_mem`.
- With one crawler worker, publish/finalize pressure disappeared, but lease selection still often took hundreds of milliseconds.

These numbers were observations, not permanent SLOs. They explain why the host runnable-head read
model exists and why lease selection remains a hot-path concern.

## Design Constraints

Execution changes should preserve these constraints:

- keep URL ledger facts separate from live scheduler execution
- keep queue membership as scheduler truth until a replacement exists
- keep active leases as the owner of in-flight execution
- keep host-first breadth as the normal execution strategy
- keep `refresh` separate from normal crawling
- keep `normal` as a runtime-facing view, not a durable URL state
- accept small schema migrations when they remove global hot-path work

## Implementation Direction

The current implementation direction is to keep queue membership as the scheduler source of truth,
but move normal host-first candidate selection onto an incremental loose read model.

Implemented order:

1. Replace correlated `host_state` subqueries in host-first candidate selection with a single join.
   This is now implemented in the URL ledger query builder.
2. Evaluate setting PostgreSQL JIT off for crawler sessions or for the specific hot-path query.
3. Recheck whether `COUNT(*) OVER (PARTITION BY host)` is still too expensive at production scale.
   Production measurement shows it remains the dominant cost.
4. Add a loose host runnable-head projection and measure rebuild/read latency.
5. Use the host runnable-head read model as the first normal host-first lease candidate source.
6. Stop rebuilding that projection globally at crawl-cycle start.
7. Refresh only the affected `(physical_queue, host)` heads when queue membership changes.

The host runnable-head projection is a runtime read model. It summarizes which hosts currently have
executable work and enough host capacity. It is not a second source of truth for URL membership.

The current `host_runnable_heads` table is intentionally transitional: it stores the representative
head URL together with host capability signals such as execution tier, runnable time, latency bucket,
and runnable URL count. Those capability fields are scheduler signals, not durable facts.
`runnable_url_count` should therefore be treated as an ordering/readiness signal rather than an
exact source-of-truth count.

Conceptually:

- host runnable capability answers whether a host can produce work and roughly how much it can offer
- host runnable head answers which URL should represent that host next
- scheduler queues remain the source of truth for URL membership
- active leases remain the source of truth for in-flight execution

The lease path therefore uses a cheap-miss pattern:

- read candidate host heads from `host_runnable_heads`
- revalidate each candidate against queue membership, terminal ledger state, active leases, and
  `host_state`
- delete and host-locally refresh stale read-model candidates after a miss
- use a bounded queue scan only as a safety fallback when the read model is empty or unavailable

The read model is maintained incrementally by queue insert/delete/lease/finalize paths. Dirty
refresh, repair, and rebuild are maintenance mechanisms for projection health; they are not the
normal execution path. A global rebuild remains a manual repair mechanism, not normal crawler
startup behavior. If production shows frequent fallback after this switch, the next step should be
to find the mutation path that is not refreshing its affected host, not to reintroduce a global
rebuild.
