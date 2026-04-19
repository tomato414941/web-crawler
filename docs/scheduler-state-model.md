# Scheduler State Model

This document defines a practical state model for future refactors. It does not describe the current implementation directly, but it is also not the top-level idealized model. Its job is to describe which states the system should treat as source-of-truth in order to make correct scheduling decisions.

The higher-level concepts live in [crawler-concepts.md](/home/dev/projects/web-crawler/docs/crawler-concepts.md).

This document should be read as a transition/convergence model that helps move the current crawler toward that ideal.

## Goals

- Keep scheduler truth small and explicit.
- Separate durable facts from current scheduling state.
- Make it obvious which values are source-of-truth and which are derived.
- Avoid storing the same meaning in multiple places.
- Provide a practical intermediate model for moving the current crawler toward the ideal one.

## Core Rule

One URL should have one durable ledger record and at most one current scheduler membership.

Operator-facing `runnable` is derived. It is not a separately stored durable state.

## Durable State Groups

### 1. URL ledger

The URL ledger answers: do we know this URL, and what is the latest durable fact about it?

Fields that belong here:

- normalized URL
- host key
- discovery metadata
- first seen timestamp
- last success timestamp
- last failure timestamp
- final outcome when terminal

Fields that should not be scheduler truth here:

- pending
- leased
- queue membership
- quarantine membership

The ledger is history and identity, not the live scheduler.

### 2. Scheduler membership

The scheduler answers: how should this URL be treated now?

Minimal live states:

- `discovered`
- `scheduled`
- `runnable`
- `leased`
- `blocked`
- `terminal`

Interpretation:

- `discovered`: known, but not yet admitted into normal scheduler membership
- `scheduled`: admitted into scheduler membership, but not runnable yet
- `runnable`: admitted and runnable now
- `leased`: currently owned by a worker
- `blocked`: temporarily excluded from runnable leasing by host/backoff/quarantine constraints
- `terminal`: terminal end state, whether success or failure

The scheduler should own these states directly. The ledger should not duplicate them as a second source of truth.

### 3. Host state

The host scheduler answers: may we touch this host now, and how aggressively?

Minimal host state:

- `next_request_at`
- `backoff_until`
- `latency_ewma_ms`
- `fail_streak`
- `inflight_budget`

This state is host-scoped, not URL-scoped.

## Derived Values

These values are useful, but they are not primary state.

- `runnable` (in readiness / operator views)
- `pending_total`
- `blocked_host_backoff`
- `blocked_host_next_request`
- `pages_per_second`
- top pending / blocked host tables

The operator-facing `runnable` view is derived from:

- scheduler membership
- no active lease
- host state allowing execution now

If this derived `runnable` view is persisted as a separate primary state, it will drift.

## State Transitions

### URL transitions

Normal path:

1. `discovered -> runnable`
2. `discovered -> scheduled`
3. `scheduled -> runnable`
4. `runnable -> leased`
5. `leased -> terminal`

Failure path:

1. `leased -> blocked`
2. `leased -> terminal`

Retry path:

1. `blocked -> runnable`

Recrawl path:

1. `terminal -> discovered`

## Invariants

These must always hold.

1. A URL cannot be in both `runnable` and `leased`.
2. A URL cannot be in both `runnable` and `blocked`.
3. A `terminal` URL cannot be in any live scheduler queue.
4. A `discovered` URL should not also appear in normal scheduler membership.
5. Every `leased` URL must have an active lease record.
6. The operator-facing `runnable` view must be derivable from scheduler membership plus host state.
7. Queue membership must be the single source of truth for current scheduler state.

## What seeds are

Seeds are only a bootstrap input set.

They are not a long-term scheduler category.

Once admitted into the system, seed-derived URLs should be treated by the same discovered-to-runnable rules as any other URLs.

## Target Interpretation For Current Concepts

Current concepts should converge toward this meaning:

- `exploration` => frontline runnable surface
- `backlog` => deferred scheduled surface
- `active_leases` => lease state
- blocked-host-backoff queue => quarantine pool
- host scheduler tables => host state

This is a convergence target, not a claim about current implementation quality.

The conceptual split should happen before naming cleanup.

- state and intent should be modeled separately now
- existing names may remain temporarily while that split is introduced
- `backlog` should not be overloaded to mean `discovered`
- lanes are optional operational groupings, not primary scheduler concepts

## Immediate Design Consequences

1. The URL ledger should stop being the scheduler's current-state truth.
2. Queue membership should become the only truth for live scheduler state.
3. The operator-facing `runnable` view should remain derived.
4. Bootstrap should be separated from normal exploration supply.
5. Seed-derived special treatment should disappear from the scheduler.
6. If worker lanes exist, they should be derived from state and strategy, not the other way around.
