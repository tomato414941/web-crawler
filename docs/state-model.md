# Crawler State Model

This document defines a practical state model for future refactors. It does not describe the current implementation directly, but it is also not the top-level idealized model. Its job is to describe which states the system should treat as source-of-truth in order to make correct scheduling decisions.

The higher-level principles live in [crawler-model.md](/home/dev/projects/web-crawler/docs/crawler-model.md).

This document should be read as a transition/convergence model that helps move the current crawler toward that ideal.

## Goals

- Keep scheduler truth small and explicit.
- Separate durable facts from current scheduling state.
- Make it obvious which values are source-of-truth and which are derived.
- Avoid storing the same meaning in multiple places.
- Provide a practical intermediate model for moving the current crawler toward the ideal one.

## Core Rule

One URL should have one durable ledger record and at most one current scheduler membership.

`ready` is derived. It is not a durable state.

## Durable State Groups

### 1. URL ledger

The URL ledger answers: do we know this URL, and what is the latest durable fact about it?

Fields that belong here:

- normalized URL
- domain / host key
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
- `runnable`
- `leased`
- `quarantined`
- `done`
- `failed`

Interpretation:

- `discovered`: known, but not currently eligible for normal leasing
- `runnable`: eligible for normal leasing now
- `leased`: currently owned by a worker
- `quarantined`: intentionally excluded from normal leasing
- `done`: terminal success state
- `failed`: terminal failure state

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

- `ready`
- `pending_total`
- `blocked_host_backoff`
- `blocked_domain_next_request`
- `pages_per_second`
- top pending / blocked domain tables

`ready` is derived from:

- membership in `runnable`
- no active lease
- host state allowing execution now

If `ready` is persisted as a primary state, it will drift.

## State Transitions

### URL transitions

Normal path:

1. `discovered -> runnable`
2. `runnable -> leased`
3. `leased -> done`

Failure path:

1. `leased -> quarantined`
2. `leased -> failed`

Retry path:

1. `quarantined -> runnable`

Recrawl path:

1. `done -> discovered`

## Invariants

These must always hold.

1. A URL cannot be in both `runnable` and `leased`.
2. A URL cannot be in both `runnable` and `quarantined`.
3. A `done` URL cannot be in any live scheduler queue.
4. A `failed` URL cannot be in any live scheduler queue.
5. Every `leased` URL must have an active lease record.
6. `ready` must be derivable from scheduler membership plus host state.
7. Queue membership must be the single source of truth for current scheduler state.

## What seeds are

Seeds are only a bootstrap input set.

They are not a long-term scheduler category.

Once admitted into the system, seed-derived URLs should be treated by the same discovered-to-runnable rules as any other URLs.

## Target Interpretation For Current Concepts

Current concepts should converge toward this meaning:

- `backlog` => discovered pool
- `exploration` => runnable pool
- `frontier_lease_active` => lease state
- blocked-domain-backoff queue => quarantine pool
- host scheduler tables => host state

This is a convergence target, not a claim about current implementation quality.

## Immediate Design Consequences

1. The URL ledger should stop being the scheduler's current-state truth.
2. Queue membership should become the only truth for live scheduler state.
3. `ready` should remain derived.
4. Bootstrap should be separated from normal exploration supply.
5. Seed-derived special treatment should disappear from the scheduler.
