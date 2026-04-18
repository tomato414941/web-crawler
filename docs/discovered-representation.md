# Discovered Representation

This document defines how `discovered` should be represented between the idealized crawler model
and the eventual runtime implementation.

It is narrower than [crawler-concepts.md](/home/dev/projects/web-crawler/docs/crawler-concepts.md) and
more conceptual than [scheduler-state-model.md](/home/dev/projects/web-crawler/docs/scheduler-state-model.md).

## Purpose

The goal is to answer one specific question:

How should the system represent a URL that is known, but does not yet have normal scheduler
membership?

## Core Position

`discovered` should be modeled as a state, not as a queue.

More precisely:

- a URL may be known by the system
- that URL may still be outside normal scheduler membership
- that condition is what `discovered` means

This keeps the concept aligned with the crawler model's separation between durable fact,
scheduler membership, execution, and policy intent.

## Relationship To URL Ledger

The URL ledger remains the durable source of truth for URL identity and history.

The ledger should answer questions such as:

- do we know this URL?
- when did we first see it?
- what is the latest durable outcome?

The ledger should not answer:

- is it runnable right now?
- which queue is it in?
- is it leased?

Therefore, `discovered` should not be stored as a live status column on the ledger.

Instead, a URL is in the `discovered` state when:

- it has a ledger record
- it does not currently have scheduler membership

## Relationship To Scheduler Membership

Scheduler membership is a separate concern from durable URL identity.

`discovered` means:

- known by the system
- not yet admitted into a normal live scheduler surface

By contrast:

- `runnable` means it has live scheduler membership and may be leased
- `leased` means execution ownership exists
- `quarantined` means it is intentionally excluded from normal leasing

This means `discovered` is logically before `runnable`, not a synonym for a deferred queue.

## Why Backlog Is Not `discovered`

`backlog` may be deferred work, but it is still scheduler work.

If a URL is in `backlog`, the scheduler has already decided:

- this URL belongs to a live scheduler surface
- it participates in current scheduling policy

That is different from `discovered`, which means:

- the URL is known
- but normal scheduler membership has not yet been assigned

So `backlog` should be read as scheduled-but-deferred, not discovered-but-unscheduled.

## Operational Surface

`discovered` is a conceptual state. That does not require the implementation to use a queue with
the same name.

The runtime may still need an operational surface for:

- admission work
- batching
- multi-worker claiming
- host-aware promotion into scheduler membership

That operational surface is an implementation detail, not the meaning of `discovered` itself.

In other words:

- `discovered` is the state
- an admission queue, admission view, or claimable table may process that state

## Multi-Worker Consequence

In a multi-worker system, workers should not interpret the full URL ledger as a work queue.

The durable ledger may be the source of truth for known URLs, while admission workers operate on a
smaller derived surface.

That keeps these concerns separate:

- durable URL fact
- admission processing
- scheduler membership

## Preferred Final Interpretation

The preferred final interpretation is:

1. URL appears in the ledger
2. URL is in `discovered`
3. admission logic decides whether and how to attach scheduler membership
4. URL becomes `runnable` or another live scheduler state

The important point is that `discovered` is not defined by queue name. It is defined by the absence
of current scheduler membership.

## Immediate Design Consequences

1. Ledger insertion and scheduler admission should be separable operations.
2. Queue membership should remain the truth for live scheduler state.
3. `backlog` should not be overloaded to mean `discovered`.
4. If the runtime needs a discovered-processing surface, it should be introduced as an operational
   mechanism, not as the conceptual definition of `discovered`.
