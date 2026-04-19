# WWW Crawler Concepts

This document captures an abstract model for an idealized WWW crawler. It is not a description of the current `web-crawler` implementation. Its role is to provide design principles for naming and responsibility boundaries.

## Core Separation

An ideal crawler should separate at least these six concerns:

- `ledger`: durable URL identity and history
- `host ledger`: durable host identity and history
- `scheduler membership`: which live surface a URL currently belongs to
- `execution`: active leases and worker ownership
- `host state`: host/site-level politeness and backoff
- `policy intent`: why the system wants to fetch this URL next

The key rule is to keep `state` and `intent` separate.

## States Versus Intents

Natural primary states look like this:

- `discovered`
- `scheduled`
- `runnable`
- `leased`
- `blocked`
- `terminal`

By contrast, these are more naturally modeled as intents:

- `explore`
- `refresh`
- `retry`

Examples:

- `runnable + explore`
- `scheduled + refresh`
- `blocked + retry`

Without this split, queue names end up carrying both "where is this URL now?" and "why do we want it?".

## Why `exploration` Feels Off

`exploration` is somewhat awkward as a state name.

Reasons:

- it reads like an action or purpose term
- as a live scheduler surface name, it emphasizes intent more than current position
- against `backlog`, the real contrast often looks more like frontline vs deferred, not explore vs not-explore

In the ideal model, `explore` remains an intent, while surface names converge toward state terms such as `scheduled` and `runnable`.

## Runnable Capability Principle

The scheduler should not be modeled as a raw set of URLs. It is better understood as a set of
`runnable capability`.

The scheduler primarily wants to know:

- which hosts/sites are open right now
- how much runnable work each host has
- which intents that work belongs to

In this sense, the scheduler's first-class unit leans more toward host/site than raw URL.

Runtime implementations may keep physical queue projections, worker lanes, or operator-facing
views. Those are execution details below this concept. The abstract rule is that normal crawling
should be driven by host/site runnable capability rather than by a raw URL queue.

For the runtime execution design, see
[scheduler-execution.md](/home/dev/projects/web-crawler/docs/scheduler-execution.md).

## Naming Guidance

Naming guidance from the abstract model:

- use `ledger` names for durable facts
- use `host_ledger` for durable host identity and history, not runtime pacing
- use `scheduled` / `runnable` / `leased` / `blocked` names for live state
- use `explore` / `refresh` / `retry` for policy intent

That makes `exploration` better understood as a transitional name from an implementation that has not fully separated state from intent yet, rather than the ideal final name.
