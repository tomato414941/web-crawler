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
- `fetch admission`: whether a selected URL is worth reading as a response body

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

## Why State Names Matter

Scheduler surface names should describe where work is now, not why the crawler wants it.

Reasons:

- action or purpose terms belong to intent
- live scheduler surfaces should emphasize current treatment
- the contrast should be executable versus not-yet-executable, not one purpose versus another

In the ideal model, `explore` remains an intent, while surface names use state terms such as
`scheduled` and `runnable`.

## Runnable Capability Principle

The scheduler should not be modeled as a raw set of URLs. It is better understood as a set of
`runnable capability`.

The scheduler primarily wants to know:

- which hosts/sites are open right now
- how much runnable work each host has
- which intents that work belongs to

In this sense, the scheduler's first-class unit leans more toward host/site than raw URL.

`host runnable capability` and `host runnable head` are related but not the same concept.
Capability describes whether a host/site can produce useful work now, and roughly how much work it
can offer. A head is the representative next URL selected from that host/site. Both are runtime
execution read-model concepts; neither replaces scheduler membership as the source of truth for
which URLs are live.

Runtime implementations may keep physical queue projections, worker lanes, or operator-facing
views. Those are execution details below this concept. The abstract rule is that normal crawling
should be driven by host/site runnable capability rather than by a raw URL queue.

For the runtime execution design, see
[scheduler-execution.md](/home/dev/projects/web-crawler/docs/scheduler-execution.md).

## Fetch Admission Principle

The scheduler may select a URL, but that does not mean the crawler should read the entire response
body. Fetch admission is the boundary between "this URL is selected for an attempt" and "this
payload is useful enough to spend body-read, parse, and storage cost on".

The abstract rule is:

- HTML and safe text can become parseable page content
- binary, media, archive, font, image, and stream resources are metadata-only by default
- one URL must not be able to hold a worker or cycle indefinitely
- metadata-only completion is a valid outcome, not a crawl failure

This keeps the crawler focused on WWW discovery rather than becoming an unbounded downloader.

## Naming Guidance

Naming guidance from the abstract model:

- use `ledger` names for durable facts
- use `host_ledger` for durable host identity and history, not runtime pacing
- use `scheduled` / `runnable` / `leased` / `blocked` names for live state
- use `explore` / `refresh` / `retry` for policy intent

Purpose terms should stay in intent fields rather than becoming scheduler surface names.
