# Design Principles

This document describes the conceptual direction of `web-crawler`. It is not a complete
implementation inventory. Current implementation details live in the system architecture and
scheduler documents.

## Product Positioning

`web-crawler` is a broad public web crawler.

It is not a site-specific crawler. Seed URLs are bootstrap entry points for discovery, not
scope boundaries. Unless a crawl run explicitly applies a restrictive policy such as same-host
crawling, discovered links may lead to other hosts and may become valid crawl targets.

The public web boundary is still a hard safety boundary. Private network addresses, loopback
addresses, local metadata endpoints, unsupported URL schemes, and unsafe redirects are outside
the intended crawl surface even if they are discovered from public pages.

The crawler core should remain downstream-neutral. It may support search indexing, LLM-agent
observation, monitoring, archival workflows, and web graph analysis, but it should not become a
search ranking engine, an LLM memory system, or a domain-specific extractor.

The crawler is responsible for producing reliable crawl observations: URL identity, fetch events,
timestamps, HTTP metadata, redirects, content type, extracted links, host state, failure state,
freshness signals, and policy decisions. Downstream systems can turn those observations into
rankings, summaries, alerts, embeddings, or product-specific judgments.

## Current Core Principles

### Observation Before Interpretation

The crawler records facts it can observe directly. It should avoid storing downstream judgments
such as "important", "trustworthy", or "relevant" as core facts unless they are clearly labeled
as policy outputs or derived downstream signals.

Useful crawler-native observations include status codes, redirect targets, content handling
decisions, link provenance, failure classes, host behavior, and scheduler state transitions.

### Bounded External Contact

The web is untrusted, unbounded, inconsistent, and constantly changing. External contact must be
bounded by explicit limits on duration, response size, redirect count, extracted links, admitted
links, retries, browser rendering work, per-host concurrency, and global backlog.

This is a control property, not only a bug-prevention measure. A broad crawler should assume some
pages will attempt to consume unlimited time, memory, network, storage, or scheduling capacity.

### Failure As State

At web scale, failure is a normal crawl outcome. Timeouts, DNS failures, connection failures, TLS
errors, robots exclusions, unsupported content types, redirect loops, oversized responses, parse
failures, browser failures, and egress blocks should be represented as classified states.

Failure classification should feed scheduling, retry, cooldown, retirement, and observability.
Generic exceptions are not enough for a crawler that needs to explain and adapt its behavior.

### Metadata-First Resource Handling

Broad crawling does not require storing every resource body. A resource can be a valid observation
even when its full content is not stored or parsed.

HTML pages may support text and link extraction. PDFs and binary resources may be represented as
metadata-only observations. Oversized or unsupported resources can still contribute URL identity,
fetch time, status, headers, content type, and discovery provenance.

### Discovery And Admission Are Separate

Discovery means the crawler saw a URL. Admission means the crawler chose to put that URL into the
frontier. Admission is a bounded policy decision.

The crawler should be able to explain why URLs were admitted, rejected, deferred, skipped, retried,
or retired where practical. At broad-web scale, the set of non-crawled URLs is much larger than the
set of crawled URLs, so rejection and deferral reasons matter.

### Safety Is Cross-Cutting

Safety belongs at admission time and immediately before external contact. A harmless seed can link
to an unsafe target, a public URL can redirect to a private address, and a rendered page can trigger
additional network requests.

Safety checks should therefore appear across admission, scheduling, HTTP fetching, browser
rendering, robots fetching, link checking, and persistence boundaries.

## Architectural Direction

The crawler should evolve around a clear separation of planes.

### Policy Plane

The policy plane decides how crawl capacity is allocated. It owns tradeoffs such as exploration
versus revisit, new-host expansion, retry budget, browser rendering budget, content handling
thresholds, per-host limits, and backlog targets.

### Scheduler / Frontier Plane

The scheduler manages candidate state transitions: pending, scheduled, runnable, leased,
completed, failed, retried, refreshed, quarantined, and retired. It owns durable URL state,
deduplication, leases, retry timing, refresh timing, and concurrency coordination.

### Execution Plane

The execution plane performs work: fetch, render, parse, classify, extract links, finalize, and
persist observations. It should convert expected web failures into structured outcomes rather than
letting worker tasks die silently.

### Observation Plane

The observation plane reports system behavior: throughput, backlog, admission rate, rejection
reasons, content-type distribution, redirect behavior, error classes, retry volume, freshness lag,
storage growth, host pressure, browser rendering usage, and scheduler readiness.

### Adaptation Loop

The adaptation loop uses observations to adjust policy. For example, backlog pressure may tighten
admission, high failure rates may increase cooldown, expensive rendering may shrink browser budget,
and freshness lag may shift capacity toward revisit work.

The important abstraction is explicit resource allocation. The system should be able to explain
how much crawl capacity is spent on exploration, observation, retry, refresh, and new-host
expansion.

## Non-Goals

The crawler core should not become:

- a search ranking engine
- an LLM memory system
- a domain-specific scraper
- a general-purpose browser automation product
- a full web archive by default
- a downstream-specific extractor or summarizer

These systems can consume crawler observations, but their product logic should live outside the
crawler core.

## Future Design Pressure

The current architecture should leave room for:

- explicit crawl-capacity budgets for exploration, revisit, retry, refresh, browser rendering, and
  new-host discovery
- richer admission and rejection reason tracking
- stronger freshness and change-detection policy
- better provenance for extracted text and link context
- cursor pagination and API evolution for large result sets
- more detailed resource history across repeated fetch events
- stronger network-level containment beyond application-layer egress checks

These are design directions, not all current guarantees.

## Summary

The crawler should not try to store the entire web or understand the entire web.

It should safely, adaptively, and explainably observe the broad public web, producing neutral crawl
data that downstream systems can trust and use.
