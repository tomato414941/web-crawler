# Web-Crawler System Architecture

This document sits between the idealized crawler model and the eventual runtime implementation.

It is broader than [crawler-principles.md](/home/dev/projects/web-crawler/docs/crawler-principles.md),
which defines the ideal principles, and broader than
[scheduler-state-model.md](/home/dev/projects/web-crawler/docs/scheduler-state-model.md), which focuses on scheduler
source-of-truth boundaries.

Its job is to explain how the whole `web-crawler` project should be decomposed as a system.

## Purpose

This document answers a practical question:

If the crawler model is the ideal north star, what are the major subsystems this project should
ultimately expose, and what should each subsystem own?

## System Layers

At the project level, the crawler should converge toward these layers:

1. URL ledger
2. Discovery and admission
3. Scheduler membership
4. Execution and lease ownership
5. Host state
6. Fetch / parse / finalize / persist pipeline
7. Read models and operator surfaces
8. Bootstrap and seed management

These layers should be separable even if the runtime still shares tables or code paths today.

## 1. URL Ledger

The URL ledger owns durable URL identity and durable URL history.

It should answer:

- do we know this URL?
- what is the normalized URL identity?
- what was the latest durable outcome?
- when did we first and last observe meaningful events for it?

It should not own:

- queue membership
- runnable truth
- active lease ownership
- host pacing

The ledger is durable fact, not live scheduling truth.

## 2. Discovery And Admission

Discovery and admission own the transition from "a URL was found" to "the scheduler may consider
this URL."

This layer includes:

- discovered URL intake
- duplicate suppression before scheduler admission
- initial policy shaping
- host-aware or budget-aware admission into live scheduler surfaces

This is where `discovered` belongs conceptually.

`discovered` should be understood as:

- known by the system
- not yet given normal scheduler membership

It should not automatically collapse into `backlog`.

## 3. Scheduler Membership

Scheduler membership owns the current live treatment of a URL.

This layer should answer:

- is this URL part of normal scheduling right now?
- if so, on which live surface?
- is it runnable, deferred, quarantined, or otherwise excluded?

The scheduler should be the only source of truth for live URL treatment.

This layer is separate from both:

- durable ledger fact
- host pacing state

Operational lanes are optional implementation surfaces, not first-class model concepts.

If the runtime keeps multiple worker lanes, they should be understood as temporary or operational
groupings layered on top of scheduler membership, not as the primary definition of URL state.

## 4. Execution And Lease Ownership

Execution owns active work ownership.

This layer should answer:

- which worker currently owns this URL?
- when does that ownership expire?

Execution state should be explicit and small.

`leased` is not a ledger concept and not a host-state concept. It is an execution concept.

## 5. Host State

Host state owns politeness, backoff, and capacity at the host/site level.

This layer should answer:

- may we touch this host now?
- what is the next safe request time?
- is the host cooled down?
- how much in-flight capacity should this host get?

Host state is not URL state. It is scheduler input.

## 6. Crawl Pipeline

The crawl pipeline owns how work moves through execution.

The project has already converged on these pipeline stages:

- fetch
- parse
- finalize
- persist

This split is important because the scheduler should not be forced to own all later stages
synchronously.

Pipeline stages are operational boundaries, not durable URL identity boundaries.

## 7. Read Models And Operator Surfaces

Operator views should be derived from primary state, not treated as scheduler truth.

This layer includes:

- `/stats`
- queue/readiness summaries
- top-domain tables
- error breakdowns
- runtime snapshots

These are useful, but they are not primary state.

## 8. Bootstrap And Seed Management

Seeds are bootstrap input, not a permanent scheduler category.

This layer owns:

- seed catalog maintenance
- rendering runtime seed sets
- initial system bootstrap

It should not leak into normal scheduler treatment once URLs are inside the crawler.

## Mapping Current Concepts Toward The Model

Current implementation concepts should converge toward the following meaning:

- `url_ledger` => URL ledger
- `discover` / admission logic => discovery and admission
- `scheduler_queue_*` => scheduler membership surfaces
- worker lanes / queue-specific worker pools => operational execution surfaces, not model truth
- `active_leases` => execution ownership
- `domain_state` => host state
- `fetch -> parse -> finalize -> persist` => crawl pipeline
- `/stats` and runtime payloads => read models
- seed catalog and bootstrap paths => bootstrap layer

This is a convergence target, not a claim that the current runtime is already cleanly separated.

## Main Gaps Versus The Ideal Model

Today, the project still has several major convergence gaps:

1. discovery and scheduler membership are still too tightly coupled
2. breadth control still relies too much on daemon-side policy around a URL-first core
3. queue naming still mixes state language and intent language
4. `priority` still carries too many meanings at once
5. bootstrap and seed influence are not fully isolated from normal scheduling

## Immediate Design Consequences

1. ledger insertion and scheduler admission must remain separable
2. live scheduler truth must remain outside the ledger
3. host-first breadth should move into scheduler truth, not remain mostly policy glue
4. read models must stay derived
5. seed handling should become less visible to normal runtime scheduling
6. worker lanes should remain subordinate to state, intent, and strategy rather than defining them
