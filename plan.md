# web-crawler plan

This document is a periodically revalidated snapshot, not a source of truth by itself.
If it conflicts with the runtime, trust the code and update this file.

## Current state

Already done:

- `frontier` is no longer the only scheduler surface; physical queue tables are the working scheduler state.
- Active leases are isolated in `active_leases` (with migration compatibility from `frontier_lease_active`).
- Host-cooled URLs are physically isolated in `scheduler_queue_retry_quarantine`.
- Retry quarantine is restored through a small retry budget instead of bulk re-entry.
- `/stats` exposes `readiness` with `runnable`, `scheduled`, `blocked_domain_next_request`, `blocked_host_backoff`, and `retry_quarantine`.
- Scheduler observability is split out of `url_ledger` into `scheduler_observability.py`.
- Retry quarantine policy is split out of `url_ledger` into `scheduler_quarantine.py`.
- Daemon pre-cycle scheduling policy is split out of `daemon.py` into `daemon_policy.py`.
- The success path is split into explicit `fetch -> parse -> finalize -> persist` stages.
- Scheduler mutations run in a dedicated finalizer queue with a dedicated connection / executor.
- Durable failure and skip mutations also run through the finalizer path, even though failure handling is not yet fully symmetric with the success path.
- Basic latency-aware behavior exists already through per-host latency EWMA, lease-order latency penalties, and elevated inflight budgets for fast hosts.

Still not done:

- Failure handling is only partially staged: durable failure mutation is on the finalizer path, but runtime error bookkeeping and some classification still begin in fetch / parse workers.
- Scheduler policy is only partially latency-aware: latency influences lease tie-breaking and host inflight budget, but it is not yet a first-class policy input across scheduling and maintenance.
- Postgres is still carrying too many synchronous responsibilities on the scheduler path.
- Public docs and some runtime names still lag the current scheduler vocabulary.

## Adopted principles

These are the ideas worth keeping regardless of source project:

- Separate scheduler truth from operator read models.
- Keep the hot path small and throughput-oriented.
- Prefer requeue over in-slot retry.
- Keep host pacing state minimal and explicit.
- Keep planner logic thin.

## Explicit non-goals for adoption

These ideas should not be copied directly from other crawler projects:

- service boundaries that only make sense in a larger search stack
- another project's exact table layout or API contracts
- product-specific ranking, denylist, or source-policy behavior
- admin snapshot semantics as scheduler truth

The goal is to adopt durable principles, not to clone `web-search` runtime
structure into `web-crawler`.

## Active priorities

1. Re-center the project on a high-throughput crawler architecture.
   - Keep the crawler hot path to `lease -> fetch -> publish`.
   - Move durable logging, admin read models, and queue hygiene off the hot path.
   - Treat `web-crawler` as the long-term crawler foundation, not just the current daemon.

2. Split the current monolithic worker path into explicit pipeline stages.
   - `frontier / scheduler`
   - `fetch workers`
   - `parse workers`
   - `finalize workers`
   - `storage / publish workers`
   - `maintenance / recrawl / analytics`
   - Do not let one crawl slot own all of these steps end-to-end.

3. Make scheduling latency-aware without host-specific rules.
   - Measure per-domain request latency and error rate.
   - Downweight slow hosts dynamically.
   - Keep policies generic; do not add hard-coded domain exceptions.

## Target architecture

### Core principles

1. Optimize for system throughput, not single-URL completeness.
2. Make slow hosts consume fewer global resources automatically.
3. Separate the crawl hot path from operational visibility and maintenance.
4. Prefer generic scheduling rules over site-specific behavior.
5. Keep Postgres durable, but do not make it the center of every hot-path write.

### Hot path

The crawler hot path should be:

1. Lease a runnable URL from the scheduler.
2. Apply minimal safety and host scheduling checks.
3. Fetch the URL.
4. Publish a normalized fetch result to the next stage.

The hot path should not synchronously own:

- full parsing
- heavy content extraction
- admin logging and history shaping
- recrawl cleanup
- queue hygiene passes
- per-cycle analytics

### Stage boundaries

#### Stage 1: Frontier / Scheduler

- Owns URL readiness, leasing, retry timing, and host fairness.
- Produces leaseable work items.
- Knows nothing about content extraction or downstream consumers.

#### Stage 2: Fetch workers

- Owns outbound HTTP/browser fetching.
- Produces lightweight fetch results and timing data.
- Returns success/failure quickly.
- Uses dynamic host policies based on measured latency and failures.

#### Stage 3: Parse workers

- Extracts text, metadata, and outlinks from fetched pages.
- Runs independently from frontier leasing.
- Can be scaled separately from fetching.

#### Stage 4: Publish / Storage

- Applies scheduler mutations after parse on a dedicated finalizer path.
- Writes parsed content and metadata after finalization.
- Admits outlinks back into the scheduler.
- Exposes results to APIs or downstream consumers.

#### Stage 5: Maintenance

- Recrawl TTL handling
- stale queue cleanup
- read models
- aggregated stats
- runbooks and operational tasks

These must not block normal fetch throughput.

### Current runtime delta vs target

- Success path already follows `fetch -> parse -> finalize -> persist`.
- Success-side scheduler mutations already run off the main event loop.
- Failure-side durable state updates now also run on the finalizer path, but failure handling still begins in workers through runtime backoff/error bookkeeping.
- Scheduling already uses measured host latency as a secondary signal, but it is still driven more by retry/backoff and queue/surface policy than by latency.

### Scheduling model

- Per-domain concurrency must be dynamic, not fixed forever.
- Slow domains should be automatically downweighted.
- Fast domains should consume more idle capacity.
- Retry policy should be fail-fast and requeue-based.
- Robots transport failure and robots policy deny should remain separate concerns.

### Storage model

- Keep durable scheduler state in Postgres for now.
- Reduce synchronous write count on the fetch path.
- Prefer append-only or batched writes for history and metrics where possible.
- Keep admin-oriented read models derived from primary state, not part of fetch execution.

## Migration phases

### Phase 1: Instrument and simplify

Status: partially done.

- `readiness`, blocked-domain breakdown, and runtime snapshots are in place.
- Queue / quarantine / daemon policy responsibilities are split into dedicated modules.
- Domain latency EWMA, slow-host reporting, and host-budget observability are in place.
- Remaining work: make latency feed more of the actual scheduler policy, not just tie-breaking and observability.

### Phase 2: Split fetch from parse/publish

Status: partially done.

- Success path stage boundaries are now explicit: `fetch -> parse -> finalize -> persist`.
- Finalize-side DB mutations no longer run on the main event loop.
- Durable failure / skip mutations also run on the finalize side now.
- Remaining work is to make failure handling fully symmetric with the staged model and shrink worker-side scheduler bookkeeping further.

### Phase 3: Add latency-aware scheduling

Status: partially done.

- Current policy is still generic and mostly host-agnostic, but it is no longer purely backoff-driven.
- Remaining work is to make latency a stronger first-class input for leasing, concurrency, and maintenance decisions.

### Phase 4: Revisit recrawl and backlog policy

Status: partially done.

- Backlog deferral and retry quarantine now exist.
- Recrawl and backlog policy still need re-tuning once the hot path is cleaner.

## Migration steps from the current runtime

Status: mostly complete.

Current position:

- `discovered` exists outside scheduler membership.
- `admission` is a separate responsibility.
- lease strategy is explicit (`host_first` / `url_order`).
- `RunnableHostHead` exists as the host-first read model.
- `runnable_surface` is available in lease, readiness, and stats entrypoints.
- conceptual `refresh` intent is separated from the physical `recrawl` queue name.
- public/runtime-facing `ready` terminology has largely been replaced by `runnable`.
- public/model-facing `frontier` terminology has largely been replaced by `scheduler` or `url_ledger`.

Migration steps and where they landed:

1. Replace remaining queue-class-priority decisions with `surface / intent` priority.
   - Largely done. Duplicate merge and most model-facing decisions now prioritize `surface / intent`.
2. Reduce queue-class-centric logic inside admission core.
   - Largely done. `CrawlTask` and `intent / runnable_surface` are now the primary admission input shape.
3. Helperize queue-backed SQL enumeration.
   - Mostly done. Physical queue joins and unions now go through shared helpers in the main runtime paths.
4. Make blocked / quarantine handling surface-aware.
   - Mostly done. Restored and promoted work is modeled as returning to `deferred` or `frontline` surfaces in model-facing code.
5. Codify `frontline / deferred / refresh` responsibilities in code.
   - In progress but far along. These surfaces are now first-class operational groupings in runtime code and stats.
6. Clarify the `scheduled` vs `runnable` boundary.
   - In progress. `runnable` is explicit in readiness/stats APIs, but some lower-level helper names and behaviors still need review.
7. Continue replacing the `backlog` concept with `deferred scheduled surface` in model-facing code.
   - In progress. Model-facing code is mostly there; physical/storage names still remain.
8. Fully settle `refresh` as an intent rather than a queue concept.
   - Mostly done above the storage layer. `recrawl` remains as a storage detail and schema name.
9. Cut out a clear physical queue adapter.
   - In progress. This is now mostly confined to storage-facing helpers and schema definitions, but not fully isolated yet.
10. Fix the model vocabulary as the long-term interface.
   - In progress and close to done in code, but docs and some internal variable names still lag.
   - State: `discovered / scheduled / runnable / leased / blocked / terminal`
   - Intent: `explore / refresh / retry`
   - Strategy: `host_first / url_order`
   - Surface: `frontline / deferred / refresh` only as operational grouping

## Remaining cleanup backlog

Model-facing cleanup still worth doing:

1. Continue removing `queue class` wording from model-facing docstrings and comments.
   - Keep it only in storage / adapter code.
2. Tidy public/runtime wording that still says `queue` where `surface` or `scheduler` is more accurate.
3. Rename the remaining runtime / local variables that still use `frontier` internally.
   - Examples: `CrawlerEngine.self.frontier`, `_finalizer_frontier`, and similar internal handles.
4. Update docs that still mention removed module names or older scheduler terminology.
   - Examples: `README.md` still refers to `frontier_observability.py` / `frontier_quarantine.py`.

Low-level / storage cleanup still worth doing:

1. Keep shrinking the places that directly touch `QUEUE_EXPLORATION / QUEUE_BACKLOG / QUEUE_RECRAWL`.
   - They should stay confined to storage-facing helpers and schema definitions.
2. Continue reducing direct dependence on concrete `scheduler_queue_*` table names outside storage-facing helpers.
   - The physical rename is now part of the runtime; remaining work is mostly scope control.
3. Reduce test dependence on physical queue names where the behavior is already covered at the `surface / intent / strategy` layer.
4. Re-check public stats and operator payloads for any remaining old naming after the code cleanup settles.

## Deferred

1. More aggressive queue hygiene only if live data still shows backlog distortion after the scheduler redesign.
2. Metadata-only handling for binary documents such as PDF instead of treating them as stored text.
