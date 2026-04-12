# web-crawler plan

## Current state

Already done:

- `frontier` is no longer the only scheduler surface; physical queue tables are the working scheduler state.
- Active leases are isolated in `frontier_lease_active`.
- Host-cooled URLs are physically isolated in `frontier_queue_blocked_domain_backoff`.
- Retry quarantine is restored through a small retry budget instead of bulk re-entry.
- `/stats` exposes `readiness` with `ready`, `scheduled`, `blocked_domain_next_request`, `blocked_host_backoff`, and `retry_quarantine`.

Still not done:

- Fetch / parse / publish are still too coupled inside one crawl worker path.
- Scheduler policy is not yet latency-aware.
- `top_blocked_domains` still uses older wording and should be split to match the new state model more directly.
- Postgres is still carrying too many synchronous responsibilities on the hot path.

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

1. Lease a ready URL from the frontier.
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

- Writes parsed content and metadata.
- Admits outlinks back into the frontier.
- Exposes results to APIs or downstream consumers.

#### Stage 5: Maintenance

- Recrawl TTL handling
- stale queue cleanup
- read models
- aggregated stats
- runbooks and operational tasks

These must not block normal fetch throughput.

### Scheduling model

- Per-domain concurrency must be dynamic, not fixed forever.
- Slow domains should be automatically downweighted.
- Fast domains should consume more idle capacity.
- Retry policy should be fail-fast and requeue-based.
- Robots transport failure and robots policy deny should remain separate concerns.

### Storage model

- Keep durable frontier state in Postgres for now.
- Reduce synchronous write count on the fetch path.
- Prefer append-only or batched writes for history and metrics where possible.
- Keep admin-oriented read models derived from primary state, not part of fetch execution.

## Migration phases

### Phase 1: Instrument and simplify

Status: partially done.

- `readiness`, blocked-domain breakdown, and runtime snapshots are in place.
- Queue state is more explicit than before, but domain latency is still not a first-class scheduler input.
- Remaining work: per-domain latency distributions and policy feedback, not just visibility.

### Phase 2: Split fetch from parse/publish

Status: not done.

- This remains the biggest structural change still ahead.

### Phase 3: Add latency-aware scheduling

Status: not done.

- Current policy is generic and host-agnostic, but it is still mostly backoff-driven rather than latency-driven.

### Phase 4: Revisit recrawl and backlog policy

Status: partially done.

- Backlog deferral and retry quarantine now exist.
- Recrawl and backlog policy still need re-tuning once the hot path is cleaner.

## Deferred

1. More aggressive queue hygiene only if live data still shows backlog distortion after the scheduler redesign.
2. Metadata-only handling for binary documents such as PDF instead of treating them as stored text.
