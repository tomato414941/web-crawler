# web-crawler plan

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

- Measure request latency, failure rate, and slot occupancy by domain.
- Remove hot-path work that is only for operator visibility.
- Confirm which synchronous writes still happen during fetch.

### Phase 2: Split fetch from parse/publish

- Introduce a fetch result handoff boundary.
- Stop having a single worker own fetch, parse, and storage in sequence.
- Preserve current behavior behind the new boundaries first.

### Phase 3: Add latency-aware scheduling

- Use rolling per-domain latency and failure signals.
- Reduce concurrency or priority for slow domains automatically.
- Increase fairness without any domain-specific hard-coded rules.

### Phase 4: Revisit recrawl and backlog policy

- Tune recrawl TTL after throughput stabilizes.
- Rework backlog deferral based on measured scheduler behavior instead of defensive defaults.

## Deferred

1. More aggressive queue hygiene only if live data still shows backlog distortion after the scheduler redesign.
2. Metadata-only handling for binary documents such as PDF instead of treating them as stored text.
