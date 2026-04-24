# web-crawler plan

## Current milestone: production observation and bounded growth

The crawler is now running with bounded page-content storage, bounded discovery admission,
Postgres-backed scheduler queues, host runnable-head read models, and split crawl pipeline stages.
The current milestone is to keep production behavior understandable while deciding the next speed
and growth-control work from repeatable observations rather than one-off manual inspection.

Current priorities:

- keep the crawler scoped to broad public-web discovery, not a site-specific corpus
- keep `pages` as a lightweight page index and `page_content` as bounded text storage
- keep extracted outlinks separate from admitted scheduler outlinks
- keep scheduler queue membership as the live URL source of truth
- keep `host_runnable_heads` as a derived execution read model, not durable URL state
- keep runtime stats cheap enough for `/stats` and detailed diagnostics out of the hot path
- use `crawler observe` for repeatable production snapshots before changing speed or growth policy

## Current operating model

Runtime flow:

```text
lease -> fetch -> parse -> finalize -> persist
```

Responsibilities:

- `fetch` obtains responses while respecting host pacing and fetch admission.
- `parse` extracts content and discovered links.
- `finalize` applies scheduler mutations, discovery admission, host success/failure state, and URL
  completion/failure transitions.
- `persist` writes page metadata/content and optional JSONL output.

Primary operator surfaces:

- `GET /stats` for fast runtime snapshots
- `GET /stats/diagnostics` for runtime-only diagnostics metadata
- `crawler observe` for a repeatable read-only production observation from PostgreSQL

## Recently completed

- Added bounded page-content storage through `page_content` and storage tiers.
- Added discovery admission caps and telemetry for extracted/admitted/rejected outlinks.
- Added batched success finalization and batched page persistence.
- Split pipeline queue/liveness/timing ownership into runtime and service boundaries.
- Moved scheduler lease selection, admission, requeue, retry policy, and host-head maintenance into
  named services while preserving public behavior.
- Added `crawler observe`:
  - summarizes crawl totals, scheduler readiness, throughput, backpressure, storage tiers, outlink
    admission ratio, URL ledger size, and relation sizes
  - supports human-readable output and `--json`
  - performs read-only database inspection

## Verification baseline

Before making code changes:

```bash
./.venv/bin/ruff check src tests
./.venv/bin/pytest -q
```

Before production interpretation, capture a fresh observation:

```bash
docker compose run --rm api crawler observe
docker compose run --rm api crawler observe --json
```

The observation should be read alongside `/stats` and service logs. Treat one sample as a point-in
time measurement, not as proof of a durable trend.

## Next candidates

### 1. Publisher and persistence throughput

Investigate whether the publisher path is still the next bottleneck after batched persistence.
Use `crawler observe`, `/stats`, and timing summaries to decide whether to:

- increase publisher worker count
- tune publisher batch size / wait time
- reduce `page_content` write cost for large bodies
- adjust queue maxsize together with publisher lane count

Do not tune publisher concurrency without checking database write latency and publish queue depth
in the same observation window.

### 2. Finalizer backpressure

If `finalize_queue_wait_ms` dominates while publish pressure stays low, inspect finalizer operation
timings before changing scheduler logic. Candidate areas:

- discovery admission batching
- host success/failure bulk updates
- URL completion/failure batch mutation
- dirty host-head refresh behavior after queue membership changes

### 3. Frontier and storage growth control

If URL ledger growth or stored content growth is too fast for the intended broad-web crawl, tune
generic policy before adding allowlists:

- lower `max_discovered_urls_per_page`
- lower `max_discovered_urls_per_target_host_per_page`
- raise `min_discovery_value`
- raise `low_value_archetype_min_discovery_value`
- consider host-level or registrable-domain-level budgets only after URL-level and per-page caps are
  shown insufficient

### 4. Documentation and operator workflow

Keep README and docs aligned with runtime behavior:

- `README.md` remains the install/CLI/API/deployment entry point
- `docs/` remains the design and policy reference
- this `plan.md` remains the concise current milestone and next-candidate document

## Guardrails

- Do not infer product scope from the current seed set or corpus.
- Do not reintroduce unbounded page-body storage.
- Do not admit every extracted outlink automatically.
- Do not make live full-queue diagnostics part of the normal `/stats` path.
- Do not treat `host_runnable_heads` as a second durable source of truth.
- Do not make speed changes without a before/after observation sample.
