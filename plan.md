# web-crawler plan

## Current milestone: production observation and bounded growth

The crawler is now running with bounded page-content storage, bounded discovery admission,
Postgres-backed scheduler queues, host runnable-head read models, split crawl pipeline stages, and
application egress guards backed by a documented runtime containment path. The current milestone is
to keep production behavior understandable while deciding the next speed, growth-control, and
egress-runtime work from repeatable observations rather than one-off manual inspection.

Current priorities:

- keep the crawler scoped to broad public-web discovery, not a site-specific corpus
- keep `pages` as a lightweight page index and `page_content` as bounded text storage
- keep extracted outlinks separate from admitted scheduler outlinks
- keep scheduler queue membership as the live URL source of truth
- keep `host_runnable_heads` as a derived execution read model, not durable URL state
- keep runtime stats cheap enough for `/stats` and detailed diagnostics out of the hot path
- use `crawler observe` for repeatable production snapshots before changing speed or growth policy
- keep public-web egress explicit: application URL checks are required, but production safety also
  depends on firewall, container policy, or the hardened proxy profile

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
- `crawler scheduler-check` for read-only scheduler invariant checks
- `scripts/egress_smoke.py` and the hardened compose smoke profile for runtime egress checks

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
- Added scheduler invariant checks and repairs for terminal URL memberships, host-head drift, and
  URL identity issues.
- Added URL ledger identity tracking and tests for duplicate identity risks.
- Hardened public-web egress handling:
  - application egress policy rejects unsafe schemes, ports, private/local/link-local/metadata
    targets, unsafe DNS answers, userinfo, and legacy IPv4 forms
  - direct egress smoke documents the current private deployment boundary
  - hardened compose profile routes crawler HTTP/HTTPS through an explicit Squid proxy
  - tests document the direct HTTP DNS time-of-check / time-of-use boundary

## Latest production baseline

Captured on 2026-05-10 from the private Docker Compose deployment on
`dev@100.92.121.94`.

- services: `api`, `crawler`, `observer`, and `postgres` running
- git: production checkout on `main`, aligned with `origin/main`
- crawl totals: 268,272 pages across 137,954 hosts
- storage: 16.0 GiB stored content from 109.4 GiB raw content
- scheduler: about 899,743 pending URLs, almost all runnable, no active leases
- URL ledger: about 1.27M URLs across 188,910 hosts
- admission: `drain` mode, target pending 500,000, admission pending about 1.02M
- latest admission sample: 3-7% admit ratio, mostly rejected by score threshold and
  per-target-host caps
- queue pressure: parse/finalize/publish queues shallow during the sample window
- scheduler check: `ok=true`, zero invariant violations
- egress smoke: passed for current direct-egress deployment; public HTTP remained reachable and
  representative local/private/link-local/metadata/CGNAT/benchmark targets did not connect

Interpretation: scheduler integrity and runtime egress posture look healthy. The next useful work
is to explain why pending remains far above target and whether the current drain-mode admission
policy is reducing it at the intended rate before making throughput or discovery-policy changes.

Follow-up read-only drain analysis from the same deployment:

- observer trend over the latest 24.15 hours: pages +351, scheduler pending -16, admission pending
  -16; average reported throughput about 9 pages/sec, but durable page growth is much lower
- pending storage surface: `scheduler_queue_scheduled` holds about 1.02M live rows and 1.2 GiB;
  this is live data, not table bloat
- `scheduler_queue_runnable` has only a few rows; `host_runnable_heads` is the execution read model
  with about 123k host heads and about 900k summed runnable URLs
- top pending hosts are dominated by generic high-degree platforms and scholarly/developer hosts:
  `github.com`, `www.youtube.com`, `www.instagram.com`, `arxiv.org`, `www.linkedin.com`, and
  `en.wikipedia.org`
- top registrable domains include `github.com`, `youtube.com`, `github.io`, `wikipedia.org`,
  `instagram.com`, `google.com`, `arxiv.org`, `blogspot.com`, `co.uk`, and `wordpress.com`
- scheduled score distribution: about 646k rows at or above the current 1.15 drain threshold,
  about 54k rows in [1.10, 1.15), and about 317k rows below 1.10
- URL length is not the main issue: about 984k scheduled URLs are under 128 bytes
- host concentration is moderate: 14 host heads have at least 1,000 runnable URLs, accounting for
  about 74.5k URLs; 183 host heads have at least 100, accounting for about 116.7k URLs
- scheduled terminal rows: 0; scheduled retry-intent rows: about 17.8k

Interpretation: the backlog is not an integrity failure or a storage bloat artifact. It is mostly
a broad live frontier with many due scheduled URLs. The next policy question is whether drain mode
should become more selective for high-degree platform links and broad domain fan-out, or whether the
runtime should increase effective crawl throughput after confirming the low durable page-growth
rate is expected.

Durable throughput accounting follow-up on 2026-05-11:

- last 24h distinct page rows: 367 newly created, 812 crawled/updated, 445 existing rows recrawled
- recent daemon cycles are completing 300 fetches per cycle, so reported `pages_per_second` is cycle
  fetch throughput, not net-new durable page growth
- production scheduler still contained about 17.1k scheduled rows whose ledger rows already had
  `last_success_at`, and about 6.7k host-head rows pointing at previously successful URLs
- code inspection found the normal discovered-task admission path did not filter
  `last_success_at IS NULL`, so already successful URLs could be reintroduced by later discovery
  instead of only by the explicit refresh path
- local fix in progress: normal discovery upsert/admission now skips successful ledger rows; refresh
  should continue to use `requeue_refresh_urls`
- deployed fix on 2026-05-12 as `debf1f6 fix: avoid rediscovering successful urls`; health,
  `crawler observe`, and `scheduler-check --sample-limit 0` passed after deploy
- post-deploy existing queue residue: about 17.0k scheduled rows and 6.5k host-head rows still point
  at previously successful URLs; the code fix prevents new normal-discovery reintroduction, but a
  separate repair/cleanup decision is needed for existing scheduler membership rows

Interpretation: the throughput/page-growth gap is mostly an accounting and repeated-fetch issue,
not evidence that the crawler is idle. The immediate fix is to stop normal discovery from
rescheduling successful URLs, then redeploy and compare the scheduled-successful count and durable
page growth before changing discovery thresholds or concurrency.

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
docker compose run --rm api crawler scheduler-check --sample-limit 0
```

The observation should be read alongside `/stats` and service logs. Treat one sample as a point-in
time measurement, not as proof of a durable trend.

Before changing deployment egress policy, run the relevant runtime smoke:

```bash
docker compose run --rm --no-deps crawler python scripts/egress_smoke.py
docker compose -f docker-compose.yml -f docker-compose.hardened.yml \
  --profile egress-smoke run --rm egress-smoke
```

## Next candidates

### 1. Production observation baseline

Capture a fresh deployment snapshot before tuning throughput or growth, then compare it with the
2026-05-10 baseline above. The next decision should come from the same observation window:

- [x] `crawler observe` and `crawler observe --json`
- [x] `/stats` and `/stats/diagnostics`
- [x] `crawler scheduler-check --sample-limit 0`
- [x] recent `api`, `crawler`, and `observer` logs
- [x] direct egress smoke for the active deployment profile
- [ ] repeat the same snapshot after any tuning change

Record whether the dominant constraint is publisher pressure, finalizer pressure, scheduler
readiness, host pacing, storage growth, runtime egress policy, or pending-drain rate.

### 2. Pending and admission drain analysis

Pending is still well above `CRAWLER_ADMISSION_TARGET_PENDING`. Before changing speed or discovery
thresholds, inspect the durable scheduler surfaces and admission shape:

- [x] top pending hosts and registrable domains
- [x] pending URL length and path-depth distribution
- [x] whether pending is flat, rising, or draining across observer snapshots
- [x] whether large relation sizes such as `scheduler_queue_scheduled` are live rows or table/index
  bloat
- [ ] whether drain-mode caps are too permissive for broad-web growth, or whether crawl throughput is
  simply below the current discovery inflow

Prefer read-only SQL and existing observer JSONL records. Do not add allowlists or topic-specific
filters to solve broad growth pressure.

Current next step: explain the gap between reported cycle throughput and durable page growth, then
choose one conservative growth-control change to test. Candidate policy directions are:

- raise drain-mode `min_score` above 1.15 when pending is more than 2x target
- lower drain-mode per-page or per-target-host caps for high-degree pages
- add a generic per-registrable-domain budget or decay mechanism for very high fan-out domains
- keep throughput unchanged until durable page-growth accounting is understood

### 3. Durable throughput accounting

The observer reports about 9 pages/sec, but the 24-hour durable `pages` count rose by only 351.
Before tuning concurrency, determine whether this is expected recrawl/upsert behavior, repeated
fetching of already-known URLs, or a stats/accounting mismatch.

Inspect:

- [x] daemon cycle completion logs versus `pages` table insert/update behavior
- [x] ratio of newly inserted pages to updated pages in persistence
- [x] whether `cycle_pages` counts successful fetches rather than net-new stored pages
- [x] whether current seeds/frontier revisit already stored URLs too often
- [x] run Postgres-backed regression tests for the successful-URL rescheduling fix
- [x] deploy the successful-URL rescheduling fix and observe before/after
- [ ] decide whether to repair existing successful URL scheduler memberships

### 4. Egress runtime posture

The application guard is in place, but direct `httpx` transport still connects by hostname after
the DNS guard has approved an answer. Decide whether the current private deployment remains on
direct HTTP plus host firewall, or moves to the hardened proxy profile.

Before changing this, verify:

- [ ] the current `DOCKER-USER` / host firewall rule is still active
- [x] direct smoke still blocks link-local / metadata targets while public HTTP works
- [ ] hardened compose config validates with the production environment
- [ ] hardened smoke passes if the proxy profile is selected
- [ ] decide whether to keep direct HTTP plus firewall or move to the hardened proxy profile

Do not set `CRAWLER_ALLOW_PRIVATE_NETWORK_EGRESS=true` in production.

### 5. Publisher and persistence throughput

Investigate whether the publisher path is still the next bottleneck after batched persistence.
Use `crawler observe`, `/stats`, and timing summaries to decide whether to:

- [ ] decide whether publisher pressure is actually the next bottleneck
- [ ] increase publisher worker count if supported by DB write latency and queue depth
- [ ] tune publisher batch size / wait time if queue wait remains high
- [ ] reduce `page_content` write cost for large bodies if storage timing dominates
- [ ] adjust queue maxsize together with publisher lane count

Do not tune publisher concurrency without checking database write latency and publish queue depth
in the same observation window.

### 6. Finalizer backpressure

If `finalize_queue_wait_ms` dominates while publish pressure stays low, inspect finalizer operation
timings before changing scheduler logic. Candidate areas:

- [ ] decide whether finalizer pressure is actually the next bottleneck
- [ ] discovery admission batching
- [ ] host success/failure bulk updates
- [ ] URL completion/failure batch mutation
- [ ] dirty host-head refresh behavior after queue membership changes

### 7. Frontier and storage growth control

If URL ledger growth or stored content growth is too fast for the intended broad-web crawl, tune
generic policy before adding allowlists:

- [ ] choose one conservative growth-control change after throughput accounting
- [ ] lower `max_discovered_urls_per_page`
- [ ] lower `max_discovered_urls_per_target_host_per_page`
- [ ] raise `min_discovery_value`
- [ ] raise `low_value_archetype_min_discovery_value`
- [ ] consider host-level or registrable-domain-level budgets only after URL-level and per-page caps are
  shown insufficient

### 8. Documentation and operator workflow

Keep README and docs aligned with runtime behavior:

- [ ] keep `README.md` as the install/CLI/API/deployment entry point
- [ ] keep `docs/` as the design and policy reference
- [x] keep this `plan.md` as the concise current milestone and next-candidate document

## Remaining checklist

- [x] Refresh production observation baseline.
- [x] Verify scheduler invariants.
- [x] Verify current direct-egress smoke.
- [x] Confirm pending surface is live data, not storage bloat.
- [x] Identify top pending hosts/domains and basic URL shape.
- [x] Explain reported throughput versus durable page growth.
- [x] Finish and verify the successful-URL rescheduling fix.
- [ ] Decide whether to clean existing successful URL scheduler memberships.
- [ ] Decide whether pending drain needs policy tightening or more effective throughput.
- [ ] If policy tightening is needed, choose one generic growth-control change.
- [ ] If throughput is the blocker, tune publisher/finalizer only after DB timing confirms it.
- [ ] Decide whether the deployment should stay on direct egress plus firewall or move to hardened
  proxy egress.
- [ ] Repeat baseline after any runtime or policy change and record the before/after result.

## Guardrails

- Do not infer product scope from the current seed set or corpus.
- Do not reintroduce unbounded page-body storage.
- Do not admit every extracted outlink automatically.
- Do not make live full-queue diagnostics part of the normal `/stats` path.
- Do not treat `host_runnable_heads` as a second durable source of truth.
- Do not make speed changes without a before/after observation sample.
- Do not treat application egress checks as the only production containment boundary.
- Do not weaken egress safety to make a smoke test pass; fix the runtime policy instead.
