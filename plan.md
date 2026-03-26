# web-crawler plan

## Current state

- Explicit schema migrations are in place and required before app startup.
- Runtime frontier backward compatibility has been removed.
- The malformed `href` parser crash has been fixed.
- Production seeds currently exclude `www.icann.org`.
- Production recrawl TTL is currently set to 30 days to avoid stale backlog churn.

## Immediate priorities

1. Reduce queue pollution from dead URLs.
   - Demote or suppress repeatedly failing URLs earlier.
   - Avoid spending cycles on long runs of known-dead historical backlog.

2. Tighten operational visibility.
   - Add simple rate and error counters that distinguish success, 4xx, 5xx, timeout, and connection failures.
   - Make it easier to see why crawl throughput drops.

3. Finish documentation cleanup.
   - Keep README aligned with compose, migrations, and production env defaults.
   - Document the current production assumptions in one place.

## Near-term cleanup

1. Revisit recrawl policy after queue quality improves.
2. Add a small runbook for safe queue resets and seed changes.
3. Add queue hygiene rules so stale low-value backlog does not dominate active crawl time.

## Done recently

- Added explicit `crawler migrate`.
- Updated compose to run migrations before `api` and `crawler`.
- Dropped frontier legacy compatibility.
- Fixed malformed anchor extraction.
- Removed `icann` from active production seeds.
