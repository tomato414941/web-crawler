# web-crawler plan

## Active priorities

1. Verify live crawl throughput after the latest queue and discovery changes.
   - Confirm cycle completion times and pages/s on production.
   - Check whether `www.rfc-editor.org` backlog still dominates active crawl time.
   - Watch `/stats` and daemon error breakdowns for new failure patterns.

2. Revisit recrawl policy once live queue quality is stable.
   - Decide whether 30-day recrawl TTL is still appropriate.
   - Tune stale-page requeueing based on measured throughput instead of defensive defaults.

3. Add a small runbook for operational queue changes.
   - Document safe queue resets.
   - Document how to change seeds without reintroducing dead backlog.

## Deferred

1. Add more queue hygiene only if live metrics show stale or dead backlog still dominating crawl time.
2. Implement metadata-only handling for binary documents such as PDF instead of treating them as stored text.
