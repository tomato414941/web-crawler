# web-crawler plan

## Current milestone: host ledger foundation

The current priority is to add the durable host identity/history layer that would have existed in a
greenfield host-first crawler. This does not replace the lease hot-path work, but it clarifies host
responsibilities before adding more scheduler read models.

The documentation split is:

- `crawler-concepts`: abstract model and naming principles
- `scheduler-state-model`: source-of-truth boundaries and invariants
- `scheduler-execution`: runtime lease strategy, read models, and hot-path constraints
- `system-architecture`: project-wide subsystem boundaries

## Active work

- Add a new `host_ledger` table for durable host identity/history.
- Keep `host_state` as runtime scheduling state, not host identity.
- Record host discovery from URL ledger insertion.
- Record host success/failure history from crawl completion.
- Record robots check summary without putting robots parser details into the ledger.

## Not in this slice

- No `host_state` rename.
- No production configuration change.
- No `host_ready` execution read model.
- No lease hot-path query rewrite.

## Acceptance

- `host_ledger` exists on new and migrated databases.
- URL discovery updates host first/last seen and known URL counts.
- crawl success/failure updates host history counters.
- robots checks update a compact host-level robots summary.
- normal lease execution does not depend on `host_ledger`.
