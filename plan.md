# web-crawler plan

This migration is complete.

The crawler now treats the scheduler model as:

- `ledger`
- `scheduler_state`
- `intent`
- `active_leases`
- `host_state`

The completed end state is:

- public and runtime-facing APIs are explained by `state` and `intent`
- `retry` is modeled as an intent, not as a queue name
- `blocked` is modeled as scheduler/host state, not as a queue flavor
- `active_leases` remains execution state
- `host_state` remains host state
- queue tables are internal physical projections for leasing and maintenance
- `scheduler_state_snapshot` is the primary runtime-facing scheduler state view
- `queue_class` no longer defines crawler meaning

Implementation notes:

- `url_ledger` keeps durable URL identity/history plus `current_intent`
- current scheduler state is derived from queue membership, active leases, and host state
- operator-facing state is exposed through `scheduler_state_snapshot`,
  `readiness_state_counts`, `effective_state_counts`, and `blocked_reason_counts`
- compatibility aliases may remain in runtime payloads, but they are derived from the
  snapshot view rather than acting as a separate source of truth

If future work starts from here, treat this file as a completion marker rather than as an
active backlog.
