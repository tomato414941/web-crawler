# Documentation

This directory contains focused design, policy, and operations documents. The root README remains
the primary entry point for project summary, quick start, common commands, and document routing.

## Entry Points

- [README.md](../README.md) — project overview, quick start, common commands, and document routing
- [plan.md](../plan.md) — current milestone, recent completed work, and next candidates
- [DESIGN_PRINCIPLES.md](DESIGN_PRINCIPLES.md) — conceptual positioning, core principles, non-goals, and architectural direction
- [CONTENT_POLICY.md](CONTENT_POLICY.md) — content handling, metadata-only resources, storage tiers, and discovery breadth
- [api.md](api.md) — REST API usage, authentication, endpoints, and current limitations
- [operations.md](operations.md) — deployment, hardened runtime expectations, scheduler tuning, and observation
- [seed-catalog.md](seed-catalog.md) — seed catalog maintenance and runtime rendering
- [AGENT_BOUNDARY.md](AGENT_BOUNDARY.md) — experimental AI browser agent boundary and constraints
- [security/egress.md](security/egress.md) — threat model, outbound network policy, and containment expectations

## Design Documents

- [crawler-concepts.md](crawler-concepts.md) / [crawler-concepts.ja.md](crawler-concepts.ja.md) — abstract WWW crawler concepts, state-vs-intent separation, fetch admission, and naming guidance
- [system-architecture.md](system-architecture.md) / [system-architecture.ja.md](system-architecture.ja.md) — project-wide subsystem boundaries between ideal principles and runtime implementation
- [scheduler-state-model.md](scheduler-state-model.md) / [scheduler-state-model.ja.md](scheduler-state-model.ja.md) — scheduler state model, source-of-truth boundaries, and URL/host invariants
- [scheduler-execution.md](scheduler-execution.md) / [scheduler-execution.ja.md](scheduler-execution.ja.md) — scheduler execution strategy, host-first lease path, runtime read models, and hot-path constraints
- [discovered-representation.md](discovered-representation.md) / [discovered-representation.ja.md](discovered-representation.ja.md) — representation of `discovered` between the durable ledger, scheduler membership, and admission surfaces
