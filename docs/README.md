# Documentation

This directory contains focused policy documents. The root-level README remains the primary
runtime overview.

- [README.md](/home/dev/projects/web-crawler/README.md) — current project overview, runtime model, current queue stages, CLI/API usage, and deployment notes
- [plan.md](/home/dev/projects/web-crawler/plan.md) — target architecture, migration phases, and the current gap between runtime and target design
- [crawler-concepts.md](/home/dev/projects/web-crawler/docs/crawler-concepts.md) — abstract WWW crawler concepts, state-vs-intent separation, and naming guidance
- [crawler-concepts.ja.md](/home/dev/projects/web-crawler/docs/crawler-concepts.ja.md) — 抽象的な WWW crawler の概念整理、state と intent の分離、命名指針
- [system-architecture.md](/home/dev/projects/web-crawler/docs/system-architecture.md) — project-wide system decomposition between ideal principles and runtime implementation
- [system-architecture.ja.md](/home/dev/projects/web-crawler/docs/system-architecture.ja.md) — ideal principles と runtime 実装の間で project 全体をどう分解して捉えるか
- [discovered-representation.md](/home/dev/projects/web-crawler/docs/discovered-representation.md) — representation of `discovered` between durable ledger, scheduler membership, and operational admission surfaces
- [discovered-representation.ja.md](/home/dev/projects/web-crawler/docs/discovered-representation.ja.md) — `discovered` を durable ledger、scheduler membership、operational surface の間でどう表現するか
- [scheduler-state-model.md](/home/dev/projects/web-crawler/docs/scheduler-state-model.md) — scheduler state model, source-of-truth boundaries, and URL/host state invariants
- [scheduler-state-model.ja.md](/home/dev/projects/web-crawler/docs/scheduler-state-model.ja.md) — scheduler の状態モデル、正本境界、URL/host state の invariant
- [scheduler-execution.md](/home/dev/projects/web-crawler/docs/scheduler-execution.md) — scheduler execution strategy, host-first lease path, runtime read models, and hot-path constraints
- [scheduler-execution.ja.md](/home/dev/projects/web-crawler/docs/scheduler-execution.ja.md) — scheduler execution strategy、host-first lease path、runtime read model、hot-path 制約
- [CONTENT_POLICY.md](/home/dev/projects/web-crawler/docs/CONTENT_POLICY.md) — content handling, metadata-only resources, and extractors that are out of scope for now
