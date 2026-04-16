# Crawler State Model

この文書は、将来のリファクタリングの指針となる practical な状態モデルを定義する。現在の実装をそのまま説明するものではないが、理想化された上位モデルをそのまま再掲するものでもない。クローラーが正しく scheduling 判断するために、どの状態を system 上の正本として持つべきかを示す。

上位原則は [crawler-model.ja.md](/home/dev/projects/web-crawler/docs/crawler-model.ja.md) に置く。

この文書は、その原則へ近づくための transition/convergence model として読むべきである。

## Goals

- scheduler の正本を小さく明示的に保つ
- 永続的な事実と現在の scheduling state を分離する
- 何が正本で何が派生値かを明確にする
- 同じ意味の状態を複数箇所に持たない
- 現在の crawler を理想モデルへ寄せるための practical な中間表現を置く

## Core Rule

1 つの URL は 1 つの durable ledger record を持ち、同時に持てる current scheduler membership は高々 1 つである。

`ready` は派生値であり、永続 state ではない。

## Durable State Groups

### 1. URL ledger

URL ledger は「この URL を知っているか」「この URL について最後に確定している durable な事実は何か」に答える。

ここに属するもの:

- normalized URL
- domain / host key
- discovery metadata
- first seen timestamp
- last success timestamp
- last failure timestamp
- terminal になった場合の final outcome

ここに scheduler truth として置くべきでないもの:

- pending
- leased
- queue membership
- quarantine membership

ledger は履歴と identity を持つ場所であり、live scheduler そのものではない。

### 2. Scheduler membership

scheduler は「この URL を今どう扱うか」に答える。

最小の live states:

- `discovered`
- `runnable`
- `leased`
- `quarantined`
- `done`
- `failed`

意味:

- `discovered`: 知ってはいるが、まだ通常の leasing 候補ではない
- `runnable`: 通常の leasing 候補にしてよい
- `leased`: 現在 worker が所有している
- `quarantined`: 通常の leasing から意図的に外している
- `done`: terminal success state
- `failed`: terminal failure state

これらの state は scheduler 自身が正本として持つべきであり、ledger 側で二重に保持するべきではない。

### 3. Host state

host scheduler は「この host に今触れてよいか」「どの程度の aggressiveness で触れてよいか」に答える。

最小の host state:

- `next_request_at`
- `backoff_until`
- `latency_ewma_ms`
- `fail_streak`
- `inflight_budget`

これは URL 単位ではなく host 単位の state である。

## Derived Values

次の値は有用だが primary state ではない。

- `ready`
- `pending_total`
- `blocked_host_backoff`
- `blocked_domain_next_request`
- `pages_per_second`
- top pending / blocked domain tables

`ready` は次から導出される:

- `runnable` membership
- active lease が無いこと
- host state 的に今実行可能であること

`ready` を primary state として保存すると drift する。

## State Transitions

### URL transitions

通常経路:

1. `discovered -> runnable`
2. `runnable -> leased`
3. `leased -> done`

失敗経路:

1. `leased -> quarantined`
2. `leased -> failed`

再試行経路:

1. `quarantined -> runnable`

再クロール経路:

1. `done -> discovered`

## Invariants

常に成立すべき条件:

1. 1 つの URL が同時に `runnable` と `leased` にあってはならない
2. 1 つの URL が同時に `runnable` と `quarantined` にあってはならない
3. `done` URL は live scheduler queue に存在してはならない
4. `failed` URL は live scheduler queue に存在してはならない
5. `leased` URL には必ず active lease record が存在しなければならない
6. `ready` は scheduler membership と host state から導出できなければならない
7. current scheduler state の単一正本は queue membership でなければならない

## What Seeds Are

seed は bootstrap 用の input set にすぎない。

長期運用の scheduler category ではない。

一度 system に入ったら、seed 由来 URL も他の URL と同じ discovered-to-runnable ルールで扱うべきである。

## Target Interpretation For Current Concepts

現在の概念は次の意味へ収束させるべきである。

- `backlog` => discovered pool
- `exploration` => runnable pool
- `frontier_lease_active` => lease state
- blocked-domain-backoff queue => quarantine pool
- host scheduler tables => host state

これは convergence target であり、現在の実装がすでにそうなっているという意味ではない。

## Immediate Design Consequences

1. URL ledger は scheduler の current-state truth であることをやめるべきである
2. live scheduler state の唯一の正本は queue membership になるべきである
3. `ready` は派生値のままにするべきである
4. bootstrap は通常の exploration supply から分離するべきである
5. seed 由来の特別扱いは scheduler から消えるべきである
