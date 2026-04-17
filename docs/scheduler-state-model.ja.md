# Scheduler State Model

この文書は、将来のリファクタリングの指針となる practical な状態モデルを定義する。現在の実装をそのまま説明するものではないが、理想化された上位モデルをそのまま再掲するものでもない。クローラーが正しく scheduling 判断するために、どの状態を system 上の正本として持つべきかを示す。

上位原則は [crawler-principles.ja.md](/home/dev/projects/web-crawler/docs/crawler-principles.ja.md) に置く。

この文書は、その原則へ近づくための transition/convergence model として読むべきである。

## Goals

- scheduler の正本を小さく明示的に保つ
- 永続的な事実と現在の scheduling state を分離する
- 何が正本で何が派生値かを明確にする
- 同じ意味の状態を複数箇所に持たない
- 現在の crawler を理想モデルへ寄せるための practical な中間表現を置く

## Core Rule

1 つの URL は 1 つの durable ledger record を持ち、同時に持てる current scheduler membership は高々 1 つである。

operator/readiness view に出る `runnable` は派生値であり、別個の durable state として保存すべきではない。

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
- `scheduled`
- `runnable`
- `leased`
- `blocked`
- `terminal`

意味:

- `discovered`: 知ってはいるが、まだ通常の scheduler membership に入っていない
- `scheduled`: scheduler membership には入ったが、まだ runnable ではない
- `runnable`: scheduler membership にあり、今 lease してよい
- `leased`: 現在 worker が所有している
- `blocked`: host/backoff/quarantine 条件により一時的に runnable leasing から外れている
- `terminal`: success / failure を含む終端状態

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

- `runnable`（readiness / operator view 上の派生値）
- `pending_total`
- `blocked_host_backoff`
- `blocked_domain_next_request`
- `pages_per_second`
- top pending / blocked domain tables

operator-facing な `runnable` view は次から導出される:

- scheduler membership
- active lease が無いこと
- host state 的に今実行可能であること

この派生 `runnable` view を別の primary state として保存すると drift する。

## State Transitions

### URL transitions

通常経路:

1. `discovered -> scheduled`
2. `scheduled -> runnable`
3. `runnable -> leased`
4. `leased -> terminal`

失敗経路:

1. `leased -> blocked`
2. `leased -> terminal`

再試行経路:

1. `blocked -> runnable`

再クロール経路:

1. `terminal -> discovered`

## Invariants

常に成立すべき条件:

1. 1 つの URL が同時に `runnable` と `leased` にあってはならない
2. 1 つの URL が同時に `runnable` と `blocked` にあってはならない
3. `terminal` URL は live scheduler queue に存在してはならない
4. `discovered` URL が通常の scheduler membership に同時に入っていてはならない
5. `leased` URL には必ず active lease record が存在しなければならない
6. operator-facing な `runnable` view は scheduler membership と host state から導出できなければならない
7. current scheduler state の単一正本は queue membership でなければならない

## What Seeds Are

seed は bootstrap 用の input set にすぎない。

長期運用の scheduler category ではない。

一度 system に入ったら、seed 由来 URL も他の URL と同じ discovered-to-runnable ルールで扱うべきである。

## Target Interpretation For Current Concepts

現在の概念は次の意味へ収束させるべきである。

- `exploration` => frontline runnable surface
- `backlog` => deferred scheduled surface
- `active_leases` => lease state
- blocked-domain-backoff queue => quarantine pool
- host scheduler tables => host state

これは convergence target であり、現在の実装がすでにそうなっているという意味ではない。

概念上の分離は naming の整理より先に進めるべきである。

- まず state と intent を設計上で分ける
- 既存名はその分離を導入する間の過渡名として残ってよい
- `backlog` を `discovered` の意味に過負荷してはいけない
- lane は必要なら置く operational grouping であり、primary scheduler concept ではない

## Immediate Design Consequences

1. URL ledger は scheduler の current-state truth であることをやめるべきである
2. live scheduler state の唯一の正本は queue membership になるべきである
3. operator-facing な `runnable` view は派生値のままにするべきである
4. bootstrap は通常の exploration supply から分離するべきである
5. seed 由来の特別扱いは scheduler から消えるべきである
6. worker lane を持つとしても、state と strategy から導かれる下位概念に留めるべきである
