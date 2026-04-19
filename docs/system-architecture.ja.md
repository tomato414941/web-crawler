# Web-Crawler System Architecture

この文書は、理想化された crawler model と、最終的な runtime 実装の間に置く中間文書である。

これは [crawler-concepts.ja.md](/home/dev/projects/web-crawler/docs/crawler-concepts.ja.md) より広く、
[scheduler-state-model.ja.md](/home/dev/projects/web-crawler/docs/scheduler-state-model.ja.md) よりも project 全体の
責務分解を扱う。

役割は、`web-crawler` という project 全体を system として見たときに、最終的にどの subsystem
へ分解されるべきか、その責務を説明することである。

## Purpose

この文書が答えたい practical な問いは次である。

crawler model が理想形だとすると、`web-crawler` 全体は最終的にどの subsystem に分かれ、
それぞれが何を所有するべきか。

## System Layers

project 全体としては、crawler は次の layer へ収束していくべきである。

1. URL ledger
2. Discovery and admission
3. Scheduler membership
4. Execution and lease ownership
5. Host state
6. Fetch / parse / finalize / persist pipeline
7. Read models and operator surfaces
8. Bootstrap and seed management

runtime 上で table や code path をまだ共有していても、概念上はこれらを分離できるべきである。

## 1. URL Ledger

URL ledger は durable な URL identity と durable な URL history を持つ。

答えるべき問い:

- この URL を知っているか
- normalized URL identity は何か
- 最後に確定した durable outcome は何か
- 最初と最後の重要イベント時刻はいつか

持つべきでないもの:

- queue membership
- runnable truth
- active lease ownership
- host pacing

ledger は durable fact であり、live scheduling truth ではない。

## 2. Discovery And Admission

Discovery and admission は、「URL を見つけた」から「scheduler が考慮してよい」へ進める責務を持つ。

ここに含まれるもの:

- discovered URL intake
- scheduler admission 前の duplicate suppression
- 初期 policy shaping
- host-aware / budget-aware な live scheduler surface への admission

概念的に `discovered` が属するのはここである。

`discovered` の意味は:

- system はその URL を知っている
- まだ通常の scheduler membership は与えられていない

であり、自動的に `backlog` と同一視するべきではない。

## 3. Scheduler Membership

Scheduler membership は、URL を今 live にどう扱うかを持つ。

答えるべき問い:

- この URL は今 normal scheduling に参加しているか
- 参加しているなら、どの live surface に属するか
- runnable か deferred か quarantined か、あるいは通常面から外れているか

live URL treatment の唯一の正本は scheduler であるべきである。

これは次とは別責務である。

- durable ledger fact
- host pacing state

operational lane は、必要なら持ってよい実装面であって、第一級の model 概念ではない。

runtime が複数の worker lane を持つとしても、それは scheduler membership の上に載る一時的 /
operational な grouping と読むべきであり、URL state そのものの定義にしてはいけない。

## 4. Execution And Lease Ownership

Execution は active work ownership を持つ。

答えるべき問い:

- この URL を今どの worker が所有しているか
- その ownership はいつ失効するか

execution state は小さく明示的であるべきである。

`leased` は ledger の概念でも host-state の概念でもなく、execution の概念である。

## 5. Host State

Host state は host/site 単位の politeness, backoff, capacity を持つ。

答えるべき問い:

- 今この host に触れてよいか
- 次に安全に request できる時刻はいつか
- host は cooldown 中か
- どの程度の in-flight capacity を与えるべきか

host state は URL state ではない。scheduler への入力である。

## 6. Crawl Pipeline

Crawl pipeline は execution 中の work の流れを持つ。

project はすでに次の段へ収束しつつある。

- fetch
- parse
- finalize
- persist

この分離が重要なのは、scheduler が後段全部を同期的に抱え込まないためである。

pipeline stage は operational boundary であり、durable URL identity boundary ではない。

## 7. Read Models And Operator Surfaces

operator view は primary state から導出されるべきであり、scheduler truth として扱うべきではない。

ここに含まれるもの:

- `/stats`
- queue/readiness summary
- top-host table
- error breakdown
- runtime snapshot

これらは有用だが、primary state ではない。

## 8. Bootstrap And Seed Management

seed は bootstrap input であり、恒久的な scheduler category ではない。

この layer が持つもの:

- seed catalog maintenance
- runtime seed set の rendering
- 初期 bootstrap

URL が crawler の内部に入った後まで、通常の scheduler treatment へ漏れてくるべきではない。

## Mapping Current Concepts Toward The Model

現在の実装概念は次の意味へ収束していくべきである。

- `url_ledger` => URL ledger
- `discover` / admission logic => discovery and admission
- `scheduler_queue_*` => scheduler membership surfaces
- worker lane / queue ごとの worker pool => model truth ではなく operational execution surface
- `active_leases` => execution ownership
- `host_state` => host state
- `fetch -> parse -> finalize -> persist` => crawl pipeline
- `/stats` と runtime payload => read models
- seed catalog と bootstrap path => bootstrap layer

これは convergence target であり、現在の runtime がすでに綺麗に分離されているという意味ではない。

## Main Gaps Versus The Ideal Model

現時点では、project には主に次の convergence gap が残っている。

1. discovery と scheduler membership の結合がまだ強い
2. breadth 制御が URL-first core の周辺 policy に依存しすぎている
3. queue naming が state と intent の語彙を混ぜている
4. `priority` が複数の意味を持ちすぎている
5. bootstrap と seed influence が通常 scheduling からまだ十分に隔離されていない

## Immediate Design Consequences

1. ledger insertion と scheduler admission は分離可能でなければならない
2. live scheduler truth は ledger の外に置かなければならない
3. host-first breadth は policy glue ではなく scheduler truth 側へ寄せるべきである
4. read model は派生値のままにするべきである
5. seed handling は通常 runtime scheduling から見えにくくなるべきである
6. worker lane は state / intent / strategy の下位概念に留めるべきである
