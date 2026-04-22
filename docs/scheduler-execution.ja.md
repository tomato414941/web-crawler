# Scheduler Execution

この文書は、scheduler state がどのように crawler の実行 work になるかを説明する。
抽象的な crawler concepts より下にあり、scheduler state model の隣に置く文書である。

関連文書:

- [crawler-concepts.ja.md](/home/dev/projects/web-crawler/docs/crawler-concepts.ja.md): 抽象モデル
- [scheduler-state-model.ja.md](/home/dev/projects/web-crawler/docs/scheduler-state-model.ja.md): scheduler の正本境界
- [system-architecture.ja.md](/home/dev/projects/web-crawler/docs/system-architecture.ja.md): project 全体の subsystem 分解

## Purpose

Scheduler execution が答える問いはもっと狭い。

scheduler membership、host state、active lease があるとき、crawler は次の work をどう選ぶべきか。
そのとき lease path を重くしないためには、どの責務をどこに置くべきか。

この文書は runtime execution strategy を扱う。durable URL state、policy intent、crawl pipeline
output の正本ではない。

## Execution Layers

execution は、次の 4 つの layer として分けて考える。

1. Scheduler membership: URL がどの live scheduler surface に属しているか
2. Execution strategy: worker がそれらの surface からどう work を選ぶか
3. Runtime-facing read models: worker や operator が使う派生 view
4. Operator stats: worker 数、active host、queue depth などの summary

現在の実装では、同じ table や query が複数 layer を兼ねている場合がある。
それでも、それぞれの意味は分けて扱う。

## Current Runtime Interpretation

現在の runtime には、まだ物理 scheduler queue がある。これらは implementation surface であり、
通常 crawler 実行の第一の主語ではない。

現在の解釈:

- `runnable` と `scheduled` は内部 scheduler membership projection
- `normal` は、それらをまたいだ通常クロール用の runtime-facing runnable view
- `refresh` work は通常クロールとは分ける
- URL が選ばれた後の実行所有権は active lease が持つ
- host に触れてよいか、どの程度触れてよいかは host state が決める

通常 crawler worker は、combined `normal` view から host-first に lease するべきである。
`scheduled` work を `runnable` に promotion しないと通常実行できない、という形にはしない。

## Hot Path Rule

lease selection は hot path である。worker ごとに何度も実行されるため、毎回の処理を重くしてはいけない。

lease path では、毎回 ready queue 全体を scan / sort / window して host runnable capability を
作り直す形を避けるべきである。

scheduler は、安い host-first executable view へ寄せる。

- host eligibility を繰り返し correlated lookup しなくてよい形にする
- host-level runnable head を安く読めるようにする
- host を選んだ後に URL を選ぶ
- operator-facing summary が worker の lease path を遅くしないようにする

read model が派生値であること自体は問題ない。ただし、高並列 crawler の lease ごとに
高コストな派生 model を最初から作り直す形は避ける。

## Observed Bottleneck

現在の production bottleneck は host-first lease selection にある。

2026 年 4 月の production 調査で観測したこと:

- crawler 24 worker では PostgreSQL CPU が 90-95% 付近まで上がり、crawler CPU はそれより低かった
- 現在の host-first candidate query は、概ね数百 ms から 1 秒程度かかった
- PostgreSQL JIT を無効化すると、ある測定では query が約 1035 ms から約 662 ms になった
- 繰り返しの correlated `host_state` lookup を single join にすると、ある測定形では約 309 ms、
  `work_mem` を大きくすると約 266 ms になった
- crawler 1 worker では publish/finalize scheduled は消えたが、lease selection はなお数百 ms になることがあった

これらの数値は恒久的な SLO ではない。次に見るべき実装箇所を示す観測値である。

## Design Constraints

execution の変更では、次の制約を守る。

- URL ledger fact と live scheduler execution を分ける
- 置き換えができるまでは、queue membership を scheduler truth として扱う
- in-flight execution の所有権は active lease に置く
- 通常実行 strategy は host-first breadth を維持する
- `refresh` は通常クロールとは分ける
- `normal` は durable URL state ではなく runtime-facing view として扱う
- hot path の global scan を避けられるなら、小さな schema migration は許容する

## Implementation Direction

現在の実装方針は、queue membership を scheduler の正本として維持しつつ、通常 host-first
candidate selection を incremental な loose read model に寄せることである。

実装済みの順序:

1. host-first candidate selection の correlated `host_state` subquery を single join に置き換える
2. PostgreSQL JIT や query shape の影響を production で測る
3. `COUNT(*) OVER (PARTITION BY host)` が production scale で重いことを確認する
4. loose な host runnable-head projection を追加する
5. host runnable-head read model を通常 host-first lease candidate source として使う
6. crawl-cycle start の global rebuild を通常経路から外す
7. queue membership 変更時は、影響を受けた `(physical_queue, host)` の head だけを更新する

`host_runnable_heads` は runtime read model である。現在の table は過渡的に、代表 URL
である head と、execution tier、runnable time、latency bucket、runnable URL count といった
host capability signal を同居させている。これらの capability field は durable fact ではない。
特に `runnable_url_count` は厳密な正本 count ではなく、ordering/readiness 用の signal として扱う。

概念上は次のように分ける。

- host runnable capability: host が work を出せるか、どれくらい work を持つか
- host runnable head: その host を代表して次に出す URL
- scheduler queue: URL membership の正本
- active lease: in-flight execution の正本

lease path は cheap-miss pattern を使う。

- `host_runnable_heads` から candidate host head を読む
- queue membership、active lease、`host_state` で candidate を再検証する
- stale candidate は削除し、その host の head を局所的に refresh する
- read model が空または利用不能な場合だけ bounded queue scan を safety fallback として使う

dirty refresh、repair、rebuild は read model maintenance であり、通常実行の中心ではない。
global rebuild は manual repair mechanism として残すが、crawler startup の通常動作にはしない。
