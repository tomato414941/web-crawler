# WWW Crawler Concepts

この文書は、理想化された WWW crawler の抽象モデルを整理する。現在の `web-crawler` 実装の説明ではなく、命名や責務分離を判断するための設計原則を置く。

## Core Separation

理想的な crawler では、少なくとも次の要素を分けて考えるべきである。

- `ledger`: 発見済み URL の durable identity と履歴
- `host ledger`: host の durable identity と履歴
- `scheduler membership`: 現在どの live surface に属しているか
- `execution`: active lease と worker ownership
- `host state`: host/site 単位の politeness と backoff
- `policy intent`: なぜその URL を次に取りたいのか
- `fetch admission`: 選ばれた URL の response body を読む価値があるか

重要なのは、`state` と `intent` を混ぜないことだ。

## States Versus Intents

主状態として自然なのは次のような語である。

- `discovered`
- `scheduled`
- `runnable`
- `leased`
- `blocked`
- `terminal`

一方で、次は state ではなく intent として扱う方が自然である。

- `explore`
- `refresh`
- `retry`

例:

- `runnable + explore`
- `scheduled + refresh`
- `blocked + retry`

この分離がないと、queue 名が「いまどこにいるか」と「なぜ取りたいのか」の両方を背負ってしまう。

## Why State Names Matter

scheduler surface 名は、その work が今どこにいるかを表すべきであり、crawler がなぜそれを取りたいかを表すべきではない。

理由:

- action や purpose に近い語は intent に属する
- live scheduler surface は現在の扱いを強調するべきである
- 対比は purpose の違いではなく、実行可能か、まだ実行可能ではないかであるべきである

理想形では、`explore` は intent として残し、surface 名は `scheduled` や `runnable` のような state 語を使う。

## Runnable Capability Principle

scheduler は URL の集合そのものではなく、`runnable capability` の集合として考えるべきである。

実際に scheduler が最初に見たいのは:

- どの host/site が今開いているか
- 各 host にどれだけ runnable work があるか
- その work がどの intent に属するか

この意味では、scheduler の一次単位は URL より host/site に寄る。

`host runnable capability` と `host runnable head` は近いが同じ概念ではない。
capability は、その host/site が今 work を出せるか、どれくらい work を持っているかを表す。
head は、その host/site から次に出す代表 URL である。どちらも runtime execution のための
read model 概念であり、どの URL が live かの正本である scheduler membership を置き換えない。

runtime 実装には、物理 queue projection、worker lane、operator-facing view が残っていてもよい。
それらはこの concept より下の execution detail である。抽象ルールとしては、通常クロールは
raw URL queue ではなく、host/site の runnable capability を見て動くべきである。

runtime execution の設計は
[scheduler-execution.ja.md](scheduler-execution.ja.md) に置く。

## Fetch Admission Principle

scheduler が URL を選んだとしても、crawler が response body 全体を読むべきとは限らない。
fetch admission は、「この URL を 1 回試す」と「この payload に body read、parse、storage のコストを払う」の境界である。

抽象ルールは次の通りである。

- HTML と安全な text は parse 可能な page content になりうる
- binary、media、archive、font、image、stream resource はデフォルトで metadata-only にする
- 1 つの URL が worker や cycle を無期限に占有してはいけない
- metadata-only completion は valid outcome であり、crawl failure ではない

これにより、crawler は unbounded downloader ではなく WWW discovery に集中できる。

## Naming Guidance

抽象モデルから見た naming の指針:

- `ledger` 系の名前は durable fact に使う
- `host_ledger` は durable host identity / history に使い、runtime pacing には使わない
- `scheduled` / `runnable` / `leased` / `blocked` 系の名前は live state に使う
- `explore` / `refresh` / `retry` は policy intent に使う

purpose に近い語は scheduler surface 名ではなく intent field に置く。
