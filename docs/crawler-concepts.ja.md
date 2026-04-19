# WWW Crawler Concepts

この文書は、理想化された WWW crawler の抽象モデルを整理する。現在の `web-crawler` 実装の説明ではなく、命名や責務分離を判断するための設計原則を置く。

## Core Separation

理想的な crawler では、少なくとも次の 5 要素を分けて考えるべきである。

- `ledger`: 発見済み URL の durable identity と履歴
- `scheduler membership`: 現在どの live surface に属しているか
- `execution`: active lease と worker ownership
- `host state`: host/site 単位の politeness と backoff
- `policy intent`: なぜその URL を次に取りたいのか

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

## Why `exploration` Feels Off

`exploration` は state 名としては少し不自然である。

理由:

- `exploration` は行為や目的に近い語である
- live scheduler surface 名としては、現在位置より意図を強く表す
- `backlog` との対概念として見ると、「探索する/しない」より「前面/後面」の差に見える

したがって理想形では、`explore` は intent として残し、surface 名は `scheduled` や `runnable` のような state 語に寄せる方が自然である。

## Runnable Capability Principle

scheduler は URL の集合そのものではなく、`runnable capability` の集合として考えるべきである。

実際に scheduler が最初に見たいのは:

- どの host/site が今開いているか
- 各 host にどれだけ runnable work があるか
- その work がどの intent に属するか

この意味では、scheduler の一次単位は URL より host/site に寄る。

runtime 実装には、物理 queue projection、worker lane、operator-facing view が残っていてもよい。
それらはこの concept より下の execution detail である。抽象ルールとしては、通常クロールは
raw URL queue ではなく、host/site の runnable capability を見て動くべきである。

runtime execution の設計は
[scheduler-execution.ja.md](/home/dev/projects/web-crawler/docs/scheduler-execution.ja.md) に置く。

## Naming Guidance

抽象モデルから見た naming の指針:

- `ledger` 系の名前は durable fact に使う
- `scheduled` / `runnable` / `leased` / `blocked` 系の名前は live state に使う
- `explore` / `refresh` / `retry` は policy intent に使う

つまり、`exploration` は最終的な理想名というより、intent と state がまだ分離されていない過渡的な名前として扱うのが妥当である。
