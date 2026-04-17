# Discovered Representation

この文書は、理想化された crawler model と最終的な runtime 実装の間で、`discovered` をどう表現するべきかを定義する。

これは [crawler-principles.ja.md](/home/dev/projects/web-crawler/docs/crawler-principles.ja.md) より具体的で、
[scheduler-state-model.ja.md](/home/dev/projects/web-crawler/docs/scheduler-state-model.ja.md) よりは概念寄りの文書である。

## Purpose

この文書が答えたい問いは 1 つである。

system が URL を知っているが、まだ通常の scheduler membership を持っていないとき、
その URL をどう表現するべきか。

## Core Position

`discovered` は queue ではなく state として表現するべきである。

より正確には:

- system はその URL を知っている
- その URL はまだ通常の scheduler membership の外にいる
- この条件が `discovered` である

この整理により、crawler model が求める durable fact / scheduler membership / execution /
policy intent の分離と整合する。

## Relationship To URL Ledger

URL ledger は URL identity と履歴の durable source of truth のままである。

ledger が答えるべき問い:

- この URL を知っているか
- 最初にいつ見たか
- 最後に確定した durable outcome は何か

ledger が答えるべきでない問い:

- 今 runnable か
- どの queue にいるか
- leased されているか

したがって、`discovered` を ledger の live status column として持つべきではない。

代わりに、ある URL が `discovered` であるとは:

- ledger record を持っている
- 現在の scheduler membership を持っていない

という意味である。

## Relationship To Scheduler Membership

scheduler membership は durable URL identity とは別責務である。

`discovered` の意味:

- system は知っている
- しかし通常の live scheduler surface にはまだ入っていない

対して:

- `runnable` は live scheduler membership を持ち、lease 可能であること
- `leased` は execution ownership があること
- `quarantined` は通常の leasing から意図的に外されていること

つまり `discovered` は `runnable` の前段であり、deferred queue の別名ではない。

## Why Backlog Is Not `discovered`

`backlog` は deferred work ではあっても scheduler work である。

URL が `backlog` にいるなら、scheduler はすでに:

- この URL は live scheduler surface に属する
- 現在の scheduling policy に参加する

と判断している。

これは `discovered` とは違う。`discovered` は:

- system は知っている
- しかし通常の scheduler membership はまだ付与されていない

という意味である。

したがって、`backlog` は discovered-but-unscheduled ではなく
scheduled-but-deferred と読むべきである。

## Operational Surface

`discovered` は概念上の state である。これは実装上、同名の queue を必須にするものではない。

runtime では次のための operational surface が必要になる可能性がある:

- admission work
- batching
- multi-worker claiming
- host-aware に scheduler membership へ昇格させる処理

しかし、その operational surface は implementation detail であり、
`discovered` という概念そのものではない。

言い換えると:

- `discovered` は state
- admission queue / admission view / claimable table は、その state を処理するための mechanism

である。

## Multi-Worker Consequence

multi-worker system では、worker が URL ledger 全体を work queue として解釈するべきではない。

durable ledger は known URL の正本でありつつ、admission worker はより小さい derived surface で
動く方が自然である。

これにより、次の責務を分離できる:

- durable URL fact
- admission processing
- scheduler membership

## Preferred Final Interpretation

最終的に望ましい解釈は次の通りである。

1. URL が ledger に現れる
2. その URL は `discovered` にある
3. admission logic が scheduler membership を付与するかどうかを決める
4. URL は `runnable` などの live scheduler state へ進む

重要なのは、`discovered` は queue 名で定義されるのではなく、
current scheduler membership をまだ持っていないことで定義される、という点である。

## Immediate Design Consequences

1. ledger insertion と scheduler admission は分離可能であるべき
2. live scheduler state の正本は queue membership のままであるべき
3. `backlog` に `discovered` の意味を背負わせるべきではない
4. runtime が discovered-processing surface を必要とするなら、それは概念定義ではなく
   operational mechanism として導入するべき
