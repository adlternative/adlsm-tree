版本控制

CURRENT Format:

`revision-sha`


Revisions Format:

  | revision wal seq number |
  | ----------------------- |
  | 3                       |

  | LEVEL | SHA           |
  | ----- | ------------- |
  | 0     | `<level-sha>` |
  | 1     | `<level-sha>` |
  | 2     | `<level-sha>` |
  | 3     | `<level-sha>` |
  | 4     | `<level-sha>` |


Level Format:

  | Level | FileNum |
  | ----- | ------- |
  | 1     | 3       |

  | min-key len | min-key | max-key len | max-key | SHA             |
  | ----------- | ------- | ----------- | ------- | --------------- |
  |             | adl     |             | alskdj  | `<sstable-sha>` |
  |             | basld   |             | caslkdj | `<sstable-sha>` |
  |             | esakld  |             | faslkdj | `<sstable-sha>` |
  |             | fjaskl  |             | qeku    | `<sstable-sha>` |
  |             | ylyly   |             | zzzz    | `<sstable-sha>` |



第 n 层 `level` 对象文件位于 `dbname/objects/level/<n>/xxx.lvl`。
`sstable` 对象文件位于 `dbname/objects/sst/xxx.sst`。
`revision` 对象文件位于 `dbname/objects/rev/xxx.rev`。
`CURRENT` 伪引用文件位于 `dbname/CURRENT`。


版本生成流程：


首先最初始化版本啥也没有，咱的 `CURRENT` 刚开始是空的。当 `memtable` 第一次写到磁盘上的 `sstable` (`minor compaction`), `sstable` 元数据比如最小键，最大键，哈希值会被更新到 `level` 文件，然后 `level` 文件的哈希值会被更新到 `revision` 最后 `revision` 的哈希值会被更新到 `CURRENT` 文件里面。`major compaction` 同理，也会生成一些 `sstable`，同理也会更新 `level`,`revision`, `CURRENT`。

内存中保存是 `CURRENT` 指向的版本对象的一个内存对象，每次发生元数据更改的时候，大致的公式是：`new-revision = current-revision + meta-file-changes; current = new-revision`。目前不打算记录版本之间的链式的关系，因为我们没有一个合适的需求去遍历这些旧版本。我们可以通过快照来保存版本，比如我们可以通过 `snapshot` 来保存一个 `revision` 的快照，然后通过 `revision` 的快照来恢复到某个版本。（未开发功能）。

何时进行垃圾回收（未开发功能）：IDLE？Every-N-Seconds？Every-N-Files?


* 设计的效果：
  将易于支持 Snapshot 和 GC 和 BLOB, TXT。

* 缺点：
  写放大和读放大。