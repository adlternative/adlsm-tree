### 压实策略
---

#### Minor Compaction

基本上 `Minor Compaction` 就是最基础的 memtable 落盘流程：当 `memtable` 大小大于用户设置的阈值时（例如 4MB），将 `memtable` 移动到 `imemtable` 中，然后后台压实线程会将 `imemtable` 落盘到 `sstable` 中。

```
memtable -> imemtable -> sstable (l0) -> sstable (ln)
```

#### Major Compaction

这篇 paper [vldb21-compaction-analysis](http://vldb.org/pvldb/vol14/p2216-sarkar.pdf) 详细的讲解了不同压实策略的优缺点。可以说工业界的不同类型的压实策略都是为了符合其对应的业务场景，必须在不同的需求之间进行 trade-off。

| 压实策略     | ADLsm-tree 做出的选择                                                                |
| ------------ | ------------------------------------------------------------------------------------ |
| 数据分布策略 | 目前实现了 tiering 后续可以改成 L-Leveling (前 N-1 层用 tiering 最后一层用 leveling) |
| 压实粒度策略 | 将一层的所有 sort runs 进行压实                                                      |
| 压实触发策略 | 一层排序 run 的数量达到阈值                                                          |
| 数据移动策略 | N/A                                                                                  |

### 为什么目前用 tiering 策略
1. 易于实现，没有像 leveling 策略中复杂的数据选择策略。
2. 可以减小写放大。leveling 策略下会频繁的进行 `major compaction`，而 tiering 策略只会在本层的 sstable 数量足够大的时候才会转移下一层，这样可以减少 `major compaction` 带来的写放大。

缺点：
1. 空间放大。每层有多个 runs，也就是说一个 key 可能在本层的多个 runs 中，这样就会占用更多的空间。
2. 读取放大。因为读取需要遍历每一层所有的 runs，而 leveling 只用读取本层一个 run，tiering 的读取代价是很高的。
3. ...

后续会改成 `L-Leveling` 策略。