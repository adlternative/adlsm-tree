### Minor Compaction

```
mem_table -> imem_table -> sstable (l0) -> sstable (ln)
```

当我们即将进行写入的时候，会对当前的 memtable 的大小进行判断，如果超过
了 4MB，则会调用后台线程执行 major compaction。

### Major Compaction

| 压实策略     |                                                                                |
| ------------ | ------------------------------------------------------------------------------ |
| 数据分布策略 | 目前想用 L-Leveling (前 N-1 层用 tiering 最后一层用 leveling) 首先实现 tiering |
| 压实粒度策略 | 将一层的所有 sort runs 进行压实                                                |
| 压实触发策略 | 一层排序 run 的数量达到阈值                                                    |
| 数据移动策略 | N/A                                                                            |