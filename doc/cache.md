LRU CACHE

### 为什么需要 cache

一个事实：内存的读取速度比磁盘快得多。

如果每次都从磁盘读取，性能会很低。
我们可以将那些经常访问的内容驻留在内存中，这样可以提高读取的性能。

### 结构
数据结构是是用 `list<kvpair>` 和 `unordered_map<key, list<kvpair>::iterator>`实现的。

### 为什么不用其他缓存策略

1. LRU 实现起来最简单直观。
2. CLOCK 算法需要时间感知。但你可以看到 rocksdb 实现了 [CLOCK CACHE](https://github.com/facebook/rocksdb/wiki/Block-Cache)

### 将什么东西作为缓存
1. table cache `sstable.oid -> sstable`
2. block cache `sstable.oid + offset -> sstable.block`

本来想要加 kv cache，但是因为如果需要支持多版本，似乎这个 cache 就不太合适了。

加了 table cache 后性能好了 60%。加了 block cache 后性能好了 20%。