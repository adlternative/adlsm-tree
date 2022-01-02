LRU CACHE

数据结构是是用 `list<kvpair>` 和 `map<key, list<kvpair>::iterator>`实现的。

为什么不用其他缓存策略？

1. LRU 实现起来最简单直观。
2. CLOCK 算法需要时间感知。但你可以看到 rocksdb 实现了 [CLOCK CACHE](https://github.com/facebook/rocksdb/wiki/Block-Cache)

将什么东西作为缓存？
1. `{sstable_sha256_hex, SSTableReader(opened)}`
2. `SSTable Block`?