### ADLSM-Tree: 一个简单的 lsm-tree 持久键值存储引擎
![License-MIT](license-MIT-green.svg)
---

lsm-tree 其主要思想源于 [The Log-Structured Merge-Tree (LSM-Tree)](https://www.cs.umb.edu/~poneil/lsmtree.pdf) 这篇 1996 年的考古 paper。其主要思想是通过将随机写入转换成顺序写入提高写入的性能。

本项目 ADLSM-Tree 是在学习了 leveldb 的原理和参考了部分 lsm-tree 的资料后自主实现一个简单的 lsm-tree 持久化键值存储引擎。

#### 实现细节

* ADLSM-Tree 的 并发内存表 `memtable` 采用了 `std::shared_mutex` 和 `std::map` 的方式简单实现，没有采用 leveldb 中的无锁跳表 `std::atomic` 和 `skiplist` 。详见 [doc/mem_table.md](doc/mem_table.md)。

* ADLSM-Tree 的 排序字符串表 `sstable` 结构基本与 leveldb 一致，但 bloom-filter 是整个 sstable 一个，而不是每个 block 一个，同时 `bloom-filter` 采用的是标准的双哈希模拟多哈希的方式实现，没有采用 leveldb 是采用单哈希模拟多哈希的方式。详见 [doc/sstable.md](doc/sstable.md)。

* ADLSM-Tree 的 预写日志 `wal` 是通过向文件追加 `{k,v}` 简单实现的，没有采用 leveldb 中的日志文件的方式。详见 [doc/wal.md](doc/wal.md)。

* ADLSM-Tree 的 版本控制 `revision` 则有些复杂，每次 `minor-compaction` 或者 `major-comacption` 都会触发版本突变，首先每个磁盘上的 `sstable` 都会在生成时计算整个文件的 `SHA-256` 并作为其文件名，然后内存中会生成一个新的 `level` 对象记录当前层级的所有 `sstable` 的元数据信息，例如 `sstable` 的 `min-key` 和 `max-key`，以及 `sstable` 的 `SHA-256`。然后将 `level` 对象持久化到磁盘中，同样计算 `level` 文件的 `SHA-256` 作为 `level` 文件名，然后将 `level` 文件的 `SHA-256` 和其层级记录到内存一个新的 `revision` 对象中，并持久化 `revision` 对象，同样计算 `revision` 对象的 `SHA-256`，更新内存中 `current_rev` 指向刚刚生成的 `revision` 对象，完成版本突变。并没有采用 leveldb 中的 `manifest` 文件去记录版本的变化。详见 [doc/revision.md](doc/revision.md)。

* ADLSM-Tree 的 压实采用 `Tiering` 策略以减小写入放大，后续会尝试采用 `L-Leveling` 等其他压实策略。详见 [doc/compaction.md](doc/compaction.md)。

* ADLSM-Tree 的缓存采用 `LRU` 策略。对 `block` 数据块和 `sstable` 分别进行缓存以提高读取性能，详见 [doc/cache.md](doc/cache.md)。

#### API

- [doc/api.md](doc/api.md)

#### 依赖
* gtest
* google-crc32c
* openssl
* fmt
* spdlog

安装依赖 (ArchLinux):

```sh
sudo pacman -S gtest
sudo pacman -S google-crc32c
sudo pacman -S openssl
sudo pacman -S fmt
sudo pacman -S spdlog
```


在45行提交了修改


#### 编译

```
mkdir build
cmake ..
make -j8
```

#### 参考

- [leveldb](https://github.com/google/leveldb)
- [rocksdb](https://github.com/facebook/rocksdb)
- [vldb21-compaction-analysis](http://vldb.org/pvldb/vol14/p2216-sarkar.pdf)