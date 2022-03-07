### sstable 排序字符串表

### 为什么需要 sstable

1. RAM 内存易失，掉电后内存中的数据将不复存在，或者关闭了进程，内存中的数据将丢失。为了数据持久存在，我们需要将内存中的数据写入到磁盘上的 sstable 中。（当然还需要 预写日志辅助崩溃恢复）
2. 内存的容量有限，我们需要让内存中 **溢出** 的数据写入到磁盘上以支持更大的容量。

### 结构

整体上 `sstable` 的构造是参考 `leveldb` 的，
* data block 存放数据
* filter block 存放过滤器
* meta block 存放元数据或者 filter block 索引
* index block 存放 data block 索引
* footer 索引 index block 索引和 meta block 的引用

`rocksdb` 的 `sstable` 实现可能会更加复杂，比如添加 `STAT BLOCK`,`HASH INDEX BLOCK` ...

| SSTable Format |
| -------------- |
| DATA BLOCK 0   |
| DATA BLOCK 1   |
| DATA BLOCK 2   |
| FILTER BLOCK   |
| META BLOCK     |
| INDEX BLOCK    |
| FOOTER BLOCK   |

基础块 block 内部存放数据和重启点位置，我们会通过重启点快速索引到数据块的偏移量（二分）。每个重启点对应的数据的键都会全量保存。
而每个非重启点对应的数据的键只会保存和前一个键的不同部分。（增量压缩）

| Block Format       |
| ------------------ |
| ENTRY 0            |
| ENTRY 1            |
| ENTRY 2            |
| RESTART POINT 0    |
| RESTART POINT 1    |
| RESTART POINT 2    |
| RESTART POINTS LEN |

| Entry Format                                                           |
| ---------------------------------------------------------------------- |
| SHARED_KEY_LEN UNSHARED_KEY_LEN `USER_KEY SEQ OP_TYPE` VALUE_LEN VALUE |


过滤器块 filter block

| Filter Block Format    |
| ---------------------- |
| FILTER 0               |
| FILTER 1               |
| FILTER 2               |
| FILTER 0 OFFSET        |
| FILTER 1 OFFSET        |
| FILTER 2 OFFSET        |
| FILTER 0 OFFSET OFFSET |
| FILTERS NUM            |
| FILTERS INFO           |
| FILTERS INFO_LENGTH    |

| Filter Format |
| ------------- |
| `<bitmap>`    |

注意这里的 `OFFSET` 是针对该 `FILTER BLOCK` 的，而不是整个文件。

`leveldb` 中是每一个 `DATA BLOCK` 配一个 `FILTER BLOCK`，本项目的策略是一个 `sstable` 配一个 `FILTER BLOCK`，这样，布隆过滤器检查的工作流程就大大简化了。当然一个很大问题就是这样过滤器的大小会非常大，这可能会影响读取性能。

而 `rocksdb` 中则是为所有 `sstable` 配一个 `FILTER BLOCK`。


索引块 Index Block Format

遵循 block format， 不过 key 会保存对应的数据块的最大键值。我们用它来二分查找 key 所对应的数据块位置。

| Index Block Format                                             |
| -------------------------------------------------------------- |
| DATA_BLOCK_MAX_KEY 1, `(DATA BLOCK 1 OFFSET, DATA BLOCK SIZE)` |
| DATA_BLOCK_MAX_KEY 2, `(DATA BLOCK 2 OFFSET, DATA BLOCK SIZE)` |
| DATA_BLOCK_MAX_KEY 3, `(DATA BLOCK 3 OFFSET, DATA BLOCK SIZE)` |


元数据块 Index Block Format

遵循 block format，可以存放很多 sstable 的元数据，例如 `sstable` 中键值对的数量...（所谓物化视图呵呵呵）
当然最朴素的就是记录一下 filter block 的偏移量。

| Meta Block Format                                            |
| ------------------------------------------------------------ |
| FILTER BLOCK KEY, `(FITLER BLOCK OFFSET, FILTER BLOCK SIZE)` |


尾部块 Footer Block Format

1. 存放元数据块的偏移量和大小。
2. 存放索引块数据块的偏移量和大小。

| Footer Block Format                      |
| ---------------------------------------- |
| `(META BLOCK OFFSET, META BLOCK SIZE)`   |
| `(INDEX BLOCK OFFSET, INDEX BLOCK SIZE)` |
| 0x12, 0x34                               |