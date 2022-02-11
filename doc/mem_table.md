## memtable 内存表

### 用途
在内存用来存放 KV 数据，当内存表大小超过用户设置的阈值时，就会触发 `minor compaction`，内存表中数据将会被写入磁盘。
目前一个 DB 实例中只有 `memtable` 和 `imemtable` 两个单例，当 `memtable` 大小超过用户设置的阈值时会移动到 `imemtable` 中，
并使用一个新的 `memtable` 来存放新的数据。如果当前已经有 `imemtable` 则会触发 `minor compaction`，将 `imemtable` 中的数据写入磁盘，
这个过程在后台进行，但是会阻塞当前的写操作。从并发的角度上看，`memtable` 需要使用加锁去读取和修改，而 `imemtable` 代表不会进行修改的 `memtable` 因此不需要加锁。目前存在的一个很大的问题就是 `imemtable` 的驻留内存的时间不够长，可以考虑将 `imemtable` 修改为内存表队列的形式（占用更多的内存）来提高读取的性能。

### 结构

* `adlsm-tree` 使用 `std::map` + `std::shared_mutex` 实现，性能可能不如无锁 `skiplist` 但是实现简单。
* 字典中存放的是 `<key,value>` KV 键值对。
* `key` 的类型是 `MemKey`， format:`|user_key|seq|type|`。

MemKey 结构和 MemTable 结构：
```cpp
struct MemKey {
  string user_key_;
  int64_t seq_;  // sequence number
  enum OpType op_type_;
};

class MemTable {
  mutable shared_mutex mu_;
  map<MemKey, string> table_;
};
```

一个  `memtable` 差不多以以下的模式分布：

| user_key | seq | type           | value |
| -------- | --- | -------------- | ----- |
| key1     | 1   | `<WriteType>`  | val1  |
| key2     | 3   | `<WriteType>`  | val3  |
| key2     | 2   | `<WriteType>`  | val2  |
| key3     | 5   | `<DeleteType>` |       |
| key3     | 4   | `<WriteType>`  | val4  |

排序规则：`user_key asc`,`seq desc`。

我们从 `memtable` 读取的结果是：

`Get("key1") -> val1`
`Get("key2") -> val3`
`Get("key3") -> (nil)`
