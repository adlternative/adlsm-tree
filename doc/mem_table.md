
* `<key,value>`

* `key` format:
`|user_key|seq|type|`

有利于前缀压缩。

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


* `adlsm-tree` 使用 `std::map` + `std::shared_mutex` 实现，性能可能不如 skiplist 但是实现简单。