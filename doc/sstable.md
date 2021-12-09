整体上 `SSTable` 的构造是参考 `leveldb` 的，`rocksdb` 的 `SSTable` 实现可能会更加复杂，比如添加 `STAT BLOCK`,`HASH INDEX BLOCK` ...

| SSTable Format |
| -------------- |
| DATA BLOCK 0   |
| DATA BLOCK 1   |
| DATA BLOCK 2   |
| FILTER BLOCK   |
| META BLOCK     |
| INDEX BLOCK    |
| FOOTER BLOCK   |


| Block Format       |
| ------------------ |
| ENTRY 0            |
| ENTRY 1            |
| ENTRY 2            |
| RESTART POINT 0    |
| RESTART POINT 1    |
| RESTART POINT 2    |
| RESTART POINTS LEN |


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

注意这里的 `OFFSET` 是针对该 `FILTER BLOCK` 的，而不是整个文件。

| Filter Format |
| ------------- |
| `<bitmap>`    |


| Index Block Format                                             |
| -------------------------------------------------------------- |
| DATA_BLOCK_MAX_KEY 1, `(DATA BLOCK 1 OFFSET, DATA BLOCK SIZE)` |
| DATA_BLOCK_MAX_KEY 2, `(DATA BLOCK 2 OFFSET, DATA BLOCK SIZE)` |
| DATA_BLOCK_MAX_KEY 3, `(DATA BLOCK 3 OFFSET, DATA BLOCK SIZE)` |


| Meta Block Format                                            |
| ------------------------------------------------------------ |
| FILTER BLOCK KEY, `(FITLER BLOCK OFFSET, FILTER BLOCK SIZE)` |


| Footer Block Format                      |
| ---------------------------------------- |
| `(META BLOCK OFFSET, META BLOCK SIZE)`   |
| `(INDEX BLOCK OFFSET, INDEX BLOCK SIZE)` |
| 0x12, 0x34                               |