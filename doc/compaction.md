### Minor Compaction

```
mem_table -> imem_table -> sstable (l0) -> sstable (ln)
```

当我们即将进行写入的时候，会对当前的 memtable 的大小进行判断，如果超过
了 4MB，则会调用后台线程执行 major compaction。

### Major Compaction

何时触发？