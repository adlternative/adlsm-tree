#### API

#### DB::Open

打开一个数据库实例，返回一个数据库句柄指针，类型为 `DB*` 。同时你可以通过设置 `DBOptions` 来指定一些配置的选项。通过返回值来获取错误信息。

```cpp
  // static RC Open(string_view dbname, DBOptions &options, DB **dbptr);
  DB *db = nullptr;
  DBOptions opts;
  string dbname = "/tmp/adl-testdb";
  opts.create_if_not_exists = true;
  auto rc = DB::Open(dbname, opts, &db);
  if (rc == OK) {
    printf("open db success\n");
  } else {
    printf("open db failed\n");
  }
```

#### DB::Close

在使用结束时通过 `DB::Close()` 关闭数据库，但是记得得使用 `delete db` 去释放 db 的内存, 否则会导致内存泄漏。

```cpp
  // RC DB::Close();
  DB *db = nullptr;
  DBOptions opts;
  string dbname = "/tmp/adl-testdb";
  opts.create_if_not_exists = true;
  auto rc = DB::Open(dbname, opts, &db);
  ...
  db->Close();
  delete db;
```

#### DB::Destroy

删除数据库中所有内容。这将销毁整个数据库所在的文件夹。

```cpp
  // static RC DB::Destroy(string_view dbname);
  auto rc = DB::Destroy("/tmp/adl-testdb");
```

#### Put

将一对 `{key, value}` 写入数据库。

```cpp
  // RC DB::Put(string_view key, string_view value);
  auto rc = db->Put(key, value);
```

#### Get

获取最后一次写入的 key 对应的 value。

```cpp
  // RC DB::Get(string_view key, std::string &value);
  string value;
  auto rc = db->Get(key, value);
```

#### Delete

将写入的 key 从数据库中删除。

```cpp
  // RC DB::Delete(string_view key);
  auto rc = db->Delete(key);
```

#### Debug

获取当前的数据库的当前版本的调试信息。

```cpp
  // void DB::Debug();
  db->Debug();
```
