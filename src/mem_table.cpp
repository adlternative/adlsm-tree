#include "mem_table.hpp"
#include <string.h>
#include "file_util.hpp"
#include "hash_util.hpp"
#include "monitor_logger.hpp"
#include "sstable.hpp"
namespace adl {

RC MemTable::Put(const MemKey &key, string_view value) {
  lock_guard<shared_mutex> lock(mu_);
  if (key.op_type_ == OP_PUT)
    table_[key] = value;
  else if (key.op_type_ == OP_DELETE)
    table_[key] = "";

  stat_.Update(key.Size(), value.size());
  return OK;
}

RC MemTable::PutTeeWAL(const MemKey &key, string_view value) {
  RC rc = OK;
  /* 首先写到预写日志 wal */
  assert(wal_);
  rc = wal_->AddRecord(EncodeKVPair(key, value));
  if (rc) return rc;
  if (options_->sync) {
    rc = wal_->Sync();
    if (rc) return rc;
  }
  /* 然后再写入内存表 memtable */
  rc = Put(key, value);
  return rc;
}
RC MemTable::Get(string_view key, string &value, int64_t seq) {
  shared_lock<shared_mutex> lock(mu_);
  return GetNoLock(key, value, seq);
}

RC MemTable::GetNoLock(string_view key, string &value, int64_t seq) {
  MemKey look_key(key, seq);

  auto iter = table_.lower_bound(look_key);
  if (iter == table_.end()) return NOT_FOUND;

  if (key == iter->first.user_key_ &&
      iter->first.op_type_ != OpType::OP_DELETE) {
    value = iter->second;
    return OK;
  }
  return NOT_FOUND;
}

/* 一般是 IMEMTABLE 进行 BUILD 不需要加锁 */
RC MemTable::BuildSSTable(string_view dbname,
                          FileMetaData **meta_data_pointer) {
  MLog->info("memtable -> sstable");
  int sstable_level = 0;
  string true_path = FileManager::FixDirName(dbname);
  dbname = true_path;
  FileMetaData *meta_data = new FileMetaData;
  int64_t max_seq = 0;

  /* 一个新的 sstable writer */
  auto sstable_ok = NewSSTableWriter(dbname, options_);
  if (!sstable_ok) {
    delete meta_data;
    return NEW_SSTABLE_ERROR;
  }
  auto sstable = std::move(sstable_ok.value());

  /* 向 sstable 写入 memtable 的所有数据 */
  RC rc = ForEachNoLock([&](const MemKey &memkey, string_view value) -> RC {
    if (max_seq < memkey.seq_) max_seq = memkey.seq_;
    string key = memkey.ToKey();
    return sstable->Add(key, value);
  });
  if (rc) return rc;
  /* 向 sstable 写入索引，过滤器等元数据 */
  if (rc = sstable->Final(meta_data->sha256); rc) return rc;

  MLog->info("sstable {} created", sstable->GetPath());

  /* 填充元数据，之后用来更新索引数据 */
  meta_data->file_size = sstable->GetFileSize();
  meta_data->min_inner_key = table_.begin()->first;
  meta_data->max_inner_key = table_.rbegin()->first;
  meta_data->num_keys = (int)table_.size();
  meta_data->max_seq = max_seq;
  /* 目前 minor compaction 生成的 sstable 就放在 l0 */
  meta_data->belong_to_level = sstable_level;
  *meta_data_pointer = meta_data;
  return OK;
}

/* 暂时用不加锁版本 */
RC MemTable::ForEachNoLock(
    std::function<RC(const MemKey &key, string_view value)> &&func) {
  for (auto iter = table_.begin(); iter != table_.end(); iter++) {
    const MemKey &memkey = iter->first;
    string_view value = iter->second;
    auto rc = func(memkey, value);
    if (rc) return rc;
  }
  return OK;
}

MemTable::MemTable(const DBOptions &options)
    : options_(&options), wal_(nullptr) {}

MemTable::MemTable(const DBOptions &options, WAL *wal)
    : options_(&options), wal_(wal) {}

size_t MemTable::GetMemTableSize() {
  shared_lock<shared_mutex> lock(mu_);
  return stat_.Sum();
}

RC MemTable::DropWAL() {
  RC rc = OK;
  if (wal_) {
    MLog->trace("MemTable::DropWAL");
    if (rc = wal_->Sync(); rc) return rc;
    if (rc = wal_->Close(); rc) return rc;
    if (rc = wal_->Drop(); rc) return rc;
    delete wal_;
    wal_ = nullptr;
  }
  return rc;
}

bool MemTable::Empty() { return table_.empty(); }
}  // namespace adl
