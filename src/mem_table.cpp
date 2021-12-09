#include "mem_table.hpp"
#include <string.h>
#include "file_util.hpp"
#include "monitor_logger.hpp"
#include "sstable.hpp"

namespace adl {

RC MemTable::Put(const MemKey &key, string_view value) {
  if (key.op_type_ == OP_PUT)
    table_[key] = value;
  else if (key.op_type_ == OP_DELETE)
    table_[key] = "";

  stat_.Update(key.Size(), value.size());
  return OK;
}

RC MemTable::Get(string_view key, string &value) {
  auto min_key = MemKey::NewMinKey(key);

  auto iter = table_.lower_bound(min_key);
  if (iter == table_.end()) {
    return NOT_FOUND;
  }

  if (key == iter->first.user_key_ &&
      iter->first.op_type_ != OpType::OP_DELETE) {
    value = iter->second;
    return OK;
  } else {
    return NOT_FOUND;
  }
}

RC MemTable::BuildSSTable(string_view dbname, string &sstable_path) {
  MLogger->info("memtable -> sstable");
  string true_path = FileManager::FixDirName(dbname);
  dbname = true_path;

  unsigned char sha256[SHA256_DIGEST_LENGTH];
  TempFile *temp_file = nullptr;
  auto rc = FileManager::OpenTempFile(dbname, "tmp_sst_", &temp_file);
  if (rc) return rc;
  auto sstable = new SSTableWriter(dbname, temp_file, *options_);

  if (rc = ForEach([&](const MemKey &memkey, string_view value) -> RC {
        string key = memkey.ToKey();
        return sstable->Add(key, value);
      });
      rc)
    return rc;
  if (rc = sstable->Final(sha256); rc) return rc;
  string sha256_hex = sha256_digit_to_hex(sha256);
  sstable_path = dbname;
  sstable_path += sha256_hex + ".sst";
  temp_file->ReName(sstable_path);
  temp_file->Close();

  MLogger->info("sstable {} created", sstable_path);
  delete sstable;
  delete temp_file;

  return OK;
}

RC MemTable::ForEach(
    std::function<RC(const MemKey &key, string_view value)> func) {
  for (auto iter = table_.begin(); iter != table_.end(); iter++) {
    const MemKey &memkey = iter->first;
    string_view value = iter->second;
    auto rc = func(memkey, value);
    if (rc) return rc;
  }
  return OK;
}

MemTable::MemTable(const DBOptions &options) : options_(&options) {}

size_t MemTable::GetMemTableSize() { return stat_.Sum(); }

}  // namespace adl
