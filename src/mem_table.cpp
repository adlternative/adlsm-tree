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

RC MemTable::BuildSSTable(string_view dbname) {
  MLogger->info("memtable -> sstable");

  unsigned char sha256[SHA256_DIGEST_LENGTH];
  TempFile *temp_file = nullptr;
  auto rc = FileManager::OpenTempFile(&temp_file);
  if (rc) return rc;
  auto sstable = new SSTable(dbname, temp_file, *options_);

  for (auto iter = table_.begin(); iter != table_.end(); iter++) {
    MemKey memkey = iter->first;
    string &value = iter->second;
    string key = memkey.ToKey();
    rc = sstable->Add(key, value);
    if (rc) return rc;
  }

  rc = sstable->Final(sha256);
  if (rc) return rc;
  string sha256_hex = sha256_digit_to_hex(sha256);
  string new_sstable_name(dbname);
  new_sstable_name += "/" + sha256_hex + ".sst";
  temp_file->ReName(new_sstable_name);
  temp_file->Close();

  MLogger->info("sstable {} created", new_sstable_name);
  delete sstable;
  delete temp_file;

  return OK;
}

MemTable::MemTable(const DBOptions &options) : options_(&options) {}

size_t MemTable::GetMemTableSize() { return stat_.Sum(); }

}  // namespace adl
