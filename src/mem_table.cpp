#include "mem_table.hpp"
#include <string.h>
#include "file_util.hpp"
#include "sstable.hpp"

namespace adl {

RC MemTable::Put(const MemKey &key, const string &value) {
  if (key.op_type_ == OP_PUT)
    table_[key] = value;
  else if (key.op_type_ == OP_DELETE)
    table_[key] = "";

  return OK;
}

RC MemTable::Get(const string &key, string &value) {
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

RC MemTable::BuildSSTable(const string &dbname) {
  TempFile *temp_file = nullptr;
  auto rc = FileManager::OpenTempFile(&temp_file);
  if (rc != OK) return rc;

  auto sstable = new SSTable(dbname, temp_file, *options_);

  for (auto iter = table_.begin(); iter != table_.end(); iter++) {
    MemKey memkey = iter->first;
    string &value = iter->second;
    string key = memkey.ToKey();
    rc = sstable->Add(key, value);
    if (rc != OK) return rc;
  }
  sstable->Final();

  auto new_sstable_name = dbname + "/" + "temp.sst";
  temp_file->ReName(new_sstable_name);
  temp_file->Close();
  delete sstable;
  delete temp_file;

  return OK;
}

MemTable::MemTable(const DBOptions &options) : options_(&options) {}

}  // namespace adl
