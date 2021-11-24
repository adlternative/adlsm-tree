#include "mem_table.hpp"
#include <string.h>

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

  auto iter = table_.upper_bound(min_key);
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

}  // namespace adl
