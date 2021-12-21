#ifndef ADL_LSM_TREE_MEM_TABLE_H__
#define ADL_LSM_TREE_MEM_TABLE_H__

#include <string.h>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include "keys.hpp"
#include "rc.hpp"
#include "wal.hpp"

namespace adl {

using namespace std;

class FileMetaData;

struct DBOptions;
class MemTable {
 public:
  struct Stat {
    Stat() : keys_size_(0), values_size_(0) {}
    void Update(size_t key_size, size_t value_size) {
      keys_size_ += key_size;
      values_size_ += value_size;
    }
    size_t Sum() { return keys_size_ + values_size_; }
    size_t keys_size_;
    size_t values_size_;
  };

  MemTable(const DBOptions &options);
  MemTable(const DBOptions &options, WAL *wal);
  ~MemTable() {
    if (wal_) delete wal_;
  }
  RC Put(const MemKey &key, string_view value);
  RC PutTeeWAL(const MemKey &key, string_view value);

  RC Get(string_view key, string &value);
  RC GetNoLock(string_view key, string &value);
  RC ForEachNoLock(
      std::function<RC(const MemKey &key, string_view value)> &&func);
  RC BuildSSTable(string_view dbname, FileMetaData **meta_data_pointer);
  size_t GetMemTableSize();
  RC DropWAL();
  bool Empty();

 private:
  mutable mutex mu_;
  const DBOptions *options_;
  map<MemKey, string> table_;
  Stat stat_; /* 整个内存表的状态 */
  WAL *wal_;
};

}  // namespace adl
#endif  // ADL_LSM_TREE_MEM_TABLE_H__