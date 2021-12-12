#ifndef ADL_LSM_TREE_MEM_TABLE_H__
#define ADL_LSM_TREE_MEM_TABLE_H__

#include <string.h>
#include <functional>
#include <map>
#include <string>
#include "keys.hpp"
#include "rc.hpp"

namespace adl {

using namespace std;



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
  RC Put(const MemKey &key, string_view value);
  RC Get(string_view key, string &value);
  RC ForEach(std::function<RC(const MemKey &key, string_view value)> func);
  RC BuildSSTable(string_view dbname, string &sstable_path);
  size_t GetMemTableSize();

 private:
  /* mutable mutex mu_;  */
  const DBOptions *options_;
  map<MemKey, string> table_;

  Stat stat_; /* 整个内存表的状态 */
};

}  // namespace adl
#endif  // ADL_LSM_TREE_MEM_TABLE_H__