#ifndef ADL_LSM_TREE_MEM_TABLE_H__
#define ADL_LSM_TREE_MEM_TABLE_H__

#include <string.h>
#include <map>
#include <string>
#include "rc.hpp"

namespace adl {

using namespace std;

enum OpType {
  OP_PUT,
  OP_DELETE,
};

struct MemKey {
  string user_key_;
  int64_t seq_;  // sequence number
  enum OpType op_type_;

  MemKey(const char *str, int64_t seq, enum OpType op_type)
      : user_key_(str), seq_(seq), op_type_(op_type) {}

  MemKey(const string &uk) : user_key_(uk), seq_(0), op_type_(OP_PUT) {}

  MemKey(const string &uk, int64_t s, enum OpType o)
      : user_key_(uk), seq_(s), op_type_(o) {}
  /* 理想中 memtable 应当不会有相同的元素 比如 [key1,3] [key1,3]  */
  bool operator<(const MemKey &other) const {
    if (user_key_ == other.user_key_) {
      return seq_ > other.seq_;
    }
    return user_key_ < other.user_key_;
  }
  string ToKey() {
    string ret(user_key_);
    char temp[8];
    memcpy(temp, &seq_, 8);
    ret.append(temp, 8);
    memcpy(temp, &op_type_, 1);
    return ret;
  }

  /* user_key 绑定最大序列号作为查询的依据 */
  static MemKey NewMinKey(const string &uk) {
    MemKey m(string(uk), INT64_MAX, OP_PUT);
    return m;
  }

  size_t Size() const { return user_key_.size() + 8 + 1; }
};

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
  RC Put(const MemKey &key, const string &value);
  RC Get(const string &key, string &value);
  RC BuildSSTable(const string &dbname);
  size_t GetMemTableSize();

 private:
  /* mutable mutex mu_;  */
  const DBOptions *options_;
  map<MemKey, string> table_;

  Stat stat_; /* 整个内存表的状态 */
};

}  // namespace adl
#endif  // ADL_LSM_TREE_MEM_TABLE_H__