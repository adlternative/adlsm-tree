#ifndef ADL_LSM_TREE_MEM_TABLE_H__
#define ADL_LSM_TREE_MEM_TABLE_H__

#include <string.h>
#include <functional>
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

  MemKey(string_view str, int64_t seq = 0, enum OpType op_type = OP_PUT)
      : user_key_(str), seq_(seq), op_type_(op_type) {}

  /* 理想中 memtable 应当不会有相同的元素 比如 [key1,3] [key1,3]  */
  bool operator<(const MemKey &other) const {
    if (user_key_ == other.user_key_) {
      return seq_ > other.seq_;
    }
    return user_key_ < other.user_key_;
  }
  string ToKey() const {
    string ret(user_key_);
    char temp[8];
    memcpy(temp, &seq_, 8);
    ret.append(temp, 8);
    temp[0] = op_type_;
    ret.append(temp, 1);
    return ret;
  }

  void FromKey(string_view key) {
    auto len = key.size();
    op_type_ = (OpType)key[len - 1];
    memcpy(&seq_, &key[len - 9], 8);
    user_key_ = string(key.data(), key.size() - 9);
  }

  /* user_key 绑定最大序列号作为查询的依据 */
  static MemKey NewMinKey(string_view uk) {
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