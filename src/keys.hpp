#ifndef ADL_LSM_TREE_KEYS_H__
#define ADL_LSM_TREE_KEYS_H__
#include <string>
#include <string_view>
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

  MemKey(const MemKey &rhs)
      : user_key_(rhs.user_key_), seq_(rhs.seq_), op_type_(rhs.op_type_) {}
  MemKey &operator=(const MemKey &rhs) {
    if (this != &rhs) {
      user_key_ = rhs.user_key_;
      seq_ = rhs.seq_;
      op_type_ = rhs.op_type_;
    }
    return *this;
  }

  MemKey(MemKey &&rhs) noexcept
      : user_key_(std::move(rhs.user_key_)),
        seq_(rhs.seq_),
        op_type_(rhs.op_type_) {}

  MemKey &operator=(MemKey &&rhs) noexcept {
    if (this != &rhs) {
      user_key_ = std::move(rhs.user_key_);
      seq_ = rhs.seq_;
      op_type_ = rhs.op_type_;
    }
    return *this;
  }

  MemKey() = default;
  MemKey(string_view str, int64_t seq = 0, enum OpType op_type = OP_PUT);
  friend ostream &operator<<(ostream &os, const MemKey &key);

  /* 理想中 memtable 应当不会有相同的元素 比如 [key1,3] [key1,3]  */
  bool operator<(const MemKey &other) const;
  string ToKey() const;
  void FromKey(string_view key);

  /* user_key 绑定最大序列号作为查询的依据 */
  static MemKey NewMinMemKey(string_view uk);

  size_t Size() const { return user_key_.size() + 8 + 1; }
};

string_view InnerKeyToUserKey(string_view inner_key);
int64_t InnerKeySeq(string_view inner_key);
OpType InnerKeyOpType(string_view inner_key);

int CmpInnerKey(string_view k1, string_view k2);
int CmpUserKeyOfInnerKey(string_view k1, string_view k2);
int CmpKeyAndUserKey(string_view key, string_view user_key);
RC SaveResultValueIfUserKeyMatch(string_view rk, string_view rv, string_view tk,
                                 string &dv);
string NewMinInnerKey(string_view key);

int EasyCmp(std::string_view key1, std::string_view key2);
adl::RC EasySave(string_view rk, string_view rv, string_view tk, string &dv);

string EncodeKVPair(const MemKey &key, string_view value);
void DecodeKVPair(string_view data, MemKey &memkey, string &value);

}  // namespace adl
#endif  // ADL_LSM_TREE_KEYS_H__