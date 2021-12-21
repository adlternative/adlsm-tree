#include "keys.hpp"
#include <string.h>
#include "encode.hpp"
namespace adl {

string_view InnerKeyToUserKey(string_view inner_key) {
  return inner_key.substr(0, inner_key.length() - 9);
}

int CmpInnerKey(string_view k1, string_view k2) {
  int ret = InnerKeyToUserKey(k1).compare(InnerKeyToUserKey(k2));
  if (ret) {
    return ret;
  }
  /* seq op 反过来比较 */
  return -k1.substr(k1.size() - 9).compare(k2.substr(k2.size() - 9));
}

int CmpKeyAndUserKey(string_view key, string_view user_key) {
  string_view key_user_key = key.substr(0, key.size() - 9);
  return key_user_key.compare(user_key);
}

RC SaveResultValueIfUserKeyMatch(string_view rk, string_view rv, string_view tk,
                                 string &dv) {
  if (InnerKeyToUserKey(rk).compare(InnerKeyToUserKey(tk))) return NOT_FOUND;
  if (InnerKeyOpType(rk) == OP_DELETE) return NOT_FOUND;
  dv.assign(rv.data(), rv.length());
  return OK;
}

MemKey::MemKey(string_view str, int64_t seq, OpType op_type)
    : user_key_(str), seq_(seq), op_type_(op_type) {}

int64_t InnerKeySeq(string_view inner_key) {
  int64_t seq;
  memcpy(&seq, &inner_key[inner_key.length() - 9], 8);
  return seq;
}

OpType InnerKeyOpType(string_view inner_key) {
  OpType op_type = (OpType)inner_key[inner_key.length() - 1];
  return op_type;
}

bool MemKey::operator<(const MemKey &other) const {
  if (user_key_ < other.user_key_)
    return true;
  else if (user_key_ == other.user_key_) {
    if (seq_ > other.seq_)
      return true;
    else if (seq_ == other.seq_) {
      if (op_type_ > other.op_type_) return true;
    }
  }
  return false;
}

std::string MemKey::ToKey() const {
  string ret(user_key_);
  char temp[8];
  memcpy(temp, &seq_, 8);
  ret.append(temp, 8);
  temp[0] = op_type_;
  ret.append(temp, 1);
  return ret;
}

void MemKey::FromKey(string_view key) {
  auto len = key.size();
  op_type_ = (OpType)key[len - 1];
  memcpy(&seq_, &key[len - 9], 8);
  user_key_ = string(key.data(), key.size() - 9);
}

MemKey MemKey::NewMinMemKey(string_view uk) {
  MemKey m(string(uk), INT64_MAX, OP_PUT);
  return m;
}

string NewMinInnerKey(string_view key) {
  MemKey mk = MemKey::NewMinMemKey(key);
  return mk.ToKey();
}
int EasyCmp(std::string_view key1, std::string_view key2) {
  return key1.compare(key2);
}
adl::RC EasySave(string_view rk, string_view rv, string_view tk, string &dv) {
  dv.assign(rv.data(), rv.length());
  return adl::OK;
}

string EncodeKVPair(const MemKey &key, string_view value) {
  string kv_pair;
  EncodeWithPreLen(kv_pair, key.ToKey());
  EncodeWithPreLen(kv_pair, value);
  return kv_pair;
}

void DecodeKVPair(string_view data, MemKey &memkey, string &value) {
  string key;
  int pos = DecodeWithPreLen(key, data);
  pos = DecodeWithPreLen(value, data.substr(pos));
  memkey.FromKey(key);
}

}  // namespace adl