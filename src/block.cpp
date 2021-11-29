#include "block.hpp"

namespace adl {
Block::Block() : entries_len_(0) {}

RC Block::Add(const string &key, const string &value) {
  int value_len = value.length();
  int key_len = key.length();
  int shared_key_len = 0;
  int unshared_key_len;

  if (!(entries_len_ % 12)) {
    restarts_.push_back(buffer_.size());
  } else {
    auto min_len = std::min(key_len, (int)last_key_.length());
    /* 寻找当前 key 和 last_key 的共享长度 */
    for (int i = 0; i < min_len; ++i) {
      if (key[i] == last_key_[i]) {
        shared_key_len++;
      }
    }
  }

  unshared_key_len = key_len - shared_key_len;
  buffer_.append(reinterpret_cast<char *>(&shared_key_len), sizeof(int));
  buffer_.append(reinterpret_cast<char *>(&unshared_key_len), sizeof(int));
  buffer_.append(reinterpret_cast<char *>(&value_len), sizeof(int));
  buffer_.append(key, shared_key_len, unshared_key_len);
  buffer_.append(value);

  entries_len_++;
  last_key_ = key;
  return OK;
}

RC Block::Finish(string &ret) {
  int restarts_len = restarts_.size();
  for (int i = 0; i < restarts_len; i++) {
    buffer_.append(reinterpret_cast<char *>(&restarts_[i]), sizeof(int));
  }
  buffer_.append(reinterpret_cast<char *>(&restarts_len), sizeof(int));
  ret = std::move(buffer_);
  return OK;
}

}  // namespace adl