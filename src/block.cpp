#include "block.hpp"

namespace adl {
BlockWriter::BlockWriter() : entries_len_(0) {}

RC BlockWriter::Add(string_view key, string_view value) {
  int value_len = (int)value.length();
  int key_len = (int)key.length();
  int shared_key_len = 0;
  int unshared_key_len;

  if (!(entries_len_ % restarts_block_len_)) {
    restarts_.push_back((int)buffer_.size());
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

RC BlockWriter::Final(string &result) {
  /* 添加重启点偏移量及其长度 */
  int restarts_len = (int)restarts_.size();
  for (int i = 0; i < restarts_len; i++) {
    buffer_.append(reinterpret_cast<char *>(&restarts_[i]), sizeof(int));
  }
  buffer_.append(reinterpret_cast<char *>(&restarts_len), sizeof(int));
  result = std::move(buffer_);
  return OK;
}

size_t BlockWriter::EstimatedSize() {
  return buffer_.size() + (restarts_.size() + 1) * sizeof(int);
}

void BlockWriter::Reset() {
  entries_len_ = 0;
  buffer_.clear();
  restarts_.clear();
  last_key_.clear();
  return;
}

bool BlockWriter::Empty() { return !entries_len_; }

}  // namespace adl