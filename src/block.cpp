#include "block.hpp"
#include "encode.hpp"
#include "monitor_logger.hpp"

namespace adl {
BlockWriter::BlockWriter() : entries_len_(0) {}

/* [
    shared_key_len:4|
    unshared_key_len:4|
    value_len:4|
    key:shared_key_len + unshared_key_len|
    value:value_len
    ]
*/
RC BlockWriter::Add(string_view key, string_view value) {
  int value_len = (int)value.length();
  int key_len = (int)key.length();
  int shared_key_len = 0;
  int unshared_key_len;

  if (!(entries_len_ % restarts_block_len_)) {
    restarts_.push_back((int)buffer_.size());
  } else {
    auto min_len = std::min(key_len, (int)last_key_.length());
    /* 寻找当前 key 和 last_key 的共享前缀长度 */
    for (int i = 0; i < min_len; ++i) {
      if (key[i] != last_key_[i]) break;
      shared_key_len++;
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

RC BlockReader::Init(string_view data,
                     std::function<int(string_view, string_view)> &&cmp) {
  cmp_ = std::move(cmp);
  data_ = data;
  const char *buffer = data_.data();
  size_t data_len = data.length();
  /* restarts_len */
  int restarts_len;
  size_t restarts_len_offset = data_len - sizeof(int);
  Decode32(buffer + restarts_len_offset, &restarts_len);
  /* restarts */
  restarts_offset_ = restarts_len_offset - restarts_len * sizeof(int);
  for (int i = 0; i < restarts_len; i++) {
    int offset;
    Decode32(buffer + restarts_offset_ + i * sizeof(int), &offset);
    restarts_.push_back(offset);
  }
  return OK;
}

/* 注意传入的是 user_key，需要和磁盘上的 [user_key,seq,op_type] 进行比较 */
RC BlockReader::Get(string_view key, string &value) {
  int restarts_len = (int)restarts_.size();
  int key_len = (int)key.length();
  int index;
  /* 二分查找最近的小于 key 的重启点 如果 key 越界了，那就直接返回没找到 */
  auto rc = BsearchRestartPoint(key, &index);
  if (rc) return rc;
  if (index < 0 || index >= restarts_len) return NOT_FOUND;

  /* 剩下的线性扫描 */
  auto cur_entry = data_.data() + restarts_[index];
  string last_key;
  for (int i = 0; i < BlockWriter::restarts_block_len_ &&
                  cur_entry < data_.data() + restarts_offset_;
       i++) {
    int value_len;
    int unshared_key_len;
    int shared_key_len;
    Decode32(cur_entry, &shared_key_len);
    Decode32(cur_entry + sizeof(int), &unshared_key_len);
    Decode32(cur_entry + sizeof(int) * 2, &value_len);
    string cur_key;
    /* expect ok */
    if (last_key.length() >= shared_key_len) {
      cur_key = last_key.substr(0, shared_key_len);
    }
    cur_key.append(cur_entry + sizeof(int) * 3, (size_t)(unshared_key_len));
    int cmp = cmp_(cur_key, key);
    if (!cmp) {
      value.assign(cur_entry + sizeof(int) * 3 + unshared_key_len, value_len);
      return OK;
    } else if (cmp < 0) {
      cur_entry += sizeof(int) * 3 + unshared_key_len + value_len;
    }
    last_key = std::move(cur_key);
  }
  return NOT_FOUND;
}

RC DecodeRestartsPointKey(const char *restart_record, int *shared_key_len,
                          int *unshared_key_len, int *value_len,
                          string_view &restarts_key) {
  Decode32(restart_record, shared_key_len);
  if (*shared_key_len != 0) return UN_SUPPORTED_FORMAT;
  Decode32(restart_record + sizeof(int), unshared_key_len);
  Decode32(restart_record + sizeof(int) * 2, value_len);
  restarts_key = {restart_record + sizeof(int) * 3,
                  (size_t)(*unshared_key_len)};
  return OK;
}

/* [user,seq,op] [user] */
int CmpKeyAndUserKey(string_view key, string_view user_key) {
  string_view key_user_key = key.substr(0, key.size() - 9);
  return key_user_key.compare(user_key);
}

RC BlockReader::BsearchRestartPoint(string_view key, int *index) {
  int restarts_len = (int)restarts_.size();
  int left = 0, right = restarts_len - 1;
  int value_len;
  int unshared_key_len;
  int shared_key_len;
  string_view restarts_key;
  RC rc;
  /* 二分找到恰好小于等于 key 的重启点 */
  while (left <= right) {
    int mid = (left + right) >> 1;
    const char *restart_record = data_.data() + restarts_[mid];

    if (RC rc = DecodeRestartsPointKey(data_.data() + restarts_[mid],
                                       &shared_key_len, &unshared_key_len,
                                       &value_len, restarts_key);
        rc) {
      MLogger->error("DecodeRestartsPointKey error: {}", strrc(rc));
      return rc;
    }
    if (cmp_(restarts_key, key) < 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }

  if (left != restarts_len) {
    if ((rc = DecodeRestartsPointKey(data_.data() + restarts_[left],
                                     &shared_key_len, &unshared_key_len,
                                     &value_len, restarts_key))) {
      MLogger->error("DecodeRestartsPointKey error: {}", strrc(rc));
      return rc;
    }
    if (!cmp_(restarts_key, key)) {
      *index = left;
      return OK;
    }
  }

  *index = right;
  return OK;
}
}  // namespace adl