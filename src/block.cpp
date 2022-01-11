#include "block.hpp"
#include <cstddef>
#include "encode.hpp"
#include "keys.hpp"
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

RC BlockReader::Init(
    string_view data, std::function<int(string_view, string_view)> &&cmp,
    std::function<RC(string_view, string_view, string_view innner_key,
                     string &value)> &&handle_result) {
  cmp_fn_ = std::move(cmp);
  handle_result_fn_ = std::move(handle_result);
  data_ = data;
  const char *buffer = data_.data();
  size_t data_len = data_.length();
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

/* 找到恰好大于等于 inner_key 的项执行 handle_result() */
RC BlockReader::GetInternal(
    string_view inner_key,
    const std::function<RC(string_view, string_view)> &handle_result) {
  int restarts_len = (int)restarts_.size();
  int key_len = (int)inner_key.length();
  int index = 0;
  /* 二分查找最近的小于 key 的重启点 如果 key 越界了，那就直接返回没找到 */
  auto rc = BsearchRestartPoint(inner_key, &index);
  if (rc) return rc;
  if (index >= restarts_len) return NOT_FOUND;
  if (index == -1) {
    const char *restart_record = data_.data() + restarts_[0];
    string_view key;
    string_view val;
    if (rc = DecodeRestartsPointKeyAndValueWrap(restart_record, key, val); rc)
      return rc;
    return handle_result(key, val);
  }

  /* 剩下的线性扫描 找到恰好大于等于 inner_key 的地方 */
  auto cur_entry = data_.data() + restarts_[index];
  string last_key;
  int i;

  for (i = 0; i < BlockWriter::restarts_block_len_ &&
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
    if (last_key.length() >= shared_key_len)
      cur_key = last_key.substr(0, shared_key_len);
    cur_key.append(cur_entry + sizeof(int) * 3, (size_t)(unshared_key_len));
    int cmp = cmp_fn_(cur_key, inner_key);
    /* 恰好大于等于 （而非强制等于） */
    if (cmp >= 0) {
      return handle_result(
          cur_key,
          {cur_entry + sizeof(int) * 3 + unshared_key_len, (size_t)value_len});
    } else {
      cur_entry += sizeof(int) * 3 + unshared_key_len + value_len;
    }
    last_key = std::move(cur_key);
  }

  /* 还没有越界 我们检测下一个重启点 */
  if (i == BlockWriter::restarts_block_len_ &&
      cur_entry < data_.data() + restarts_offset_) {
    string_view key;
    string_view val;
    if (rc = DecodeRestartsPointKeyAndValueWrap(cur_entry, key, val); rc)
      return rc;
    return handle_result(key, val);
  }

  return NOT_FOUND;
}

RC BlockReader::Get(string_view innner_key, string &value) {
  return GetInternal(
      innner_key, [&](string_view result_key, string_view result_value) -> RC {
        return handle_result_fn_(result_key, result_value, innner_key, value);
      });
}

RC DecodeRestartsPointKeyAndValue(const char *restart_record,
                                  int *shared_key_len, int *unshared_key_len,
                                  int *value_len, string_view &restarts_key,
                                  string_view &restarts_value) {
  Decode32(restart_record, shared_key_len);
  if (*shared_key_len != 0) return UN_SUPPORTED_FORMAT;
  Decode32(restart_record + sizeof(int), unshared_key_len);
  Decode32(restart_record + sizeof(int) * 2, value_len);
  restarts_key = {restart_record + sizeof(int) * 3,
                  (size_t)(*unshared_key_len)};
  restarts_value = {restart_record + sizeof(int) * 3 + *unshared_key_len,
                    (size_t)(*value_len)};
  return OK;
}

RC DecodeRestartsPointKeyAndValueWrap(const char *restart_record,
                                      string_view &restarts_key,
                                      string_view &restarts_value) {
  int shared_key_len, unshared_key_len, value_len;
  return DecodeRestartsPointKeyAndValue(restart_record, &shared_key_len,
                                        &unshared_key_len, &value_len,
                                        restarts_key, restarts_value);
}

RC DecodeRestartsPointKeyWrap(const char *restart_record,
                              string_view &restarts_key) {
  string_view unused_restarts_value;
  return DecodeRestartsPointKeyAndValueWrap(restart_record, restarts_key,
                                            unused_restarts_value);
}

RC DecodeRestartsPointValueWrap(const char *restart_record,
                                string_view &restarts_value) {
  string_view unused_restarts_key;
  return DecodeRestartsPointKeyAndValueWrap(restart_record, unused_restarts_key,
                                            restarts_value);
}

RC BlockReader::BsearchRestartPoint(string_view key, int *index) {
  int restarts_len = (int)restarts_.size();
  int left = 0, right = restarts_len - 1;
  int value_len;
  int unshared_key_len;
  int shared_key_len;
  string_view restarts_key;
  RC rc = OK;
  /* 二分找到恰好小于等于 key 的重启点 */
  while (left <= right) {
    int mid = (left + right) >> 1;
    const char *restart_record = data_.data() + restarts_[mid];

    if (RC rc = DecodeRestartsPointKeyWrap(restart_record, restarts_key); rc) {
      MLog->error("DecodeRestartsPointKey error: {}", strrc(rc));
      return rc;
    }
    if (cmp_fn_(restarts_key, key) < 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }

  /* 如果 left == restarts_len 则 index 为 right */
  if (left != restarts_len) {
    /* 否则看下 left 是否和 key 相等如果是则直接返回left */
    const char *restart_record = data_.data() + restarts_[left];
    if (rc = DecodeRestartsPointKeyWrap(restart_record, restarts_key); rc) {
      MLog->error("DecodeRestartsPointKey error: {}", strrc(rc));
      return rc;
    }
    if (!cmp_fn_(restarts_key, key)) {
      *index = left;
      return OK;
    }
  }

  *index = right;
  return OK;
}

}  // namespace adl