#ifndef ADL_LSM_TREE_BLOCK_H__
#define ADL_LSM_TREE_BLOCK_H__

#include <cassert>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include "encode.hpp"
#include "rc.hpp"

namespace adl {
using namespace std;

#define RESTARTS_BLOCK_LEN 12
/* 今日盛开的花，将在明日凋谢 */

class BlockWriter {
 public:
  BlockWriter();
  RC Add(string_view key, string_view value);
  RC Final(string &result);
  size_t EstimatedSize();
  void Reset();
  bool Empty();

 private:
  std::vector<int> restarts_;  //  重启点
  string last_key_;
  int entries_len_;
  string buffer_;
};

class BlockReader : public enable_shared_from_this<BlockReader> {
 public:
  class Iterator {
   public:
    Iterator(shared_ptr<BlockReader> &&container, size_t restarts_block_idx = 0
             /* size_t entries_idx = 0 */);

    Iterator()
        : restarts_block_idx_(0),
          entries_idx_(0),
          value_len_(0),
          unshared_key_len_(0),
          shared_key_len_(0),
          cur_entry_(nullptr),
          valid_(false) {}

    Iterator &operator++();

    Iterator operator++(int);

    int operator<=>(Iterator &rhs) {
      assert(container_ == rhs.container_);

      if (cur_entry_ > rhs.cur_entry_)
        return 1;
      else if (cur_entry_ < rhs.cur_entry_)
        return -1;
      return 0;
    }

    Iterator &operator=(const Iterator &rhs);

    Iterator(const Iterator &rhs);

    Iterator &operator=(Iterator &&rhs) noexcept;

    Iterator(Iterator &&rhs) noexcept;

    bool operator==(Iterator &rhs) {
      return container_ == rhs.container_ && cur_entry_ == rhs.cur_entry_;
    }

    bool operator!=(Iterator &rhs) { return !(*this == rhs); }

    bool Valid() { return valid_; }

    shared_ptr<BlockReader> GetContainer() { return container_; }
    operator bool() {
      if (!container_) return false;
      if (*this >= container_->end()) return false;

      return true;
    }

    void Fetch();
    void FetchWithoutValue();

    const string_view Key() const { return cur_key_; }
    const string_view Value() const { return cur_value_; }

   private:
    void SetInValid();

    size_t restarts_block_idx_;
    size_t entries_idx_;
    shared_ptr<BlockReader> container_;

    const char *cur_entry_;
    int value_len_;
    int unshared_key_len_;
    int shared_key_len_;

    bool valid_;
    string cur_key_;
    string cur_value_;
  };
  friend class Iterator;

  Iterator begin() { return Iterator(shared_from_this(), 0); }

  Iterator end() {
    { return Iterator(shared_from_this(), restarts_.size()); }
  }

  BlockReader() = default;
  RC Init(string_view data, std::function<int(string_view, string_view)> &&cmp,
          std::function<RC(string_view result_key, string_view result_value,
                           string_view want_inner_key, string &key,
                           string &value)> &&handle_result);

  /* 点查 */
  RC Get(string_view want_key, string &key, string &value);

 private:
  RC BsearchRestartPoint(string_view key, int *index);
  RC GetInternal(
      string_view key,
      const std::function<RC(string_view, string_view)> &handle_result);

  string_view data_;        /* [data][restarts] */
  string_view data_buffer_; /* [data] */
  /* 既是重启点数组的起点偏移量，也是数据项的结束偏移量 */
  size_t restarts_offset_;
  std::vector<int> restarts_;  //  重启点
  std::function<int(string_view, string_view)> cmp_fn_;
  std::function<RC(string_view, string_view, string_view, string &, string &)>
      handle_result_fn_;
};

struct BlockHandle {
  int block_offset_ = 0;
  int block_size_ = 0;

  void EncodeMeta(string &ret) {
    ret.append((char *)&block_offset_, sizeof(int));
    ret.append((char *)&block_size_, sizeof(int));
  }

  void DecodeFrom(string_view src) {
    block_offset_ = *(int *)src.data();
    block_size_ = *(int *)(src.data() + sizeof(int));
  }

  void SetMeta(int block_offset, int block_size) {
    block_offset_ = block_offset;
    block_size_ = block_size;
  }
};

struct BlockCacheHandle {
  string oid_;
  int offset_;
  BlockCacheHandle() = default;
  BlockCacheHandle(const string &oid, int offset)
      : oid_(oid), offset_(offset) {}

  bool operator==(const BlockCacheHandle &h) const {
    return h.oid_ == oid_ && h.offset_ == offset_;
  }
};

RC DecodeRestartsPointKeyWrap(const char *restart_record,
                              string_view &restarts_key);
RC DecodeRestartsPointValueWrap(const char *restart_record,
                                string_view &restarts_value);

RC DecodeRestartsPointKeyAndValue(const char *restart_record,
                                  int *shared_key_len, int *unshared_key_len,
                                  int *value_len, string_view &restarts_key,
                                  string_view &restarts_value);

RC DecodeRestartsPointKeyAndValueWrap(const char *restart_record,
                                      string_view &restarts_key,
                                      string_view &restarts_value);

}  // namespace adl

namespace std {
template <>  // function-template-specialization
class hash<adl::BlockCacheHandle> {
 public:
  size_t operator()(const adl::BlockCacheHandle &h) const {
    return hash<string>()(h.oid_) ^ hash<int>()(h.offset_);
  }
};

};  // namespace std

#endif  // ADL_LSM_TREE_BLOCK_H__