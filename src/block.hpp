#ifndef ADL_LSM_TREE_BLOCK_H__
#define ADL_LSM_TREE_BLOCK_H__

#include <string>
#include <vector>
#include "rc.hpp"

namespace adl {
using namespace std;

class BlockWriter {
 public:
  static constexpr unsigned int restarts_block_len_ = 12;
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

struct BlockHandle {
  int block_offset_ = 0;
  int block_size_ = 0;

  void EncodeMeta(string &ret) {
    ret.append((char *)&block_offset_, sizeof(int));
    ret.append((char *)&block_size_, sizeof(int));
  }

  void SetMeta(int block_offset, int block_size) {
    block_offset_ = block_offset;
    block_size_ = block_size;
  }
};

}  // namespace adl

#endif  // ADL_LSM_TREE_BLOCK_H__