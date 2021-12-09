#ifndef ADL_LSM_TREE_FOOTER_BLOCK_H__
#define ADL_LSM_TREE_FOOTER_BLOCK_H__

#include <string>
#include "block.hpp"
#include "rc.hpp"

namespace adl {
using namespace std;

class FooterBlockWriter {
 public:
  RC Add(string_view meta_block_handle, string_view index_block_handle);

  RC Final(string &result);
  static constexpr int footer_size = 2 + 8 * 2;

 private:
  string_view meta_block_handle_;
  string_view index_block_handle_;
};

class FooterBlockReader {
 public:
  FooterBlockReader() = default;
  RC Init(string_view footer_buffer);
  const BlockHandle &meta_block_handle() const { return meta_block_handle_; }
  const BlockHandle &index_block_handle() const { return index_block_handle_; }

 private:
  string_view footer_buffer_;

  BlockHandle meta_block_handle_;
  BlockHandle index_block_handle_;
};

}  // namespace adl

#endif  // ADL_LSM_TREE_FOOTER_BLOCK_H__