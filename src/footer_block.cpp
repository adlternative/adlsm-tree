#include "footer_block.hpp"

namespace adl {
RC FooterBlock::Add(string_view meta_block_handle,
                    string_view index_block_handle) {
  meta_block_handle_ = meta_block_handle;
  index_block_handle_ = index_block_handle;
  return OK;
}

RC FooterBlock::Final(string &result) {
  static const char c[2] = {0x12, 0x34};
  string footer;
  int meta_block_handle_len = (int)meta_block_handle_.length();
  int index_block_handle_len = (int)index_block_handle_.length();

  footer.append(meta_block_handle_);
  footer.append((char *)&meta_block_handle_len, sizeof(int));

  footer.append(index_block_handle_);
  footer.append((char *)&index_block_handle_len, sizeof(int));

  footer.append(c, 2);
  result = std::move(footer);
  return OK;
}

}  // namespace adl
