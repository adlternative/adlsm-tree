#include "footer_block.hpp"

namespace adl {
RC FooterBlockWriter::Add(string_view meta_block_handle,
                          string_view index_block_handle) {
  meta_block_handle_ = meta_block_handle;
  index_block_handle_ = index_block_handle;
  return OK;
}

RC FooterBlockWriter::Final(string &result) {
  static const char magic_number[2] = {0x12, 0x34};
  string footer;

  if (meta_block_handle_.length() != 8 || index_block_handle_.length() != 8)
    return UN_SUPPORTED_FORMAT;

  /* expect 8 */
  footer.append(meta_block_handle_);
  /* expect 8 */
  footer.append(index_block_handle_);
  /* 2 */
  footer.append(magic_number, 2);

  if (footer_size != footer.length()) return UN_SUPPORTED_FORMAT;

  result = std::move(footer);

  return OK;
}

RC FooterBlockReader::Init(string_view footer_buffer) {
  footer_buffer_ = footer_buffer;
  /* MAGIC NUMBER */
  if (footer_buffer_.length() != FooterBlockWriter::footer_size ||
      footer_buffer_[FooterBlockWriter::footer_size - 2] != 0x12 ||
      footer_buffer_[FooterBlockWriter::footer_size - 1] != 0x34)
    return UN_SUPPORTED_FORMAT;

  meta_block_handle_.DecodeFrom(footer_buffer_.substr(0, 8));
  index_block_handle_.DecodeFrom(footer_buffer_.substr(8, 16));
  return OK;
}

}  // namespace adl
