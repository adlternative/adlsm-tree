#include "sstable.hpp"
#include <openssl/sha.h>
#include <iomanip>
#include <sstream>
#include "db.hpp"
#include "file_util.hpp"

namespace adl {

string sha256_digit_to_hex(unsigned char hash[SHA256_DIGEST_LENGTH]) {
  stringstream ss;
  for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
    ss << hex << setw(2) << setfill('0') << (int)hash[i];
  }
  return ss.str();
}

SSTable::SSTable(string_view dbname, WritAbleFile *file,
                 const DBOptions &options)
    : dbname_(dbname),
      file_(file),
      offset_(0),
      filter_block_(make_unique<BloomFilter>(options.bits_per_key)) {
  SHA256_Init(&sha256_);
}

RC SSTable::Add(string_view key, string_view value) {
  /* add K to filter block */
  filter_block_.Update(key);
  /* add <K,V> to data block */
  auto rc = data_block_.Add(key, value);
  if (rc) return rc;
  last_key_ = key;
  if (data_block_.EstimatedSize() > need_flush_size_) FlushDataBlock();
  return OK;
}

RC SSTable::FlushDataBlock() {
  /* write <K,V>(s) from data block to file */
  data_block_.Final(buffer_);
  SHA256_Update(&sha256_, buffer_.c_str(), buffer_.size());
  auto rc = file_->Append(buffer_);
  if (rc) return rc;
  data_block_handle_.SetMeta(offset_, (int)buffer_.size());
  offset_ += (int)buffer_.size();
  data_block_.Reset();
  /* add <K, {offset,size}> to index_block */
  string encoded_data_block_handle;
  data_block_handle_.EncodeMeta(encoded_data_block_handle);
  /* 最大key （目前不用 leveldb 那种优化） */
  index_block_.Add(last_key_, encoded_data_block_handle);
  return OK;
}

RC SSTable::Final(unsigned char sha256_digit[]) {
  /* 将剩余的数据块刷盘 */
  if (!data_block_.Empty()) FlushDataBlock();
  /* Filter 块 */
  filter_block_.Final(buffer_);
  SHA256_Update(&sha256_, buffer_.c_str(), buffer_.size());
  file_->Append(buffer_);
  filter_block_handle_.SetMeta(offset_, (int)buffer_.size());
  offset_ += (int)buffer_.size();
  string encoded_filter_block_handle;
  data_block_handle_.EncodeMeta(encoded_filter_block_handle);
  /* Meta 块 */
  meta_data_block_.Add("filter", encoded_filter_block_handle);
  meta_data_block_.Final(buffer_);
  SHA256_Update(&sha256_, buffer_.c_str(), buffer_.size());
  file_->Append(buffer_);
  meta_data_block_handle_.SetMeta(offset_, (int)buffer_.size());
  offset_ += (int)buffer_.size();
  /* Index 块 */
  index_block_.Final(buffer_);
  SHA256_Update(&sha256_, buffer_.c_str(), buffer_.size());
  file_->Append(buffer_);
  index_block_handle_.SetMeta(offset_, (int)buffer_.size());
  offset_ += (int)buffer_.size();
  /* Footer 块 */
  string encode_meta_block_handle, encode_index_block_handle;
  meta_data_block_handle_.EncodeMeta(encode_meta_block_handle);
  index_block_handle_.EncodeMeta(encode_index_block_handle);
  foot_block_.Add(encode_meta_block_handle, encode_index_block_handle);
  foot_block_.Final(buffer_);
  SHA256_Update(&sha256_, buffer_.c_str(), buffer_.size());
  file_->Append(buffer_);
  offset_ += (int)buffer_.size();

  SHA256_Final(sha256_digit, &sha256_);
  return OK;
}

}  // namespace adl