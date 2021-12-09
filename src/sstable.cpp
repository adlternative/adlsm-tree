#include "sstable.hpp"
#include <openssl/sha.h>
#include <iomanip>
#include <sstream>
#include "block.hpp"
#include "db.hpp"
#include "file_util.hpp"
#include "monitor_logger.hpp"

namespace adl {

string sha256_digit_to_hex(unsigned char hash[SHA256_DIGEST_LENGTH]) {
  stringstream ss;
  for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
    ss << hex << setw(2) << setfill('0') << (int)hash[i];
  }
  return ss.str();
}

SSTableWriter::SSTableWriter(string_view dbname, WritAbleFile *file,
                             const DBOptions &options)
    : dbname_(dbname),
      file_(file),
      offset_(0),
      filter_block_(make_unique<BloomFilter>(options.bits_per_key)) {
  SHA256_Init(&sha256_);
}

RC SSTableWriter::Add(string_view key, string_view value) {
  /* add K to filter block */
  filter_block_.Update(key);
  /* add <K,V> to data block */
  auto rc = data_block_.Add(key, value);
  if (rc) return rc;
  last_key_ = key;
  if (data_block_.EstimatedSize() > need_flush_size_) FlushDataBlock();
  return OK;
}

RC SSTableWriter::FlushDataBlock() {
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

RC SSTableWriter::Final(unsigned char sha256_digit[]) {
  /* 将剩余的数据块刷盘 */
  if (!data_block_.Empty()) FlushDataBlock();
  /* Filter 块 */
  filter_block_.Final(buffer_);
  SHA256_Update(&sha256_, buffer_.c_str(), buffer_.size());
  file_->Append(buffer_);
  filter_block_handle_.SetMeta(offset_, (int)buffer_.size());
  offset_ += (int)buffer_.size();
  string encoded_filter_block_handle;
  filter_block_handle_.EncodeMeta(encoded_filter_block_handle);
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
  RC rc = foot_block_.Final(buffer_);
  if (rc != OK) MLog->critical("foot_block Final failed: {}", strrc(rc));

  SHA256_Update(&sha256_, buffer_.c_str(), buffer_.size());
  file_->Append(buffer_);
  offset_ += (int)buffer_.size();

  SHA256_Final(sha256_digit, &sha256_);
  return OK;
}

RC SSTableReader::ReadFooterBlock() {
  RC rc;
  string_view footer_block_buffer;

  if (rc = file_->Read(file_size_ - FooterBlockWriter::footer_size,
                       FooterBlockWriter::footer_size, footer_block_buffer);
      rc) {
    MLog->error("Read file error: off:{} size:{}",
                   file_size_ - FooterBlockWriter::footer_size,
                   FooterBlockWriter::footer_size);
    return rc;
  }
  if (rc = foot_block_reader_.Init(footer_block_buffer); rc) {
    MLog->error("Init foot block error: {}", strrc(rc));
    return rc;
  }
  return OK;
}

RC SSTableReader::ReadIndexBlock() {
  RC rc;
  string_view index_block_buffer;
  auto index_block_handle = foot_block_reader_.index_block_handle();
  MLog->info("Index Block Handle: off:{} size:{}",
                index_block_handle.block_offset_,
                index_block_handle.block_size_);

  if (rc = file_->Read(index_block_handle.block_offset_,
                       index_block_handle.block_size_, index_block_buffer);
      rc) {
    MLog->error("Read index block buffer error: off:{} size:{}",
                   index_block_handle.block_offset_,
                   index_block_handle.block_size_);
    return rc;
  }
  if (rc = index_block_reader_.Init(index_block_buffer, CmpKeyAndUserKey); rc) {
    MLog->error("Init index block error: {}", strrc(rc));
    return rc;
  }
  return OK;
}

RC SSTableReader::ReadMetaBlock() {
  RC rc;
  string_view meta_block_buffer;
  auto meta_block_handle = foot_block_reader_.meta_block_handle();
  MLog->info("Meta Block Handle: off:{} size:{}",
                meta_block_handle.block_offset_, meta_block_handle.block_size_);

  if (rc = file_->Read(meta_block_handle.block_offset_,
                       meta_block_handle.block_size_, meta_block_buffer);
      rc) {
    MLog->error("Read meta block buffer error: off:{} size:{}",
                   meta_block_handle.block_offset_,
                   meta_block_handle.block_size_);
    return rc;
  }
  if (rc = meta_block_reader_.Init(meta_block_buffer, CmpKeyAndUserKey); rc) {
    MLog->error("Init meta block error: {}", strrc(rc));
    return rc;
  }
  return OK;
}

RC SSTableReader::ReadFilterBlock() {
  RC rc;
  BlockHandle filter_block_handle;
  string filter_block_handle_buffer;
  string_view filter_block_buffer;

  if (rc = meta_block_reader_.Get("filter", filter_block_handle_buffer); rc) {
    MLog->error("Get filter block handle error: {}", strrc(rc));
    return rc;
  }

  filter_block_handle.DecodeFrom(filter_block_handle_buffer);

  if (rc = file_->Read(filter_block_handle.block_offset_,
                       filter_block_handle.block_size_, filter_block_buffer);
      rc) {
    MLog->error("Read filter block buffer error: off:{} size:{}",
                   filter_block_handle.block_offset_,
                   filter_block_handle.block_size_);
    return rc;
  }

  if (rc = filter_block_reader_.Init(filter_block_buffer); rc) {
    MLog->error("Init filter block error: {}", strrc(rc));
    return rc;
  }
  return OK;
}

RC SSTableReader::Open(MmapReadAbleFile *file, SSTableReader **table) {
  SSTableReader *t = new SSTableReader(file);
  RC rc;
  /* read footer */
  if (rc = t->ReadFooterBlock(); rc) return rc;
  /* read index */
  if (rc = t->ReadIndexBlock(); rc) return rc;
  /* read meta */
  if (rc = t->ReadMetaBlock(); rc) return rc;
  /* read filter */
  if (rc = t->ReadFilterBlock(); rc) return rc;
  /* no read data! */
  *table = t;
  return OK;
}

SSTableReader::SSTableReader(MmapReadAbleFile *file)
    : file_(file), file_size_(file_->Size()) {}
}  // namespace adl