#include "sstable.hpp"
#include <openssl/sha.h>
#include <iomanip>
#include <sstream>
#include "block.hpp"
#include "db.hpp"
#include "file_util.hpp"
#include "hash_util.hpp"
#include "keys.hpp"
#include "monitor_logger.hpp"
#include "options.hpp"
#include "rc.hpp"

namespace adl {

SSTableWriter::SSTableWriter(string_view dbname, WritAbleFile *file,
                             const DBOptions &options)
    : dbname_(dbname),
      file_(file),
      offset_(0),
      filter_block_(make_unique<BloomFilter>(options.bits_per_key)) {
  SHA256_Init(&sha256_);
}

/* inner_key -> "[user_key, seq, op]" */
RC SSTableWriter::Add(string_view inner_key, string_view value) {
  /* add K.user_key to filter block */
  filter_block_.Update(InnerKeyToUserKey(inner_key));
  /* add <K,V> to data block */
  auto rc = data_block_.Add(inner_key, value);
  if (rc) return rc;
  last_key_ = inner_key;
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

  string sstable_path =
      SstFile(SstDir(dbname_), sha256_digit_to_hex(sha256_digit));

  if (rc = file_->ReName(sstable_path); rc) return rc;
  if (rc = file_->Close(); rc) return rc;

  return OK;
}

optional<unique_ptr<SSTableWriter>> NewSSTableWriter(string_view dbname,
                                                     const DBOptions *options) {
  TempFile *temp_file = nullptr;
  auto rc = FileManager::OpenTempFile(SstDir(dbname), "tmp_sst_", &temp_file);
  if (rc) {
    MLog->error("open temp file in {} with {}", dbname, strrc(rc));
    return nullopt;
  }

  /* temp_file belong to sstable writer now */
  return make_unique<SSTableWriter>(dbname, temp_file, *options);
}

RC SSTableReader::ReadFooterBlock() {
  RC rc = OK;
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
  RC rc = OK;
  string_view index_block_buffer;
  auto index_block_handle = foot_block_reader_.index_block_handle();
  MLog->info("Index Block Handle: off:{} size:{}",
             index_block_handle.block_offset_, index_block_handle.block_size_);

  if (rc = file_->Read(index_block_handle.block_offset_,
                       index_block_handle.block_size_, index_block_buffer);
      rc) {
    MLog->error("Read index block buffer error: off:{} size:{}",
                index_block_handle.block_offset_,
                index_block_handle.block_size_);
    return rc;
  }
  if (rc = index_block_reader_->Init(index_block_buffer, CmpInnerKey, EasySave);
      rc) {
    MLog->error("Init index block error: {}", strrc(rc));
    return rc;
  }
  return OK;
}

RC SSTableReader::ReadMetaBlock() {
  RC rc = OK;
  string_view meta_block_buffer;
  auto meta_block_handle = foot_block_reader_.meta_block_handle();
  MLog->info("Meta Block Handle: off:{} size:{}",
             meta_block_handle.block_offset_, meta_block_handle.block_size_);

  if (rc = file_->Read(meta_block_handle.block_offset_,
                       meta_block_handle.block_size_, meta_block_buffer);
      rc) {
    MLog->error("Read meta block buffer error: off:{} size:{}",
                meta_block_handle.block_offset_, meta_block_handle.block_size_);
    return rc;
  }
  if (rc = meta_block_reader_->Init(meta_block_buffer, EasyCmp, EasySave); rc) {
    MLog->error("Init meta block error: {}", strrc(rc));
    return rc;
  }
  return OK;
}

RC SSTableReader::ReadFilterBlock() {
  RC rc = OK;
  BlockHandle filter_block_handle;
  string filter_block_handle_buffer;
  string_view filter_block_buffer;

  if (rc = meta_block_reader_->Get("filter", filter_block_handle_buffer); rc) {
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

RC SSTableReader::Open(MmapReadAbleFile *file, SSTableReader **table,
                       const string &oid, DB *db) {
  SSTableReader *t = new SSTableReader(file, oid, db);
  RC rc = OK;
  /* read footer */
  if (rc = t->ReadFooterBlock(); rc) return rc;
  MLog->info("Read footer block ok");
  /* read index */
  if (rc = t->ReadIndexBlock(); rc) return rc;
  MLog->info("Read index block ok");
  /* read meta */
  if (rc = t->ReadMetaBlock(); rc) return rc;
  MLog->info("Read meta block ok");
  /* read filter */
  if (rc = t->ReadFilterBlock(); rc) return rc;
  MLog->info("Read filter block ok");
  /* no read data! */
  *table = t;
  return OK;
}

/* 在 sstable 内点查 */
RC SSTableReader::Get(string_view inner_key, string &value) {
  RC rc = OK;
  // MLog->trace("SSTableReader want Get key {}", key);

  /* 布隆过滤器过滤 */
  if (!filter_block_reader_.IsKeyExists(0, InnerKeyToUserKey(inner_key))) {
    // MLog->error("filter_block_reader_ think key {} is not exists!", key);
    return NOT_FOUND;
  }

  /* index 搜索 高键 */
  string data_block_handle_data;

  if (auto rc = index_block_reader_->Get(inner_key, data_block_handle_data);
      rc) {
    MLog->info("the key {} out range of this table",
               InnerKeyToUserKey(inner_key));
    return rc;
  }

  BlockHandle data_block_handle;

  data_block_handle.DecodeFrom(data_block_handle_data);
  MLog->info("Data Block Handle: off:{} size:{}",
             data_block_handle.block_offset_, data_block_handle.block_size_);
  shared_ptr<BlockReader> data_block_reader;

  rc = GetBlockReader(data_block_handle, data_block_reader);
  if (rc) return rc;

  /* read from data block */
  return data_block_reader->Get(inner_key, value);
}

RC SSTableReader::GetBlockReader(BlockHandle &data_block_handle,
                                 shared_ptr<BlockReader> &data_block_reader) {
  RC rc = OK;
  string_view data_block_buffer;
  BlockCacheHandle block_cache_handle(oid_, data_block_handle.block_offset_);

  /* read from block cache first; */
  if (!db_ || !db_->block_cache_->Get(block_cache_handle, data_block_reader)) {
    /* cache miss! read from file */
    if (rc = file_->Read(data_block_handle.block_offset_,
                         data_block_handle.block_size_, data_block_buffer);
        rc) {
      MLog->error("Read data block buffer error: off:{} size:{}",
                  data_block_handle.block_offset_,
                  data_block_handle.block_size_);
      return rc;
    }
    data_block_reader = make_shared<BlockReader>();
    if (rc = data_block_reader->Init(data_block_buffer, CmpInnerKey,
                                     SaveResultValueIfUserKeyMatch);
        rc) {
      MLog->error("data_block_reader Init error: {}", strrc(rc));
      return rc;
    }
    if (db_) db_->block_cache_->Put(block_cache_handle, data_block_reader);
  }
  return OK;
}

string SSTableReader::GetFileName() { return file_->GetFileName(); }

SSTableReader::SSTableReader(MmapReadAbleFile *file, const string &oid, DB *db)
    : file_(file),
      file_size_(file_->Size()),
      db_(db),
      oid_(oid),
      index_block_reader_(make_shared<BlockReader>()),
      meta_block_reader_(make_shared<BlockReader>()) {}

SSTableReader::~SSTableReader() {
  if (file_) {
    delete file_;
  }
}

}  // namespace adl