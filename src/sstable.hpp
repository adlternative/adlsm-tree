#ifndef ADL_LSM_TREE_SSTABLE_H__
#define ADL_LSM_TREE_SSTABLE_H__

#include <openssl/sha.h>
#include <string>
#include "block.hpp"
#include "filter_block.hpp"
#include "footer_block.hpp"
#include "rc.hpp"

using namespace std;

namespace adl {

class WritAbleFile;
struct DBOptions;

/* Sorted Strings Table */
class SSTableWriter {
 public:
  SSTableWriter(string_view dbname, WritAbleFile *file,
                const DBOptions &options);
  /* Build */
  RC Add(string_view key, string_view value);
  RC Final(unsigned char sha256_digit[]);

 private:
  RC FlushDataBlock();

  static constexpr unsigned int need_flush_size_ = 4 * (1UL << 12);
  string dbname_;
  WritAbleFile *file_;

  /* 数据块 */
  BlockWriter data_block_;
  BlockHandle data_block_handle_;

  /* 索引块 */
  BlockWriter index_block_;
  BlockHandle index_block_handle_;

  /* 过滤器块 */
  FilterBlockWriter filter_block_;
  BlockHandle filter_block_handle_;

  /* 元数据块 */
  BlockWriter meta_data_block_;
  BlockHandle meta_data_block_handle_;

  /* 尾信息块 */
  FooterBlock foot_block_;
  // BlockHandle foot_block_handle_;

  SHA256_CTX sha256_;
  string buffer_;   /* 数据 */
  int offset_;      /* 数据的偏移量 */
  string last_key_; /* 最后一次 add 的 key */
};

string sha256_digit_to_hex(unsigned char hash[SHA256_DIGEST_LENGTH]);

class SSTable {};

class SSTableReader {
 public:
  static RC Open(WritAbleFile *file, const DBOptions &options, SSTable **table);
 private:
  RC ReadMetaDataBlock();
  RC ReadFooterBlock();
  RC ReadIndexBlock();
  RC ReadDataBlock();
  RC ReadFilterBlock();
};

}  // namespace adl

#endif  // ADL_LSM_TREE_SSTABLE_H__