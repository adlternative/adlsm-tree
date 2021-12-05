#include "filter_block.hpp"
#include <string.h>
#include "encode.hpp"
#include "murmur3_hash.hpp"

namespace adl {

RC BloomFilter::Keys2Block(const vector<string> &keys, string &result) {
  int keys_len = (int)keys.size();
  int bitmap_bits_len =
      (keys_len * bits_per_key_ + 7) * 8; /* bitmap 长度（bits） */
  int init_len = (int)result.size();
  int bitmap_len = bitmap_bits_len / 8; /* bitmap 长度（bytes） */

  /* 开辟 bitmap 空间 */
  result.resize(init_len + bitmap_len);
  auto bitmap = &result[init_len];

  for (const auto &key : keys) {
    /* 双哈希模拟多哈希 （ leveldb 单哈希模拟多哈希）*/
    auto h1 = murmur3_hash(0xe2c6928a, key.data(), key.size());
    auto h2 = murmur3_hash(0xbaea8a8f, key.data(), key.size());

    for (int j = 0; j < k_; j++) {
      auto h = h1 + j * h2;
      int bit_pos = (int)(h % bitmap_bits_len);
      bitmap[bit_pos / 8] |= (1 << (bit_pos % 8));
    }
  }

  return RC::OK;
}

BloomFilter::BloomFilter(int bits_per_key) : bits_per_key_(bits_per_key) {
  /* k 个 哈希函数
   * m [m bit] 过滤器数组的 bit 数
   * n 个 key
   *
   * 当 k = ln2 * (m/n) 时，Bloom Filter 获取最优的准确率。m/n 即 bits per
   * key（集合中每个 key 平均分到的 bit 数）。
   */

  k_ = static_cast<int>(bits_per_key * 0.69);  // 0.69 =~ ln(2)
  if (k_ < 1) k_ = 1;
  if (k_ > 30) k_ = 30;
}

bool BloomFilter::IsKeyExists(string_view key, string_view bitmap) {
  int bitmap_bits_len = (int)bitmap.size() * 8;

  auto h1 = murmur3_hash(0xe2c6928a, key.data(), key.size());
  auto h2 = murmur3_hash(0xbaea8a8f, key.data(), key.size());
  for (int j = 0; j < k_; j++) {
    auto h = h1 + j * h2;
    int bit_pos = (int)(h % bitmap_bits_len);
    /* 一定不在 */
    if (!(bitmap[bit_pos / 8] & (1 << (bit_pos % 8)))) return false;
  }
  /* 可能存在 */
  return true;
}

void BloomFilter::FilterInfo(string &info) {
  info.append("bf:");
  info.append((char *)&bits_per_key_, sizeof(int));
}

FilterBlock::FilterBlock(unique_ptr<FilterAlgorithm> &&method)
    : method_(std::move(method)) {}

RC FilterBlock::Update(string_view key) {
  keys_.emplace_back(key);
  return OK;
}

RC FilterBlock::Final(string &result) {
  if (!keys_.empty()) Keys2Block();

  int offset_begin_offset = (int)buffer_.size();
  int offset_len = (int)offsets_.size();
  /* 追加 offsets */
  for (int i = 0; i < offset_len; i++) {
    buffer_.append(reinterpret_cast<char *>(&offsets_[i]), sizeof(int));
  }
  /* 追加 offsets[0].offset */
  buffer_.append(reinterpret_cast<char *>(&(offset_begin_offset)), sizeof(int));
  /* 追加  offsets_.size() */
  buffer_.append(reinterpret_cast<char *>(&(offset_len)), sizeof(int));

  /* 追加 filter 算法相关信息及其长度, 比如我们在 bloom filter 选择的
   * bits_per_key  */
  string filter_info;
  method_->FilterInfo(filter_info);
  int filter_info_len = (int)filter_info.length();
  if (filter_info_len) {
    buffer_.append(filter_info);
    buffer_.append(reinterpret_cast<char *>(&filter_info_len), sizeof(int));
  }
  result = std::move(buffer_);
  return OK;
}

RC FilterBlock::Keys2Block() {
  offsets_.push_back((int)buffer_.size());
  method_->Keys2Block(keys_, buffer_);
  keys_.clear();
  return OK;
}

FilterBlockReader::FilterBlockReader() : filters_nums_(0) {}

RC FilterBlockReader::Init(const string /* string_view */ &filter_blocks) {
  filter_blocks_ = filter_blocks;

  auto filter_block_len = filter_blocks_.length();
  /* 1. GET INFO_LEN */
  if (filter_block_len < (int)sizeof(int)) return FILTER_BLOCK_ERROR;
  int info_len = 0;
  int info_len_offset = (int)(filter_block_len - sizeof(int));
  Decode32(&filter_blocks_[info_len_offset], &info_len);
  if (info_len > info_len_offset || info_len <= 0) return FILTER_BLOCK_ERROR;
  /* 2. 根据 info 生成相应的过滤器算法。 */
  int info_offset = (int)(info_len_offset - info_len);
  filter_info_ = {&filter_blocks_[info_offset], (size_t)info_len};
  auto rc = CreateFilterAlgorithm();
  if (rc != OK) return rc;
  /* 3. GET filter num */
  if (info_offset < sizeof(int)) return FILTER_BLOCK_ERROR;
  int filters_num_offset = (int)(info_offset - sizeof(int));
  Decode32(&filter_blocks_[filters_num_offset], &filters_nums_);
  /* 4. GET filter 0 offset offset*/
  filters_offsets_offset_ = 0;
  if (filters_num_offset < sizeof(int)) return FILTER_BLOCK_ERROR;
  int filters_offsets_offset_offset = (int)(filters_num_offset - sizeof(int));
  Decode32(&filter_blocks_[filters_offsets_offset_offset],
           &filters_offsets_offset_);
  /* 5. GET filter 0 offset*/
  if (filters_offsets_offset_ < 0) return FILTER_BLOCK_ERROR;

  /* 6. GET filter 0 block addr */
  int filters_zero_offset = 0;
  Decode32(&filter_blocks_[filters_offsets_offset_], &filters_zero_offset);
  if (filters_zero_offset != 0) return FILTER_BLOCK_ERROR;

  filters_offsets_ = {&filter_blocks_[filters_offsets_offset_],
                      sizeof(int) * filters_nums_};

  return OK;
}

/* 目前只有 bf: */
RC FilterBlockReader::CreateFilterAlgorithm() {
  static const char *kBloomFilter = "bf";

  string_view type = filter_info_.substr(0, 2);
  if (type != kBloomFilter) return FILTER_BLOCK_ERROR;
  int bits_per_key = 0;
  Decode32(&filter_info_[3], &bits_per_key);
  method_ = std::make_unique<BloomFilter>(bits_per_key);
  return OK;
}

bool FilterBlockReader::IsKeyExists(int filter_block_num, string_view key) {
  int filter_offset1, filter_offset2;
  if (filter_block_num >= filters_nums_) return false;
  Decode32(&filters_offsets_[filter_block_num * sizeof(int)], &filter_offset1);
  if (filter_block_num + 1 == filters_nums_)
    filter_offset2 = filters_offsets_offset_;
  else
    Decode32(&filters_offsets_[(filter_block_num + 1) * sizeof(int)],
             &filter_offset2);

  return method_->IsKeyExists(
      key, {&filter_blocks_[filter_offset1], &filter_blocks_[filter_offset2]});

  return true;
}

}  // namespace adl