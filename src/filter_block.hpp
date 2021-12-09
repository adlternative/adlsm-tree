#ifndef ADL_LSM_TREE_FILTER_BLOCK_H__
#define ADL_LSM_TREE_FILTER_BLOCK_H__

#include <memory>
#include <string>
#include <vector>
#include "rc.hpp"

namespace adl {

using namespace std;

class FilterAlgorithm {
 public:
  virtual RC Keys2Block(const vector<string> &keys, string &result) = 0;
  virtual bool IsKeyExists(string_view key, string_view bitmap) = 0;
  virtual void FilterInfo(string &info) { /* NOTHING */
  }
  virtual ~FilterAlgorithm() = default;
};

class BloomFilter : public FilterAlgorithm {
 public:
  /* bits_per_key 将会决定 一个 bloom-filter-block n 个 key 需要存储的总大小 */
  explicit BloomFilter(int bits_per_key);
  RC Keys2Block(const vector<string> &keys, string &result) override;
  bool IsKeyExists(string_view key, string_view bitmap) override;
  void FilterInfo(string &info) override;
  ~BloomFilter() = default;

 private:
  int bits_per_key_; /* 每个 key 所占用 的 bit 数量 */
  int k_;            /* 哈希函数个数 */
};

class FilterBlockWriter {
 public:
  explicit FilterBlockWriter(unique_ptr<FilterAlgorithm> &&method);
  /* 将 key 添加到过滤器中等待计算 */
  RC Update(string_view key);
  /* 将布隆过滤器生成的结果加入从 ret 返回 */
  RC Final(string &result);
  /* 将积攒的 keys 生成 filter_block 追加到 buffer_ 中 */
  RC Keys2Block();

 private:
  vector<string> keys_; /* 用来保存目前填入的 key ，在 Keys2Block 被调用时生成
                           filter_block */
  vector<int> offsets_; /* 每个 filter 的偏移量  */
  string buffer_;       /* filter_block 缓冲区，之后会传出 */
  unique_ptr<FilterAlgorithm> method_; /* 过滤器算法，目前只有 bloom-filter */
};

class FilterBlockReader {
 public:
  FilterBlockReader();
  RC Init(string_view filter_block);
  /**
   * @brief 我们从外界传入 key 所在 FILTER BLOCK NUM，因为它和
   * DATA BLOCK NUM 是相同的。
   */
  bool IsKeyExists(int filter_block_num, string_view key);

 private:
  RC CreateFilterAlgorithm();

  int filters_nums_;           /* 过滤器块的个数 */
  int filters_offsets_offset_; /* 过滤器偏移量数组在块中的偏移量 */
  string_view filters_offsets_; /* 过滤器数组 */
  string_view filter_info_;     /* 过滤器信息 */
  string filter_blocks_;        /*  整个过滤器块 */
  unique_ptr<FilterAlgorithm> method_; /* 过滤器算法，目前只有 bloom-filter */
};

}  // namespace adl

#endif  // ADL_LSM_TREE_FILTER_BLOCK_H__