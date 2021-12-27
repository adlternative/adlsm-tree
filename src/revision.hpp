#ifndef ADL_LSM_TREE_REVISION_H__
#define ADL_LSM_TREE_REVISION_H__

#include <cstring>
#include <deque>
#include <memory>
#include <set>
#include <string>
#include <vector>
#include "file_util.hpp"
namespace adl {

using namespace std;

struct FileMetaData;

class Object {
 public:
  Object() = default;
  virtual ~Object();
  virtual string GetOid() const = 0;
  virtual RC BuildFile(string_view dbname) = 0;

 private:
};

struct FileMetaDataCompare {
  using is_transparent = void;
  //    ~~~~~~~~~~~~~^

  bool operator()(const shared_ptr<FileMetaData> &a,
                  const shared_ptr<FileMetaData> &b) const {
    return *a < *b;
  }

  bool operator()(const shared_ptr<FileMetaData> &a, FileMetaData *b) const {
    return *a < *b;
  }

  bool operator()(FileMetaData *a, const shared_ptr<FileMetaData> &b) const {
    return *a < *b;
  }
};
/**
 * @brief Level 对象
  Level Format:

  | Level  4Bytes   |   FileNum   4Bytes|
  | min-key len | min-key | max-key len  | max-key | SHA             |
  |------------| ------- |-------------- | ------- | --------------- |
  |            | adl     |               | alskdj  | `<sstable-sha>` |
  |            | basld   |               | caslkdj | `<sstable-sha>` |
  |            | esakld  |               | faslkdj | `<sstable-sha>` |
  |            | fjaskl  |               | qeku    | `<sstable-sha>` |
  |            | ylyly   |               | zzzz    | `<sstable-sha>` |


  除了 L0 可能会出现 OverLap，其它层都是按照 min-key 排序的
  当 L0 出现了 overlap 的时候，我们应该考虑到如何保证读的正确性，
  所以我们应该让 L0 的 sstable 有一个序列号，用来保证获取最新的数据
  否则需要遍历所有的 L0 的 sstable。

  注意到 L0 的 min-key 和 max-key 中的 seq 应该是可以决定一个 sstable 的时序的。
  LN 和 LN+1 中 sstable 的时序也是确定的。所以复用 min-key.seq 应该是合理的。

  在 Major Compaction 发生时，L0.s1 L0.s2 L1.s3 L1.s4 通过检查 KEY 的 SEQ
 进行合并。
 */
class Level {
 public:
  Level();
  Level(const Level &);
  Level &operator=(Level &);
  Level(int level, const string_view &oid);

  void Insert(FileMetaData *file_meta);
  void Erase(FileMetaData *file_meta);
  bool Empty() const;
  string GetOid() const;
  int GetLevel() const;
  void SetLevel(int level) { level_ = level; }
  RC BuildFile(string_view dbname);
  RC LoadFromFile(string_view dbname, string_view lvl_sha_hex);
  bool HaveCheckSum() const;
  int64_t GetMaxSeq();

  RC Get(string_view key, string &value);

 private:
  /* checksum */
  SHA256_CTX sha256_;
  unsigned char sha256_digit_[SHA256_DIGEST_LENGTH];
  /* 文件元数据 */
  set<shared_ptr<FileMetaData>, FileMetaDataCompare> files_meta_;
  /* 第几层 */
  int level_;
};

/**
 * @brief Revision 版本对象
  Revisions Format:

  | LEVEL | SHA            |
  | ----- | ------------- |
  | 1     | `<level-sha>` |
  | 2     | `<level-sha>` |
  | 3     | `<level-sha>` |
  | 4     | `<level-sha>` |
  | 5     | `<level-sha>` |

 */
class Revision {
 public:
  Revision();
  Revision(vector<Level> &&levels, deque<int64_t> &&log_nums_) noexcept;
  string GetOid() const;
  RC BuildFile(string_view dbname);
  const vector<Level> &GetLevels();
  void SetLevel(int level, Level *v);
  // void SetSeq(int64_t seq);
  // int64_t GetSeq() { return seq_; }
  RC LoadFromFile(string_view dbname, string_view rev_sha_hex);

  void PushLogNumber(int64_t num) { log_nums_.push_back(num); }
  void PopLogNumber() { log_nums_.pop_front(); }
  const deque<int64_t> &GetLogNumbers() const { return log_nums_; }
  int64_t GetMaxSeq() {
    int64_t maxseq = 0;
    for (int i = 0; i < levels_.size(); i++)
      maxseq = max(maxseq, levels_[i].GetMaxSeq());
    return maxseq;
  }

  RC Get(string_view key, std::string &value);

 private:
  /* checksum */
  SHA256_CTX sha256_;
  unsigned char sha256_digit_[SHA256_DIGEST_LENGTH];
  /* 层级 */
  vector<Level> levels_;
  // int64_t seq_;

  /* 不需要持久化到 revsion file;而是持久化到 current file */
  deque<int64_t> log_nums_;
};

}  // namespace adl

#endif  // ADL_LSM_TREE_REVISION_H__