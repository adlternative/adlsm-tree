#ifndef ADL_LSM_TREE_REVISION_H__
#define ADL_LSM_TREE_REVISION_H__

#include <cassert>
#include <cstring>
#include <deque>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>
#include "file_util.hpp"
namespace adl {

using namespace std;

struct FileMetaData;
class DB;

class Object {
 public:
  Object(DB *db) : db_(db) {}
  virtual ~Object() = default;
  virtual RC BuildFile(string_view dbname) = 0;
  virtual RC LoadFromFile(string_view dbname, string_view sha_hex) = 0;
  virtual RC Get(string_view key, string &value) = 0;

  string GetOid() const;
  void Clear() {
    memset(&sha256_, 0, sizeof(SHA256_CTX));
    memset(&sha256_digit_, 0, SHA256_DIGEST_LENGTH * sizeof(unsigned char));
  }

 protected:
  DB *db_;
  /* checksum */
  SHA256_CTX sha256_;
  unsigned char sha256_digit_[SHA256_DIGEST_LENGTH];
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
class Level : public Object {
 public:
  Level(DB *db, int level = -1);
  Level(DB *db, int level, const string_view &oid);
  Level(const Level &);
  Level &operator=(Level &);
  virtual ~Level() = default;
  virtual RC BuildFile(string_view dbname) override;
  virtual RC LoadFromFile(string_view dbname, string_view sha_hex) override;
  virtual RC Get(string_view key, std::string &value) override;

  void Insert(FileMetaData *file_meta);
  void Erase(FileMetaData *file_meta);
  bool Empty() const;
  int FilesCount() const;
  int GetLevel() const;
  void SetLevel(int level);
  bool HaveCheckSum() const;
  int TotalFileSize() const;
  int64_t GetMaxSeq();
  const set<shared_ptr<FileMetaData>, FileMetaDataCompare>
      &GetSSTableFilesMeta() const;
  void Clear() {
    Object::Clear();
    files_meta_.clear();
  }

 private:
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
class Revision : public Object {
 public:
  Revision(DB *db);
  Revision(DB *db, vector<Level> &&levels, deque<int64_t> &&log_nums_) noexcept;
  virtual ~Revision() = default;
  virtual RC BuildFile(string_view dbname) override;
  virtual RC LoadFromFile(string_view dbname, string_view sha_hex) override;
  virtual RC Get(string_view key, std::string &value) override;

  const vector<Level> &GetLevels() const;
  const Level &GetLevel(int i) const;

  void SetLevel(int level, Level *v);
  void PushLogNumber(int64_t num);
  void PopLogNumber();
  const deque<int64_t> &GetLogNumbers() const;
  int64_t GetMaxSeq();
  int PickBestCompactionLevel();

 private:
  /* 层级 */
  vector<Level> levels_;
  /* 不需要持久化到 revsion file;而是持久化到 current file */
  deque<int64_t> log_nums_;
};

}  // namespace adl

#endif  // ADL_LSM_TREE_REVISION_H__