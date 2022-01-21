#ifndef ADL_LSM_TREE_SSTABLE_H__
#define ADL_LSM_TREE_SSTABLE_H__

#include <openssl/sha.h>
#include <string>
#include "block.hpp"
#include "file_util.hpp"
#include "filter_block.hpp"
#include "footer_block.hpp"
#include "rc.hpp"

using namespace std;

namespace adl {

class WritAbleFile;
class MmapReadAbleFile;
struct DBOptions;

/* Sorted Strings Table */
class SSTableWriter {
 public:
  SSTableWriter(string_view dbname, WritAbleFile *file,
                const DBOptions &options);
  /* Build */
  RC Add(string_view key, string_view value);
  RC Final(unsigned char sha256_digit[]);

  size_t GetFileSize() {
    size_t size = 0;
    FileManager::GetFileSize(file_->GetPath(), &size);
    return size;
  }

  string GetPath() { return file_->GetPath(); }

 private:
  RC FlushDataBlock();

  static constexpr unsigned int need_flush_size_ = (1UL << 12); /* 4KB */
  string dbname_;
  unique_ptr<WritAbleFile> file_;

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
  FooterBlockWriter foot_block_;
  // BlockHandle foot_block_handle_;

  SHA256_CTX sha256_;
  string buffer_;   /* 数据 */
  int offset_;      /* 数据的偏移量 */
  string last_key_; /* 最后一次 add 的 key */
};

optional<unique_ptr<SSTableWriter>> NewSSTableWriter(string_view dbname,
                                                     const DBOptions *options);

class DB;

class SSTableReader : public enable_shared_from_this<SSTableReader> {
 public:
  class Iterator {
   public:
    Iterator(shared_ptr<SSTableReader> &&container,
             BlockReader::Iterator &&idx_iter)
        : container_(std::move(container)),
          idx_iter_(std::move(idx_iter)),
          data_iter_(nullopt) {
      if (*idx_iter_ != container_->index_block_reader_->end()) ResetDataIter();
    }

    Iterator &operator++() {
      if (data_iter_) {
        ++(*data_iter_);
        if ((*data_iter_) >= data_iter_->GetContainer()->end()) {
          ++(*idx_iter_);
          if (idx_iter_.value() != container_->index_block_reader_->end())
            ResetDataIter();
          else
            data_iter_ = nullopt;
        }
      }
      return *this;
    }

    Iterator operator++(int) {
      Iterator tmp = *this;
      this->operator++();
      return tmp;
    }

    int operator<=>(Iterator &rhs) {
      assert(container_ == rhs.container_);
      assert(idx_iter_);
      assert(rhs.idx_iter_);

      if (*idx_iter_ > *rhs.idx_iter_)
        return 1;
      else if (*idx_iter_ < *rhs.idx_iter_)
        return -1;
      if (!data_iter_ && !rhs.data_iter_)
        return 0;
      else if (!data_iter_ && rhs.data_iter_)
        return 1;
      else if (data_iter_ && !rhs.data_iter_)
        return -1;
      else
        return *data_iter_ <=> *rhs.data_iter_;
    }

    Iterator &operator=(const Iterator &rhs) {
      if (&rhs != this) {
        container_ = rhs.container_;
        if (rhs.idx_iter_) idx_iter_ = rhs.idx_iter_.value();
        if (rhs.data_iter_) data_iter_ = rhs.data_iter_.value();
      }
      return *this;
    }

    Iterator(const Iterator &rhs) : container_(rhs.container_) {
      if (rhs.idx_iter_) idx_iter_ = *rhs.idx_iter_;
      if (rhs.data_iter_) data_iter_ = *rhs.data_iter_;
    }

    Iterator &operator=(Iterator &&rhs) noexcept {
      if (this != &rhs) {
        container_ = std::move(rhs.container_);
        idx_iter_ = std::move(rhs.idx_iter_);
        data_iter_ = std::move(rhs.data_iter_);
      }
      return *this;
    }
    Iterator(Iterator &&rhs) noexcept
        : container_(std::move(rhs.container_)),
          idx_iter_(std::move(rhs.idx_iter_)),
          data_iter_(std::move(rhs.data_iter_)) {}

    bool operator==(Iterator &rhs) {
      return container_ == rhs.container_ && idx_iter_ && rhs.idx_iter_ &&
             *idx_iter_ == *rhs.idx_iter_ &&
             ((!data_iter_ && !rhs.data_iter_) ||
              (data_iter_ && rhs.data_iter_ && *data_iter_ == *rhs.data_iter_));
    }

    bool operator!=(Iterator &rhs) { return !(*this == rhs); }

    const string_view Key() const { return data_iter_->Key(); }
    const string_view Value() const { return data_iter_->Value(); }

    void Fetch() {
      // assert(data_iter_);
      data_iter_->Fetch();
    }

    bool Valid() {
      // assert(data_iter_);
      return data_iter_->Valid();
    }

    Iterator GetContainerEnd() { return container_->end(); }

   private:
    void ResetDataIter() {
      if (!idx_iter_) return;
      if (!idx_iter_->Valid()) idx_iter_->Fetch();

      BlockHandle data_block_handle;
      data_block_handle.DecodeFrom(idx_iter_->Value());

      shared_ptr<BlockReader> data_block_reader;

      container_->GetBlockReader(data_block_handle, data_block_reader);
      data_iter_ = data_block_reader->begin();
    }
    optional<BlockReader::Iterator> idx_iter_;
    optional<BlockReader::Iterator> data_iter_;
    shared_ptr<SSTableReader> container_;
  };

  Iterator begin() {
    return Iterator(shared_from_this(), index_block_reader_->begin());
  }
  Iterator end() {
    return Iterator(shared_from_this(), index_block_reader_->end());
  }

  static RC Open(MmapReadAbleFile *file, SSTableReader **table,
                 const string &oid, DB *db = nullptr);
  RC Get(string_view key, string &value);
  string GetFileName();

  SSTableReader(MmapReadAbleFile *file, const string &oid, DB *db = nullptr);
  ~SSTableReader();
  SSTableReader &operator=(const SSTableReader &) = delete;
  SSTableReader(const SSTableReader &) = delete;

 private:
  RC GetBlockReader(BlockHandle &data_block_handle,
                    shared_ptr<BlockReader> &data_block_reader);

  RC ReadFooterBlock();
  RC ReadMetaBlock();
  RC ReadIndexBlock();
  RC ReadFilterBlock();
  // RC ReadDataBlock();

  MmapReadAbleFile *file_;
  size_t file_size_;
  string oid_;

  FilterBlockReader filter_block_reader_;
  shared_ptr<BlockReader> index_block_reader_;
  shared_ptr<BlockReader> meta_block_reader_;
  FooterBlockReader foot_block_reader_;

  DB *db_;
};

}  // namespace adl

#endif  // ADL_LSM_TREE_SSTABLE_H__