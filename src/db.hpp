#ifndef ADL_LSM_TREE_DB_H__
#define ADL_LSM_TREE_DB_H__

#include <atomic>
#include <condition_variable>
#include <string>
#include "back_ground_worker.hpp"
#include "cache.hpp"
#include "mem_table.hpp"
#include "monitor_logger.hpp"
#include "options.hpp"
#include "rc.hpp"
#include "sstable.hpp"

namespace adl {

class Revision;
class Level;

class DB {
 public:
  DB(string_view db_path, DBOptions &options);
  ~DB();
  DB(const DB &) = delete;
  DB &operator=(const DB &) = delete;

  static RC Create(string_view dbname, DBOptions &options, DB **dbptr);
  static RC Open(string_view dbname, DBOptions &options, DB **dbptr);
  static RC Destroy(string_view dbname);
  RC Close();
  RC Put(string_view key, string_view value);
  RC Delete(string_view key);

  RC Get(string_view key, std::string &value);

  void Debug();
  RC DebugSSTable(string_view oid);

 private:
  RC Write(string_view key, string_view value, OpType op);
  RC MaybeDoCompaction();
  void DoCompaction();
  RC DoMinorCompaction();
  RC DoMajorCompaction();

  RC GetSSTableReader(const string &oid, shared_ptr<SSTableReader> &sstable);
  /* 将一层中所有 runs 进行合并，
  创建一个新的 run 放到下一层 */
  RC MergeRuns(const Level &level, FileMetaData **meta_data_pointer);

  RC BuildSSTable(const shared_ptr<adl::MemTable> &mem);
  RC FreezeMemTable();
  bool NeedCompactions();
  bool NeedMajorCompactions();
  bool NeedMinorCompactions();

  // RC InitCurrentRev();
  RC UpdateCurrentRev(Revision *rev);
  RC WriteCurrentRev(string_view write_buffer);
  RC WriteCurrentRev(Revision *rev);
  RC ParseCurrentRev(string_view current_file_data, string &current_rev_sha_hex,
                     deque<int64_t> &log_nums);
  RC LoadMetaData();
  RC LoadWALs(const deque<int64_t> &log_nums);
  RC LoadWAL(string_view wal_file_path);

  /* memtables */
  shared_ptr<MemTable> mem_;
  shared_ptr<MemTable> imem_;

  std::atomic<int64_t> sequence_id_;

  /* disk */
  string dbname_;
  DBOptions *options_;

  /* thread sync */
  mutex mutex_;
  std::condition_variable background_work_done_cond_;

  /* back ground */
  vector<Worker *> workers_;
  RC save_backgound_rc_;

  /* state */
  std::atomic<bool> closed_;
  bool is_compacting_;

  /* current revision */
  shared_ptr<Revision> current_rev_;

  /* wal */
  std::atomic<int64_t> log_number_;

  /* cache */
  unique_ptr<LRUCache<string, shared_ptr<SSTableReader>, std::mutex>>
      table_cache_;

  unique_ptr<LRUCache<BlockCacheHandle, shared_ptr<BlockReader>, std::mutex>>
      block_cache_;

  friend class Level;
  friend class Revision;
  friend class SSTableReader;
};

}  // namespace adl
#endif  // ADL_LSM_TREE_DB_H__