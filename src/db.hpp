#ifndef ADL_LSM_TREE_DB_H__
#define ADL_LSM_TREE_DB_H__

#include <atomic>
#include <condition_variable>
#include <string>
#include "back_ground_worker.hpp"
#include "mem_table.hpp"
#include "monitor_logger.hpp"
#include "options.hpp"
#include "rc.hpp"

namespace adl {

class Revision;

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
  RC Sync();
  RC Put(string_view key, string_view value);
  RC Delete(string_view key);

  RC Get(string_view key, std::string &value);

 private:
  RC Write(string_view key, string_view value, OpType op);
  RC CheckMemAndCompaction();
  RC DoCompaction();
  RC DoMinorCompaction();

  RC BuildSSTable(const shared_ptr<adl::MemTable> &mem);
  RC FreezeMemTable();
  bool NeedCompactions();
  bool NeedFreezeMemTable();

  RC UpdateCurrentRev(Revision *rev);
  RC WriteCurrentRev(string_view write_buffer);

  RC LoadMetaData();
  RC LoadWALs();
  RC LoadWAL(string_view wal_file_path);

  /* memtables */
  shared_ptr<MemTable> mem_;
  shared_ptr<MemTable> imem_;

  /* state */
  std::atomic<bool> closed_;
  std::atomic<int64_t> sequence_id_;
  bool is_compacting_;

  /* disk */
  string dbname_;
  DBOptions *options_;
  /* metadata */

  /* thread sync */
  mutex mutex_;
  std::condition_variable background_work_done_cond_;

  /* back ground */
  vector<Worker *> workers_;
  RC save_backgound_rc_;

  // MonitorLogger monitor_logger_;
  shared_ptr<Revision> current_rev_;
};
}  // namespace adl
#endif  // ADL_LSM_TREE_DB_H__