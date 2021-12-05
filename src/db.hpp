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

class DB {
 public:
  DB(string_view db_path, DBOptions &options);
  ~DB();
  DB(const DB &) = delete;
  DB &operator=(const DB &) = delete;

  static RC Open(string_view dbname, DBOptions &options, DB **dbptr);
  static RC Create(string_view dbname);
  static RC Destroy(string_view dbname);
  RC Close();

  RC Put(string_view key, string_view value);
  RC Delete(string_view key);

  RC Get(string_view key, std::string &value);

 private:
  RC Write(string_view key, string_view value, OpType op);
  RC CheckMemAndCompaction();
  RC DoCompaction();
  RC DoMinorCompaction();

  RC BuildSSTable();
  void FreezeMemTable();
  bool NeedCompactions();
  bool NeedFreezeMemTable();

  /* memory */
  MemTable *mem_;
  MemTable *imem_;

  /* state */
  std::atomic<bool> closed_;
  std::atomic<int64_t> sequence_id_;

  /* disk */
  string dbname_;
  DBOptions *options_;
  /* metadata */

  /* thread sync */
  mutex mutex_;
  std::condition_variable background_work_done_cond_;

  /* back ground */
  vector<Worker *> workers_;

  /* monitor_logger.hpp */
  // MonitorLogger monitor_logger_;

  bool is_compacting_;
};

}  // namespace adl
#endif  // ADL_LSM_TREE_DB_H__