#ifndef ADL_LSM_TREE_DB_H__
#define ADL_LSM_TREE_DB_H__

#include <atomic>
#include <condition_variable>
#include <string>
#include "back_ground_worker.hpp"
#include "mem_table.hpp"
#include "options.hpp"
#include "rc.hpp"

namespace adl {

class DB {
 public:
  DB(const string &dp_path, DBOptions &options);
  ~DB();
  DB(const DB &) = delete;
  DB &operator=(const DB &) = delete;

  static RC Open(const std::string &dbname, DBOptions &options, DB **dbptr);
  static RC Create(const std::string &dbname);
  static RC Destroy(const std::string &dbname);
  RC Close();

  RC Put(const std::string &key, const std::string &value);
  RC Delete(const std::string &key);

  RC Get(const std::string &key, std::string &value);

 private:
  RC Write(const std::string &key, const std::string &value, OpType op);
  RC CheckMemAndCompaction();
  RC DoCompaction();
  RC DoMinorCompaction();

  RC BuildSSTable();
  RC FreezeMemTable();
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

  bool is_compacting_;
};

}  // namespace adl
#endif  // ADL_LSM_TREE_DB_H__