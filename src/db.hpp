#ifndef ADL_LSM_TREE_DB_H__
#define ADL_LSM_TREE_DB_H__

#include <atomic>
#include <string>
#include "mem_table.hpp"
#include "options.hpp"
#include "rc.hpp"

namespace adl {

class DB {
 public:
  DB(const string &dp_path, const DBOptions &options);
  ~DB();
  DB(const DB &) = delete;
  DB &operator=(const DB &) = delete;

  static RC Open(const std::string &dbname, const DBOptions &options,
                 DB **dbptr);
  static RC Create(const std::string &dbname);
  static RC Destroy(const std::string &dbname);
  RC Close();

  RC Put(const std::string &key, const std::string &value);
  RC Delete(const std::string &key);

  RC Get(const std::string &key, std::string &value);

 private:
  RC Write(const std::string &key, const std::string &value, OpType op);

  RC BuildSSTable();

  /* memory */
  MemTable *mem_;
  MemTable *imem_;

  /* state */
  std::atomic<bool> closed_;
  std::atomic<int64_t> sequence_id_;

  /* disk */
  string dbname_;
  DBOptions options;
  /* metadata */
};

}  // namespace adl
#endif  // ADL_LSM_TREE_DB_H__