#include "db.hpp"
#include "file_util.hpp"

namespace adl {

DB::DB(const string &dbname)
    : dbname_(dbname),
      sequence_id_(0),
      mem_(new MemTable),
      imem_(new MemTable),
      closed_(false) {}

DB::~DB() {
  if (!closed_) Close();
  delete mem_;
  delete imem_;
}

RC DB::Open(std::string dbname, const DBOptions &options, DB **dbptr) {
  if (!FileManager::Exists(dbname)) {
    if (options.create_if_not_exists) {
      auto rc = Create(dbname);
      if (rc) return rc;
    } else {
      return RC::NOT_FOUND;
    }
  }

  if (!FileManager::IsDirectory(dbname)) {
    return IS_NOT_DIRECTORY;
  }

  DB *db = new DB(dbname);

  /* load metadata... */

  /* load WAL... */

  *dbptr = db;
  return OK;
}

RC DB::Create(std::string dbname) { return FileManager::Create(dbname, DIR_); }

RC DB::Destroy(std::string dbname) {
  // stop all services
  return FileManager::Destroy(dbname);
}

RC DB::Close() {
  // stop all services
  closed_ = true;
  return OK;
}

RC DB::Put(const std::string &key, const std::string &value) {
  return Write(key, value, OP_PUT);
}

RC DB::Delete(const std::string &key) { return Write(key, "", OP_DELETE); }

RC DB::Get(const std::string &key, std::string &value) {
  return mem_->Get(key, value);
}

RC DB::Write(const std::string &key, const std::string &value, OpType op) {
  MemKey mem_key(key, sequence_id_.fetch_add(1), op);
  return mem_->Put(mem_key, value);
}

}  // namespace adl