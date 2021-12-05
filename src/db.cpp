#include "db.hpp"
//#include <fmt/format.h>
#include <mutex>
#include "file_util.hpp"

namespace adl {

DB::DB(const string &dbname, DBOptions &options)
    : dbname_(dbname),
      sequence_id_(0),
      options_(&options),
      mem_(new MemTable(options)),
      imem_(nullptr),
      closed_(false),
      is_compacting_(false) {
  for (int i = 0; i < options_->background_workers_number; i++)
    workers_.push_back(Worker::NewBackgroundWorker());
}

DB::~DB() {
  if (!closed_) Close();
  for (auto worker : workers_) {
    worker->Stop();
    delete worker;
  }
  if (mem_) delete mem_;
  if (imem_) delete imem_;
}

RC DB::Open(const std::string &dbname, DBOptions &options, DB **dbptr) {
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

  DB *db = new DB(dbname, options);

  /* load metadata... */

  /* load WAL... */

  *dbptr = db;
  return OK;
}

RC DB::Create(const std::string &dbname) {
  return FileManager::Create(dbname, DIR_);
}

RC DB::Destroy(const std::string &dbname) {
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
  lock_guard<mutex> lock(mutex_);

  auto rc = CheckMemAndCompaction();
  if (rc) return rc;
  /* write WAL */
  /* write memtable */
  MemKey mem_key(key, sequence_id_.fetch_add(1), op);
  return mem_->Put(mem_key, value);
}

RC DB::CheckMemAndCompaction() {
  /* 表示已经锁 */
  unique_lock<mutex> lock(mutex_, adopt_lock);

  while (!closed_) {
    /* 如果 memtable 没满，我们就可以返回  */
    if (!NeedFreezeMemTable()) break;
    /* memtable 满了，如果 imem 非空 等待
    minor compaction 将 imem 置为空 */
    // fmt::print("DB wait until imem_ empty \n");
    while (imem_ != nullptr) background_work_done_cond_.wait(lock);
    // fmt::print("DB wait imem empty ok \n");
    /* mem -> imem */
    auto rc = FreezeMemTable();
    if (rc) return rc;
    /* 检查是否需要 Compaction */
    if (!NeedCompactions()) break;
    workers_[0]->Add([this]() { DoCompaction(); });
    /* wait for compaction done */
    background_work_done_cond_.wait(lock);
  }
  lock.release();
  return closed_ ? DB_CLOSED : OK;
}

/* background thread do compaction */
RC DB::DoCompaction() {
  RC rc;
  // fmt::print("DB Compaction want get lock\n");
  lock_guard<mutex> lock(mutex_);
  // fmt::print("DB Compaction get lock\n");
  is_compacting_ = true;

  if (imem_ != nullptr) {
    /* minor compaction */
    rc = DoMinorCompaction();
  } else {
    /* major compaction */
    rc = UN_IMPLEMENTED;
  }
  if (rc) return rc;

  is_compacting_ = false;
  /* TODO(adlternative) 可能产生 compaction 级联更新 但不一定是立刻
  （因为不是立即抢锁，或许可以修改它）*/
  // fmt::print("DB Compaction may need Compaction Again\n");
  if (NeedCompactions()) workers_[0]->Add([this]() { DoCompaction(); });
  // fmt::print("DB Compaction add Compaction Again\n");
  background_work_done_cond_.notify_all();
  return rc;
}

RC DB::DoMinorCompaction() {
  auto rc = BuildSSTable();
  if (rc) return rc;
  /* 等会改成智能指针 */
  delete imem_;
  imem_ = nullptr;
  return OK;
}

/* imm --> sstable */
RC DB::BuildSSTable() {
  auto rc = imem_->BuildSSTable(dbname_);
  if (rc) return rc;
  /* 更新元数据跟踪新 sstable */
  return OK;
}

/* mem -> imem */
RC DB::FreezeMemTable() {
  imem_ = mem_;
  mem_ = new MemTable(*options_);
  return OK;
}

bool DB::NeedCompactions() {
  if (closed_) return false;
  if (imem_) return true;
  // if (level0_files_ > options_->level0_files_limit) return true;
  // if (leveln_total_size_ > options_->leveln_total_size_limit) return true;
  return false;
}

bool DB::NeedFreezeMemTable() {
  return !closed_ && mem_->GetMemTableSize() > options_->mem_table_max_size;
}

}  // namespace adl