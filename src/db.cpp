#include "db.hpp"
#include <cstdint>
#include <mutex>
#include "file_util.hpp"
#include "mem_table.hpp"
#include "monitor_logger.hpp"
#include "rc.hpp"

namespace adl {

DB::DB(string_view dbname, DBOptions &options)
    : dbname_(dbname),
      sequence_id_(0),
      options_(&options),
      mem_(make_shared<MemTable>(options)),
      imem_(nullptr),
      closed_(false),
      is_compacting_(false) {
  /* 后台工作者线程，目前只有一个线程 */
  MLogger.SetDbNameAndOptions(dbname_, &options);
  for (int i = 0; i < options_->background_workers_number; i++)
    workers_.push_back(Worker::NewBackgroundWorker());
  MLog->info("DB {} have created", dbname_);
}

DB::~DB() {
  MLog->info("DB is closing");
  if (!closed_) Close();
  for (auto worker : workers_) {
    worker->Stop();
    worker->Join();
    delete worker;
  }
  MLog->info("DB closed");
  spdlog::drop(options_->logger_name);
}

RC DB::Open(string_view dbname, DBOptions &options, DB **dbptr) {
  string true_dbname(FileManager::FixDirName(dbname));
  if (!FileManager::Exists(true_dbname)) {
    if (options.create_if_not_exists) {
      auto rc = Create(true_dbname);
    } else {
      return RC::NOT_FOUND;
    }
  }

  if (!FileManager::IsDirectory(true_dbname)) {
    return IS_NOT_DIRECTORY;
  }

  DB *db = new DB(true_dbname, options);

  /* load metadata... */

  /* load WAL... */

  *dbptr = db;
  return OK;
}

RC DB::Create(string_view dbname) { return FileManager::Create(dbname, DIR_); }

RC DB::Destroy(string_view dbname) {
  // stop all services
  return FileManager::Destroy(dbname);
}

RC DB::Close() {
  // stop all services
  closed_ = true;
  return OK;
}

RC DB::Put(string_view key, string_view value) {
  if (auto rc = Write(key, value, OP_PUT); rc) {
    MLog->error("Put key:{} value:{} failed", key, value);
    return rc;
  }
  return OK;
}

RC DB::Delete(string_view key) {
  if (auto rc = Write(key, "", OP_PUT); rc) {
    MLog->error("Delete key:{} failed", key);
    return rc;
  }
  return OK;
}

RC DB::Get(string_view key, std::string &value) {
  RC rc;
  unique_lock<mutex> lock(mutex_);
  auto mem = mem_;
  auto imem = imem_;
  // int64_t sequence_id_ = INT64_MAX;
  lock.unlock();
  /* 1. memtable */
  rc = mem->Get(key, value);
  if (!rc) return rc;
  /* 2. imemtable */
  if (imem) {
    rc = imem->GetNoLock(key, value);
    if (!rc) return rc;
  }
  /* 3 L0 sstable | L1-LN sstable 需要依赖于版本控制 元数据管理 */
  /* 4. 读是否需要更新元数据？ */
  return NOT_FOUND;
}

/* 目前写入的并发策略是整个 Write Db锁，因为想要保护 memtable
 * 否则一个写入线程修改 mem = new MemTable 这个线程不加锁读取 mem
 * 是有问题的。 */
RC DB::Write(string_view key, string_view value, OpType op) {
  unique_lock<mutex> lock(mutex_);
  /* check if need freeze mem or compaction 这可能需要好一会儿 */
  if (auto rc = CheckMemAndCompaction(); rc) {
    MLog->error("DB CheckMemAndCompaction failed: {}", strrc(rc));
    return rc;
  }
  /* write WAL */
  /* write memtable */
  MemKey mem_key(key, sequence_id_.fetch_add(1), op);
  return mem_->Put(mem_key, value);
}

/* 由于读 imem_ 需要锁定 */
RC DB::CheckMemAndCompaction() {
  /* 表示已经锁 */
  unique_lock<mutex> lock(mutex_, adopt_lock);

  while (!closed_) {
    /* 如果 memtable 没满，我们就可以返回  */
    if (!NeedFreezeMemTable()) break;
    /* memtable 满了，如果 imem 非空 等待
    minor compaction 将 imem 置为空 */
    MLog->info("DB wait until imem empty");
    while (imem_) background_work_done_cond_.wait(lock);
    MLog->info("DB wait imem empty ok");
    /* mem -> imem */
    FreezeMemTable();
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
/* 由于读 imem_ 需要锁定 */
RC DB::DoCompaction() {
  RC rc;
  lock_guard<mutex> lock(mutex_);
  is_compacting_ = true;

  if (imem_) {
    /* minor compaction */
    rc = DoMinorCompaction();
  } else {
    /* major compaction */
    rc = UN_IMPLEMENTED;
  }
  if (rc) {
    MLog->error("DB compaction failed: {}", strrc(rc));
    return rc;
  }

  is_compacting_ = false;
  /* TODO(adlternative) 可能产生 compaction 级联更新 但不一定是立刻
  （因为不是立即抢锁，或许可以修改它）*/
  // fmt::print("DB Compaction may need Compaction Again\n");
  if (NeedCompactions()) workers_[0]->Add([this]() { DoCompaction(); });
  // fmt::print("DB Compaction add Compaction Again\n");
  background_work_done_cond_.notify_all();
  return rc;
}

/* 由于写 imem_ 需要锁定 */
RC DB::DoMinorCompaction() {
  MLog->info("DB is doing minor compaction");
  if (auto rc = BuildSSTable(); rc) {
    MLog->error("DB build sstable failed: {}", strrc(rc));
    return rc;
  }
  imem_.reset();
  return OK;
}

/* imm --> sstable */
RC DB::BuildSSTable() {
  MLog->info("DB is building sstable");
  string sstable_path;
  if (auto rc = imem_->BuildSSTable(dbname_, sstable_path); rc) return rc;
  /* 更新元数据跟踪新 sstable */
  return OK;
}

/* mem -> imem */
/* 由于写 imem 和 mem 需要锁定 */
void DB::FreezeMemTable() {
  MLog->info("DB mem -> imem");
  imem_ = mem_;
  mem_ = make_shared<MemTable>(*options_);
}

/* 读 imem_ 需要锁定 */
bool DB::NeedCompactions() {
  if (closed_) return false;
  if (imem_) return true;
  // if (level0_files_ > options_->level0_files_limit) return true;
  // if (leveln_total_size_ > options_->leveln_total_size_limit) return true;
  return false;
}

/* 读 mem 需要锁定 */
bool DB::NeedFreezeMemTable() {
  return !closed_ && mem_->GetMemTableSize() > options_->mem_table_max_size;
}

}  // namespace adl