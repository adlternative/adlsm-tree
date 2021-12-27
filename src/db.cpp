#include "db.hpp"
#include <openssl/sha.h>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <sstream>
#include "defer.hpp"
#include "file_util.hpp"
#include "mem_table.hpp"
#include "monitor_logger.hpp"
#include "rc.hpp"
#include "revision.hpp"

namespace adl {

DB::DB(string_view dbname, DBOptions &options)
    : dbname_(dbname),
      sequence_id_(0),
      log_number_(0),
      options_(&options),
      mem_(nullptr),
      imem_(nullptr),
      closed_(false),
      is_compacting_(false),
      current_rev_(make_shared<Revision>()),
      save_backgound_rc_(OK) {
  /* 后台工作者线程，目前只有一个线程 */
  MLogger.SetDbNameAndOptions(dbname_, &options);
  for (int i = 0; i < options_->background_workers_number; i++)
    workers_.push_back(Worker::NewBackgroundWorker());
  MLog->info("DB will run in {}", dbname_);
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
  RC rc = OK;
  string true_dbname(FileManager::FixDirName(dbname));
  if (!FileManager::Exists(true_dbname))
    return options.create_if_not_exists ? Create(true_dbname, options, dbptr)
                                        : NOT_FOUND;
  if (!FileManager::IsDirectory(true_dbname)) return IS_NOT_DIRECTORY;

  DB *db = new DB(true_dbname, options);
  defer def([&]() {
    if (rc) delete db;
  });

  /* load metadata... (current_rev) */
  if (rc = db->LoadMetaData(); rc) return rc;
  if (!db->mem_) {
    /* init memtable */
    WAL *wal = nullptr;
    int64_t log_number = db->log_number_.fetch_add(1, memory_order_relaxed);
    rc = FileManager::OpenWAL(dbname, log_number, &wal);
    if (rc) return rc;
    db->current_rev_->PushLogNumber(log_number);
    db->mem_ = make_shared<MemTable>(*db->options_, wal);
    db->WriteCurrentRev(&*db->current_rev_);
  }
  *dbptr = db;
  return OK;
}

RC DB::Create(string_view dbname, DBOptions &options, DB **dbptr) {
  RC rc = OK;
  if (rc = FileManager::Create(dbname, DIR_); rc) {
    MLog->error("Create db {} failed", dbname);
    return rc;
  }
  if (rc = FileManager::Create(RevDir(dbname), DIR_); rc) {
    MLog->error("Create rev dir {} failed", RevDir(dbname));
    return rc;
  }
  if (rc = FileManager::Create(LevelDir(dbname), DIR_); rc) {
    MLog->error("Create level dir {} failed", LevelDir(dbname));
    return rc;
  }
  for (int i = 0; i < 5; i++) {
    if (rc = FileManager::Create(LevelDir(dbname, i), DIR_); rc) {
      MLog->error("Create level dir {} failed", LevelDir(dbname, i));
      return rc;
    }
  }
  if (rc = FileManager::Create(SstDir(dbname), DIR_); rc) {
    MLog->error("Create sst dir {} failed", SstDir(dbname));
    return rc;
  }
  if (rc = FileManager::Create(WalDir(dbname), DIR_); rc) {
    MLog->error("Create sst dir {} failed", SstDir(dbname));
    return rc;
  }

  DB *db = new DB(dbname, options);
  defer def([&]() {
    if (rc) delete db;
  });

  /* 建立初始版本文件 */
  if (rc = db->current_rev_->BuildFile(dbname); rc) {
    MLog->error("Create current rev file {} failed", dbname);
    return rc;
  }
  /* init memtable */
  WAL *wal = nullptr;
  int64_t log_number = db->log_number_.fetch_add(1, memory_order_relaxed);
  rc = FileManager::OpenWAL(dbname, log_number, &wal);
  if (rc) return rc;
  db->current_rev_->PushLogNumber(log_number);
  db->mem_ = make_shared<MemTable>(*db->options_, wal);
  db->WriteCurrentRev(&*db->current_rev_);
  MLog->info("DB init rev {}", db->current_rev_->GetOid());
  *dbptr = db;
  return rc;
}

RC DB::Destroy(string_view dbname) {
  // stop all services
  return FileManager::Destroy(dbname);
}

RC DB::Close() {
  // stop all services
  closed_ = true;
  return OK;
}

RC DB::Sync() {
  /* 等待 IMM?MEM?WAL? write to disk? */
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
  if (auto rc = Write(key, "", OP_DELETE); rc) {
    MLog->error("Delete key:{} failed", key);
    return rc;
  }
  return OK;
}

RC DB::Get(string_view key, std::string &value) {
  RC rc = OK;
  unique_lock<mutex> lock(mutex_);
  auto mem = mem_;
  auto imem = imem_;
  auto current_rev = current_rev_;

  // int64_t sequence_id_ = INT64_MAX;
  lock.unlock();
  /* 1. memtable */
  rc = mem->Get(key, value);
  if (!rc) {
    MLog->debug("Get key {} hint in memtable", key);
    return rc;
  }
  /* 2. imemtable */
  if (imem) {
    rc = imem->GetNoLock(key, value);
    if (!rc) {
      MLog->debug("Get key {} hint in imemtable", key);
      return rc;
    }
  }

  /* 3 L0 sstable | L1-LN sstable 需要依赖于版本控制 元数据管理 */
  rc = current_rev->Get(key, value);
  if (!rc) {
    MLog->debug("Get key {} hint in current rev {}", key,
                current_rev_->GetOid());
    return rc;
  }
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

  /* write memtable and wal */
  MemKey mem_key(key, sequence_id_.fetch_add(1), op);
  return mem_->PutTeeWAL(mem_key, value);
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
    while (imem_ && !save_backgound_rc_ && !closed_)
      background_work_done_cond_.wait(lock);
    if (closed_) return OK;
    if (save_backgound_rc_) return save_backgound_rc_;
    MLog->info("DB wait imem empty ok");
    /* mem -> imem */
    FreezeMemTable();
    /* 检查是否需要 Compaction */
    if (!NeedCompactions()) break;
    workers_[0]->Add([this]() { DoCompaction(); });
    /* wait for compaction done */
    background_work_done_cond_.wait(lock);
    if (save_backgound_rc_) return save_backgound_rc_;
  }
  lock.release();
  return closed_ ? DB_CLOSED : OK;
}

/* background thread do compaction */
/* 由于读 imem_ 需要锁定 */
RC DB::DoCompaction() {
  RC rc = OK;
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
    save_backgound_rc_ = rc;
    MLog->error("DB compaction failed: {}", strrc(rc));
  } else {
    is_compacting_ = false;
    /* TODO(adlternative) 可能产生 compaction 级联更新 但不一定是立刻
    （因为不是立即抢锁，或许可以修改它）*/
    // fmt::print("DB Compaction may need Compaction Again\n");
    if (NeedCompactions()) workers_[0]->Add([this]() { DoCompaction(); });
    // fmt::print("DB Compaction add Compaction Again\n");
  }
  background_work_done_cond_.notify_all();
  return rc;
}

/* 由于写 imem_ 需要锁定 */
RC DB::DoMinorCompaction() {
  MLog->info("DB is doing minor compaction");
  if (auto rc = BuildSSTable(imem_); rc) {
    MLog->error("DB build sstable failed: {}", strrc(rc));
    return rc;
  }
  /* 由于memtable 已经落盘，丢弃 wal + Imem  */
  imem_->DropWAL();
  imem_.reset();
  return OK;
}

/* imm --> sstable */
RC DB::BuildSSTable(const shared_ptr<adl::MemTable> &mem) {
  RC rc = OK;
  MLog->info("DB is building sstable");
  if (mem->Empty()) return NOEXCEPT_SIZE;
  FileMetaData *sstable_file_meta = nullptr;
  /* 内存数据刷盘  创建新 SSTable 对象文件*/
  if (auto rc = mem->BuildSSTable(dbname_, &sstable_file_meta); rc) return rc;
  /* 更新层级文件元数据 */
  vector<Level> new_levels = current_rev_->GetLevels();
  if (!sstable_file_meta || sstable_file_meta->belong_to_level >= 5 ||
      sstable_file_meta->belong_to_level < 0)
    return BAD_FILE_META;
  auto &new_level = new_levels[sstable_file_meta->belong_to_level];
  new_level.Insert(sstable_file_meta);
  /* 创建新层级对象文件 */
  if (auto rc = new_level.BuildFile(dbname_); rc) return rc;

  /* 最旧的 wal 不需要了 */
  deque<int64_t> new_log_nums = current_rev_->GetLogNumbers();
  if (new_log_nums.size() < 1) return NOEXCEPT_SIZE;
  new_log_nums.pop_front();

  /* 创建新版本对象文件 */
  auto new_revision =
      new Revision(std::move(new_levels), std::move(new_log_nums));
  defer de([&]() {
    if (rc) delete new_revision;
  });

  if (rc = new_revision->BuildFile(dbname_); rc) return rc;

  /* 更新 current 引用 */
  if (rc = UpdateCurrentRev(new_revision); rc) return rc;
  return OK;
}

/* mem -> imem */
/* 由于写 imem 和 mem 需要锁定 */
RC DB::FreezeMemTable() {
  MLog->info("DB mem -> imem");
  imem_ = mem_;
  WAL *wal = nullptr;
  int64_t log_number = log_number_.fetch_add(1, memory_order_relaxed);
  auto rc = FileManager::OpenWAL(dbname_, log_number, &wal);
  if (rc) return rc;
  current_rev_->PushLogNumber(log_number);
  mem_ = make_shared<MemTable>(*options_, wal);
  WriteCurrentRev(&*current_rev_);
  return OK;
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

RC DB::UpdateCurrentRev(Revision *rev) {
  WriteCurrentRev(rev);
  MLog->info("DB update current rev from {} to {}", current_rev_->GetOid(),
             rev->GetOid());
  current_rev_.reset(rev);
  return OK;
}

RC DB::WriteCurrentRev(string_view write_buffer) {
  TempFile *temp_current_file = nullptr;
  string current_file_path = CurrentFile(dbname_);
  auto rc = FileManager::OpenTempFile(dbname_, "tmp_cur", &temp_current_file);
  if (rc) return rc;
  temp_current_file->Append(write_buffer);
  temp_current_file->Sync();
  if (rc = temp_current_file->ReName(current_file_path); rc) return rc;
  if (rc = temp_current_file->Close(); rc) return rc;
  MLog->info("'{}' have been written to CURRENT", write_buffer);
  delete temp_current_file;
  return OK;
}

RC DB::WriteCurrentRev(Revision *rev) {
  const auto &log_numbers = rev->GetLogNumbers();
  string write_buffer = fmt::format("{} {}", rev->GetOid(), log_numbers.size());
  for (int i = 0; i < log_numbers.size(); i++)
    write_buffer += " " + to_string(log_numbers[i]);
  write_buffer += '\n';
  return WriteCurrentRev(write_buffer);
}

RC DB::ParseCurrentRev(string_view current_file_data,
                       string &current_rev_sha_hex, deque<int64_t> &log_nums) {
  if (current_file_data.size() < 2 * SHA256_DIGEST_LENGTH + 1)
    return BAD_CURRENT_FILE;
  current_rev_sha_hex = current_file_data.substr(0, 2 * SHA256_DIGEST_LENGTH);
  string log_num_str(current_file_data.substr(2 * SHA256_DIGEST_LENGTH + 1));
  stringstream ss(log_num_str);
  int log_nums_size;
  ss >> log_nums_size;
  for (int i = 0; i < log_nums_size; i++) {
    int64_t log_num;
    ss >> log_num;
    log_nums.push_back(log_num);
  }
  return OK;
}

RC DB::LoadWALs(const deque<int64_t> &log_nums) {
  RC rc = OK;
  int log_nums_size = (int)log_nums.size();
  if (!log_nums_size) MLog->info("No wal for loading");
  MLog->info("there are {} wals", log_nums_size);

  for (int i = 0; i < log_nums_size; i++) {
    auto current_rev = current_rev_;
    current_rev->PushLogNumber(log_nums[i]);
    if (rc = LoadWAL(WalFile(WalDir(dbname_), log_nums[i])); rc) return rc;
    current_rev->PopLogNumber();
    log_number_ = log_nums[i] + 1;
  }
  return rc;
}

/**
 * @brief 加载预写日志，目前的策略是将每个日志写到 memtable 中, 如果发现大于
 * memtable 大小则刷盘，
 */
RC DB::LoadWAL(string_view wal_file_path) {
  MLog->trace("DB::LoadWAL");
  RC rc = OK;
  string record;
  WALReader *wal_reader = nullptr;
  if (rc = FileManager::OpenWALReader(wal_file_path, &wal_reader); rc)
    return rc;
  /* 将日志中读出的每条记录添加到 memtable 中 */
  auto mem = make_shared<MemTable>(*options_);

  /* 但在出现错误开始，后面的记录丢弃 */
  for (rc = wal_reader->ReadRecord(record); !rc;
       rc = wal_reader->ReadRecord(record)) {
    string value;
    MemKey memkey;
    DecodeKVPair(record, memkey, value);
    /* 如果没有 memtable 则创建 */
    if (!mem) mem = make_shared<MemTable>(*options_);
    rc = mem->Put(memkey, value);
    if (rc) break;

    /* 看 memtable 是否超额 需要落盘 */
    if (mem->GetMemTableSize() > options_->mem_table_max_size) {
      rc = BuildSSTable(mem);
      if (rc) break;
      mem.reset();
    }
  }
  if (rc && rc != FILE_EOF && rc != BAD_RECORD) {
    MLog->error("LoadWAL {} failed, rc = {}", wal_file_path, rc);
    return rc;
  }
  /* 如果 memtable 中还有没写完的内容则再次刷盘（后期这里可以做优化复用 mem_）
   */
  if (mem) {
    rc = BuildSSTable(mem);
    mem.reset();
  }

  /* 全部落盘成功删除 wal */
  wal_reader->Close();
  wal_reader->Drop();
  delete wal_reader;
  return OK;
}

RC DB::LoadMetaData() {
  // 1 load current file
  string current_rev_sha_hex;
  string current_file_data;
  string current_file = CurrentFile(dbname_);
  if (!FileManager::Exists(current_file)) {
    return OK;
  }
  /* 读取 current  */
  if (auto rc = FileManager::ReadFileToString(current_file, current_file_data);
      rc) {
    MLog->error("DB load current file failed: {}", strrc(rc));
    return rc;
  }
  if (current_file_data[current_file_data.size() - 1] == '\n')
    current_file_data.pop_back();

  deque<int64_t> log_nums;
  /* 从 current 文件中解析 current rev oid + log numbers */
  if (auto rc =
          ParseCurrentRev(current_file_data, current_rev_sha_hex, log_nums);
      rc)
    return rc;

  MLog->info("DB load current file success: oid:{}", current_rev_sha_hex);

  // 2 load current rev
  if (auto rc = current_rev_->LoadFromFile(dbname_, current_rev_sha_hex); rc) {
    MLog->error("DB load current rev {} failed: {}", current_rev_sha_hex,
                strrc(rc));
    return rc;
  }

  /* load current rev WAL */
  if (auto rc = LoadWALs(log_nums); rc) return rc;

  sequence_id_ = current_rev_->GetMaxSeq() + 1;
  return OK;
}

}  // namespace adl