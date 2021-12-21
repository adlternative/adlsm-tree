#include "wal.hpp"
#include <unistd.h>
#include <cstdint>
#include <cstring>
#include "crc32c/crc32c.h"
#include "monitor_logger.hpp"
#include "rc.hpp"

namespace adl {
WAL::WAL(WritAbleFile *wal_file) { wal_file_.reset(wal_file); }

RC WAL::AddRecord(string_view data) {
  RC rc = OK;
  /* checksum */
  uint32_t check_sum = crc32c::Crc32c(data);
  if (rc = wal_file_->Append({(char *)&check_sum, sizeof(uint32_t)}); rc)
    return rc;
  /* type */
  int type = wal_kv_;
  if (rc = wal_file_->Append({(char *)&type, sizeof(int)}); rc) return rc;
  /* len */
  int lens = (int)data.length();
  if (rc = wal_file_->Append({(char *)&lens, sizeof(int)}); rc) return rc;
  /* data */
  if (rc = wal_file_->Append(data); rc) return rc;
  return rc;
}

RC WAL::Sync() {
  RC rc = wal_file_->Flush();
  if (rc) return rc;
  return wal_file_->Sync();
}

RC WAL::Close() { return wal_file_->Close(); }

RC WAL::Drop() {
  MLog->info("Drop WAL file {}", wal_file_->GetPath());
  return FileManager::Destroy(wal_file_->GetPath());
}

WALReader::WALReader(SeqReadFile *wal_file)
    : wal_file_(wal_file) /* , offset_(0)  */ {}

RC WALReader::ReadRecord(string &record) {
  RC rc = OK;
  string buffer;
  string_view view;
  uint32_t check_sum;
  int type;
  int lens;

  /* read head */
  if (rc = wal_file_->Read(sizeof(uint32_t) * 3, buffer, view); rc) {
    MLog->error("read WAL head error");
    return rc;
  }
  if (view.length() != sizeof(uint32_t) * 3) return FILE_EOF;
  /* checksum */
  memcpy(&check_sum, view.data(), sizeof(uint32_t));
  /* type */
  memcpy(&type, view.data() + sizeof(uint32_t), sizeof(int));
  if (type != wal_kv_) {
    MLog->error("read WAL type error");
    return BAD_RECORD;
  }
  /* len */
  memcpy(&lens, view.data() + sizeof(uint32_t) * 2, sizeof(int));
  /* data */
  if (rc = wal_file_->Read(lens, buffer, view); rc) {
    MLog->error("read WAL data error");
    return rc;
  }
  if (view.size() != lens) return FILE_EOF;
  if (check_sum != crc32c::Crc32c(view)) {
    MLog->error("check sum error");
    return CHECK_SUM_ERROR;
  }
  record.assign(view.data(), view.size());
  return rc;
}

RC WALReader::Drop() {
  MLog->info("Drop WAL file {}", wal_file_->GetPath());
  return FileManager::Destroy(wal_file_->GetPath());
}

RC WALReader::Close() { return wal_file_->Close(); }

}  // namespace adl