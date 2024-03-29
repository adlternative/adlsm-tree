#include "file_util.hpp"
#include <fcntl.h>
#include <fmt/ostream.h>
#include <linux/limits.h>
#include <pwd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/dir.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <filesystem>
#include "defer.hpp"
#include "hash_util.hpp"
#include "monitor_logger.hpp"
#include "rc.hpp"
#include "sstable.hpp"
#include "wal.hpp"

namespace adl {

bool FileManager::Exists(string_view path) {
  struct stat sb;
  return stat(FileManager::FixFileName(path).c_str(), &sb) == 0;
}

bool FileManager::IsDirectory(string_view path) {
  struct stat sb;
  return stat(path.data(), &sb) == 0 && S_ISDIR(sb.st_mode);
}

RC FileManager::Create(std::string_view path, FileOptions options) {
  if (options == DIR_) {
    int err = mkdir(path.data(), 0755);
    if (err) {
      return CREATE_DIRECTORY_FAILED;
    }
  } else if (options == FILE_) {
    return UN_IMPLEMENTED;
  }
  return OK;
}

/* Dangerous rm -rf */
int remove_directory(const char *path) {
  DIR *d = opendir(path);
  size_t path_len = strlen(path);
  int r = -1;

  if (d) {
    struct dirent *p;

    r = 0;
    while (!r && (p = readdir(d))) {
      int r2 = -1;
      char *buf;
      size_t len;

      /* Skip the names "." and ".." and "/" as we
       * don't want to recurse on them. */
      if (!strcmp(p->d_name, ".") || !strcmp(p->d_name, "..") ||
          !strcmp(p->d_name, "/"))
        continue;

      len = path_len + strlen(p->d_name) + 2;
      buf = new char[len];

      if (buf) {
        struct stat statbuf;

        snprintf(buf, len, "%s/%s", path, p->d_name);
        if (!stat(buf, &statbuf)) {
          if (S_ISDIR(statbuf.st_mode))
            r2 = remove_directory(buf);
          else
            r2 = unlink(buf);
        }
        delete[] buf;
      }
      r = r2;
    }
    closedir(d);
  }

  if (!r) r = rmdir(path);

  return r;
}

RC FileManager::Destroy(string_view old_path) {
  auto path = HandleHomeDir(old_path);

  if (IsDirectory(path)) {
    int err = remove_directory(path.data());
    if (err) {
      return DESTROY_DIRECTORY_FAILED;
    }
  } else {
    int err = unlink(path.data());
    if (err) {
      return DESTROY_FILE_FAILED;
    }
  }
  return OK;
}

string FileManager::HandleHomeDir(string_view path) {
  string true_path;

  if (!path.starts_with("~")) {
    true_path = path;
  } else {
    const char *homedir;
    if (!(homedir = getenv("HOME"))) homedir = getpwuid(getuid())->pw_dir;
    if (homedir) {
      true_path = homedir;
      true_path += path.substr(1);
    }
  }
  return true_path;
}

string FileManager::FixFileName(string_view path) {
  if (path.starts_with("~")) return HandleHomeDir(path);
  return string(path);
}

string FileManager::FixDirName(string_view path) {
  if (path.empty()) return ".";
  string fix_path;
  if (path.starts_with("~")) {
    fix_path = HandleHomeDir(path);
  } else {
    fix_path = path;
  }
  if (fix_path.back() != '/') fix_path += "/";
  return fix_path;
}

RC FileManager::OpenWritAbleFile(string_view file_path, WritAbleFile **result) {
  return WritAbleFile::Open(file_path, result);
}

RC FileManager::OpenTempFile(string_view dir_path, string_view subfix,
                             TempFile **result) {
  return TempFile::Open(dir_path, subfix, result);
}

RC FileManager::OpenAppendOnlyFile(string_view filename,
                                   WritAbleFile **result) {
  int fd =
      ::open(filename.data(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
  if (fd < 0) {
    *result = nullptr;
    return OPEN_FILE_ERROR;
  }

  *result = new WritAbleFile(filename, fd);
  return OK;
}

RC FileManager::OpenSeqReadFile(string_view filename, SeqReadFile **result) {
  int fd = ::open(filename.data(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    *result = nullptr;
    return OPEN_FILE_ERROR;
  }

  *result = new SeqReadFile(filename, fd);
  return OK;
}

WritAbleFile::WritAbleFile(string_view file_path, int fd)
    : file_path_(file_path), fd_(fd), pos_(0), closed_(false) {}

RC WritAbleFile::Open(string_view file_path, WritAbleFile **result) {
  int fd =
      ::open(file_path.data(), O_TRUNC | O_WRONLY | O_CREAT | O_CLOEXEC, 0644);
  if (fd < 0) {
    *result = nullptr;
    return OPEN_FILE_ERROR;
  }

  *result = new WritAbleFile(file_path, fd);
  return OK;
}

SeqReadFile::SeqReadFile(string_view filename, int fd)
    : filename_(filename), fd_(fd), closed_(false) {}

SeqReadFile::~SeqReadFile() { Close(); }

RC SeqReadFile::Read(size_t len, string &buffer, string_view &result) {
  RC rc = OK;
  if (len > buffer.size()) buffer.resize(len);
  auto ret = read_n(fd_, buffer.data(), len);
  if (ret == -1) {
    MLog->error("read_n failed");
    return IO_ERROR;
  }
  result = {buffer.data(), (size_t)ret};
  return rc;
}

RC SeqReadFile::Skip(size_t n) {
  if (lseek(fd_, n, SEEK_CUR) == -1) {
    MLog->error("lseek failed");
    return IO_ERROR;
  }
  return OK;
}

string SeqReadFile::GetPath() { return filename_; }

RC SeqReadFile::Close() {
  if (!closed_) {
    int ret = close(fd_);
    if (ret == -1) return CLOSE_FILE_ERROR;
    closed_ = true;
    fd_ = -1;
  }
  return OK;
}

WritAbleFile::~WritAbleFile() {
  if (!closed_) Close();
}

RC WritAbleFile::Close() {
  if (pos_ > 0) {
    auto rc = Flush();
    if (rc) return rc;
  }
  if (fd_ != -1) {
    int ret = close(fd_);
    if (ret < 0) return CLOSE_FILE_ERROR;
  }

  closed_ = true;
  return OK;
}

ssize_t write_n(int fd, const char *buf, size_t len) {
  ssize_t n = 0;
  while (n < len) {
    ssize_t r = write(fd, buf + n, len - n);
    if (r < 0) {
      if (errno == EINTR) {
        continue;
      }
      return -1;
    }
    n += r;
  }
  return n;
}

ssize_t read_n(int fd, const char *buf, size_t len) {
  ssize_t n = 0;
  while (n < len) {
    ssize_t r = read(fd, (void *)(buf + n), len - n);
    if (r < 0) {
      if (errno == EINTR) {
        continue;
      }
      return -1;
    }
    if (r == 0) {
      return n;
    }
    n += r;
  }
  return n;
}

RC WritAbleFile::Flush() {
  if (pos_ > 0) {
    size_t written = write_n(fd_, buf_, pos_);
    if (written == -1) {
      return IO_ERROR;
    }
    pos_ = 0;
  }
  return OK;
}

RC WritAbleFile::Sync() {
  auto rc = Flush();
  if (rc == IO_ERROR) {
    return rc;
  }

  int ret = fdatasync(fd_);
  if (ret == -1) {
    return IO_ERROR;
  }
  return OK;
}

RC WritAbleFile::Append(string_view data) {
  auto data_len = data.size();

  /* 没有超过限制 */
  if (pos_ + data_len < kWritAbleFileBufferSize) {
    memcpy(buf_ + pos_, data.data(), data.size());
    pos_ += data_len;
    return OK;
  }

  /* 超过限制则 1. 刷盘 2. 拷贝 | 直接写 */
  auto written = kWritAbleFileBufferSize - pos_;
  memcpy(buf_ + pos_, data.data(), written);
  pos_ += written;
  auto left_data_len = data_len - written;

  auto rc = Flush();
  if (rc != OK) {
    return rc;
  }

  if (left_data_len > kWritAbleFileBufferSize) {
    written = write_n(fd_, data.data() + written, left_data_len);
    if (written != left_data_len) return IO_ERROR;
  } else {
    memcpy(buf_, data.data() + written, left_data_len);
    pos_ += left_data_len;
  }

  return OK;
}

RC WritAbleFile::ReName(string_view new_file) {
  string true_path = FileManager::FixFileName(new_file);
  int ret = rename(file_path_.c_str(), true_path.c_str());
  if (ret) {
    MLog->error("tempfile name:{} rename to {} failed: {}", file_path_,
                true_path, strerror(errno));
    return RENAME_FILE_ERROR;
  }
  file_path_ = std::move(true_path);
  // MLog->info("tempfile name:{} rename to {}", file_path_, true_path);
  return OK;
}

TempFile::TempFile(const std::string &file_path, int fd)
    : WritAbleFile(file_path, fd) {}

RC TempFile::Open(string_view dir_path, string_view subfix, TempFile **result) {
  string tmp_file(FileManager::FixDirName(dir_path));
  tmp_file += subfix;
  tmp_file += "XXXXXX";

  int fd = mkstemp(tmp_file.data());
  if (fd == -1) {
    *result = nullptr;
    MLog->error("mkstemp {} failed: {}", tmp_file, strerror(errno));
    return MAKESTEMP_ERROR;
  }

  std::string file_path =
      std::filesystem::read_symlink(std::filesystem::path("/proc/self/fd/") /
                                    std::to_string(fd))
          .string();
  // MLog->info("create new tempfile: {}", file_path);
  *result = new TempFile(file_path, fd);
  return OK;
}

RC FileManager::OpenWAL(string_view dbname, int64_t log_number, WAL **result) {
  /* open append only file for wal */
  string wal_file_name = WalFile(WalDir(dbname), log_number);
  WritAbleFile *wal_file = nullptr;

  if (auto rc = OpenAppendOnlyFile(wal_file_name, &wal_file); rc) {
    MLog->error("open wal file {} failed: {} {}", wal_file_name, strrc(rc),
                strerror(errno));
    return rc;
  }
  *result = new WAL(wal_file);
  MLog->info("wal_file {} created", wal_file_name);
  return OK;
}

RC FileManager::OpenWALReader(string_view dbname, int64_t log_number,
                              WALReader **result) {
  /* open append only file for wal */
  return OpenWALReader(WalFile(WalDir(dbname), log_number), result);
}

RC FileManager::OpenWALReader(string_view wal_file_path, WALReader **result) {
  /* open append only file for wal */
  SeqReadFile *seq_read_file = nullptr;
  if (auto rc = OpenSeqReadFile(wal_file_path, &seq_read_file); rc) return rc;
  *result = new WALReader(seq_read_file);
  return OK;
}

RC FileManager::OpenMmapReadAbleFile(string_view file_name,
                                     MmapReadAbleFile **result) {
  RC rc = OK;
  int fd = ::open(file_name.data(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    *result = nullptr;
    return OPEN_FILE_ERROR;
  }
  size_t file_size = 0;
  if (rc = GetFileSize(file_name, &file_size); rc) {
    MLog->error("Failed to open mmap file {}, error: {}", file_name, strrc(rc));
    return rc;
  }

  if (auto base_addr = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
      base_addr == MAP_FAILED) {
    MLog->error("Failed to mmap file {}, error: {}", file_name,
                strerror(errno));
    rc = MMAP_ERROR;
  } else {
    *result = new MmapReadAbleFile(file_name, (char *)base_addr, file_size);
    rc = OK;
  }

  if (close(fd)) {
    MLog->error("Failed to close mmap file {}, error: {}", file_name,
                strerror(errno));
    rc = CLOSE_FILE_ERROR;
  }
  return rc;
}

RC FileManager::ReadFileToString(string_view filename, string &result) {
  MmapReadAbleFile *mmap_readable_file = nullptr;
  RC rc = OK;
  if (rc = OpenMmapReadAbleFile(filename, &mmap_readable_file); rc) {
    MLog->error("ReadFileToString {} error: {}", filename, strrc(rc));
  } else {
    auto file_size = mmap_readable_file->Size();
    result.resize(file_size);
    string_view result_view;
    if (auto rc = mmap_readable_file->Read(0, file_size, result_view); rc)
      MLog->error("mmap_readable_file {} Read error: {}", filename, strrc(rc));
    result = result_view;
  }
  delete mmap_readable_file;
  return rc;
}

/* 获得一个文件夹的所有子文件 opendir+readdir+closedir */
RC FileManager::ReadDir(string_view directory_path,
                        std::vector<std::string> &result) {
  result.clear();
  DIR *dir = opendir(directory_path.data());
  if (dir == nullptr) return IO_ERROR;
  struct dirent *entry;
  while ((entry = readdir(dir)) != nullptr) result.emplace_back(entry->d_name);
  closedir(dir);
  return OK;
}

RC FileManager::ReadDir(string_view directory_path,
                        const std::function<bool(string_view)> &filter,
                        const std::function<void(string_view)> &handle_result) {
  DIR *dir = opendir(directory_path.data());
  if (dir == nullptr) return IO_ERROR;
  struct dirent *entry;
  while ((entry = readdir(dir)) != nullptr)
    if (filter(entry->d_name)) handle_result(entry->d_name);
  closedir(dir);
  return OK;
}

MmapReadAbleFile::MmapReadAbleFile(string_view file_name, char *base_addr,
                                   size_t file_size)
    : file_name_(file_name), base_addr_(base_addr), file_size_(file_size) {}

MmapReadAbleFile::~MmapReadAbleFile() {
  if (int ret = ::munmap((void *)base_addr_, file_size_); ret) {
    MLog->error("Failed to munmap file {}, error: {}", file_name_,
                strerror(errno));
  }
}

RC MmapReadAbleFile::Read(size_t offset, size_t len, string_view &buffer) {
  if (offset + len > file_size_) {
    buffer = {};
    return OUT_OF_RANGE;
  }
  buffer = {base_addr_ + offset, len};
  return OK;
}

RandomAccessFile::RandomAccessFile(string_view filename, int fd)
    : file_name_(filename), fd_(fd) {}

RC RandomAccessFile::Read(size_t offset, size_t len, string_view &buffer,
                          bool use_extra_buffer) {
  char *buf;
  if (!use_extra_buffer) {
    buf = new char[len];
  } else {
    buf = (char *)buffer.data();
  }
  /* better than seek + read ; which will not change file pointer */
  ssize_t read_len = pread(fd_, buf, len, (off_t)offset);
  if (read_len < 0) {
    if (!use_extra_buffer) delete[] buf;
    return IO_ERROR;
  }
  if (!use_extra_buffer) buffer = {buf, (size_t)read_len};
  return OK;
}

RandomAccessFile::~RandomAccessFile() {
  if (fd_ != -1) close(fd_);
}

string FileMetaData::GetSSTablePath(string_view dbname) {
  return SstFile(SstDir(dbname), sha256_digit_to_hex(sha256));
}

string FileMetaData::GetOid() const { return sha256_digit_to_hex(sha256); }

RC FileManager::GetFileSize(string_view path, size_t *size) {
  struct stat file_stat;
  if (stat(path.data(), &file_stat)) {
    MLog->error("can not get {} stat, error: {}", path.data(), strerror(errno));
    *size = 0;
    return STAT_FILE_ERROR;
  }
  *size = file_stat.st_size;
  return OK;
}

RC FileManager::ReName(string_view old_path, string_view new_path) {
  int ret = rename(old_path.data(), new_path.data());
  if (ret == -1) return RENAME_FILE_ERROR;
  return OK;
}

string LevelDir(string_view dbname) {
  string level_dir(dbname);
  if (!dbname.ends_with("/")) level_dir += '/';
  level_dir += "level/";
  return level_dir;
}

string LevelDir(string_view dbname, int n) {
  string level_dir(dbname);
  if (!dbname.ends_with("/")) level_dir += '/';
  level_dir += "level/" + to_string(n) + "/";
  return level_dir;
}

string LevelFile(string_view level_dir, string_view sha256_hex) {
  string file_path(level_dir);
  file_path += sha256_hex;
  file_path += ".lvl";
  return file_path;
}

string RevDir(string_view dbname) {
  string rev_dir(dbname);
  if (!dbname.ends_with("/")) rev_dir += '/';
  rev_dir += "rev/";
  return rev_dir;
}

string RevFile(string_view rev_dir, string_view sha256_hex) {
  string file_path(rev_dir);
  file_path += sha256_hex;
  file_path += ".rev";
  return file_path;
}

string CurrentFile(string_view dbname) {
  string file_path(dbname);
  if (!dbname.ends_with("/")) file_path += '/';
  file_path += "CURRENT";
  return file_path;
}

string SstDir(string_view dbname) {
  string level_dir(dbname);
  if (!dbname.ends_with("/")) level_dir += '/';
  level_dir += "sst/";
  return level_dir;
}

string SstFile(string_view sst_dir, string_view sst_oid) {
  return fmt::format("{}{}.sst", sst_dir, sst_oid);
}

string WalDir(string_view dbname) {
  string wal_dir(dbname);
  if (!dbname.ends_with("/")) wal_dir += '/';
  wal_dir += "wal/";
  return wal_dir;
}

string WalFile(string_view wal_dir, int64_t log_number) {
  return fmt::format("{}{}.wal", wal_dir, log_number);
}

RC ParseWalFile(string_view filename, int64_t &seq) {
  std::istringstream iss(
      string(filename.substr(0, filename.length() - strlen(".wal"))));
  iss >> seq;
  return OK;
}

std::string WritAbleFile::GetPath() { return file_path_; }
std::ostream &operator<<(ostream &os, const FileMetaData &meta) {
  os << fmt::format(
      "@FileMetaData[ file_size={}, num_keys={}, max_seq={}, belong_to_level={}, "
      "max_inner_key={} "
      "min_inner_key={}, sha256={} ]\n",
      meta.file_size, meta.num_keys, meta.max_seq, meta.belong_to_level,
      meta.max_inner_key, meta.min_inner_key, sha256_digit_to_hex(meta.sha256));
  return os;
}
}  // namespace adl