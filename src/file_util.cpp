#include "file_util.hpp"
#include <fcntl.h>
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
#include "monitor_logger.hpp"
#include "rc.hpp"

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

RC FileManager::Destroy(string_view path) {
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

string FileManager::FixFileName(string_view path) {
  string true_path;
  if (path[0] == '~') {
    const char *homedir;
    if (!(homedir = getenv("HOME"))) homedir = getpwuid(getuid())->pw_dir;
    if (homedir) {
      true_path = homedir;
      true_path += path.substr(1);
      path = true_path;
    }
  }
  return true_path;
}

string FileManager::FixDirName(string_view path) {
  string true_path;
  if (path.empty()) return ".";
  if (path[0] == '~') {
    const char *homedir;
    if (!(homedir = getenv("HOME"))) homedir = getpwuid(getuid())->pw_dir;
    if (homedir) {
      true_path = homedir;
      true_path += path.substr(1);
      path = true_path;
    }
  }

  if (path.back() == '/') {
    return string(path);
  }
  return string(path) + "/";
}

RC FileManager::OpenWritAbleFile(string_view file_path, WritAbleFile **result) {
  return WritAbleFile::Open(file_path, result);
}

RC FileManager::OpenTempFile(string_view dir_path, string_view subfix,
                             TempFile **result) {
  return TempFile::Open(dir_path, subfix, result);
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

RC WritAbleFile::Append(const std::string &data) {
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

TempFile::TempFile(const std::string &file_path, int fd)
    : WritAbleFile(file_path, fd) {}

RC TempFile::ReName(string_view new_file) {
  string true_path = FileManager::FixFileName(new_file);
  int ret = rename(file_path_.c_str(), true_path.c_str());
  if (ret) {
    MLogger->error("tempfile name:{} rename to {} failed: {}", file_path_,
                   true_path, strerror(errno));
    return RENAME_FILE_ERROR;
  }
  MLogger->info("tempfile name:{} rename to {}", file_path_, true_path);
  return OK;
}

RC TempFile::Open(string_view dir_path, string_view subfix, TempFile **result) {
  string tmp_file(FileManager::FixDirName(dir_path));
  tmp_file += subfix;
  tmp_file += "XXXXXX";

  int fd = mkstemp(tmp_file.data());
  if (fd == -1) {
    *result = nullptr;
    MLogger->error("mkstemp {} failed: {}", tmp_file, strerror(errno));
    return MAKESTEMP_ERROR;
  }

  std::string file_path =
      std::filesystem::read_symlink(std::filesystem::path("/proc/self/fd/") /
                                    std::to_string(fd))
          .string();
  MLogger->info("create new tempfile: {}", file_path);
  *result = new TempFile(file_path, fd);
  return OK;
}

RC FileManager::OpenMmapReadAbleFile(string_view file_name,
                                     MmapReadAbleFile **result) {
  RC rc;
  int fd = ::open(file_name.data(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    *result = nullptr;
    return OPEN_FILE_ERROR;
  }
  size_t file_size = 0;
  if (rc = GetFileSize(file_name, &file_size); rc) {
    MLogger->error("Failed to open mmap file {}, error: {}", file_name,
                   strrc(rc));
    return rc;
  }

  if (auto base_addr = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
      base_addr == MAP_FAILED) {
    MLogger->error("Failed to mmap file {}, error: {}", file_name,
                   strerror(errno));
    rc = MMAP_ERROR;
  } else {
    *result = new MmapReadAbleFile(file_name, (char *)base_addr, file_size);
    rc = OK;
  }

  if (close(fd)) {
    MLogger->error("Failed to close mmap file {}, error: {}", file_name,
                   strerror(errno));
    rc = CLOSE_FILE_ERROR;
  }
  return rc;
}

MmapReadAbleFile::MmapReadAbleFile(string_view file_name, char *base_addr,
                                   size_t file_size)
    : file_name_(file_name), base_addr_(base_addr), file_size_(file_size) {}

MmapReadAbleFile::~MmapReadAbleFile() {
  if (int ret = ::munmap((void *)base_addr_, file_size_); ret) {
    MLogger->error("Failed to munmap file {}, error: {}", file_name_,
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

RC FileManager::GetFileSize(string_view path, size_t *size) {
  struct stat file_stat;
  if (stat(path.data(), &file_stat)) {
    MLogger->error("can not get {} stat, error: {}", path.data(),
                   strerror(errno));
    *size = 0;
    return STAT_FILE_ERROR;
  }
  *size = file_stat.st_size;
  return OK;
}

}  // namespace adl