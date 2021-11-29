#include "file_util.hpp"
#include <fcntl.h>
#include <linux/limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/dir.h>
#include <sys/stat.h>
#include <unistd.h>
#include <filesystem>
#include "rc.hpp"

namespace adl {

bool FileManager::Exists(const std::string &path) {
  struct stat sb;
  return stat(path.c_str(), &sb) == 0;
}

bool FileManager::IsDirectory(const std::string &path) {
  struct stat sb;
  return stat(path.c_str(), &sb) == 0 && S_ISDIR(sb.st_mode);
}

RC FileManager::Create(const std::string &path, FileOptions options) {
  if (options == DIR_) {
    int err = mkdir(path.c_str(), 0755);
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

RC FileManager::Destroy(const std::string &path) {
  if (IsDirectory(path)) {
    int err = remove_directory(path.c_str());
    if (err) {
      return DESTROY_DIRECTORY_FAILED;
    }
  } else {
    int err = unlink(path.c_str());
    if (err) {
      return DESTROY_FILE_FAILED;
    }
  }
  return OK;
}

RC FileManager::OpenWritAbleFile(const std::string &filename,
                                 WritAbleFile **result) {
  return WritAbleFile::Open(filename, result);
}

RC FileManager::OpenTempFile(TempFile **result) {
  return TempFile::Open(result);
}

WritAbleFile::WritAbleFile(const std::string &filename, int fd)
    : filename_(filename), fd_(fd), pos_(0), closed_(false) {}

RC WritAbleFile::Open(const std::string &filename, WritAbleFile **result) {
  int fd =
      ::open(filename.c_str(), O_TRUNC | O_WRONLY | O_CREAT | O_CLOEXEC, 0644);
  if (fd < 0) {
    *result = nullptr;
    return OPEN_FILE_ERROR;
  }

  *result = new WritAbleFile(filename, fd);
  return OK;
}

WritAbleFile::~WritAbleFile() {
  if (!closed_) Close();
}

RC WritAbleFile::Close() {
  if (pos_ > 0) {
    auto rc = Flush();
    if (rc != OK) return rc;
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

TempFile::TempFile(const std::string &filename, int fd)
    : WritAbleFile(filename, fd) {}

RC TempFile::ReName(const std::string &new_file) {
  int ret = rename(filename_.c_str(), new_file.c_str());
  if (ret == -1) return RENAME_FILE_ERROR;
  return OK;
}

RC TempFile::Open(TempFile **result) {
  static char tmpfile[] = "/tmp/adl_XXXXXX";
  int fd = mkstemp(tmpfile);
  if (fd == -1) {
    *result = nullptr;
    return MAKESTEMP_ERROR;
  }
  std::string filename =
      std::filesystem::read_symlink(std::filesystem::path("/proc/self/fd/") /
                                    std::to_string(fd))
          .string();

  *result = new TempFile(filename, fd);
  return OK;
}

}  // namespace adl