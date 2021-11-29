#ifndef ADL_LSM_TREE_FILE_UTIL_H__
#define ADL_LSM_TREE_FILE_UTIL_H__

#include <string>
#include "rc.hpp"

namespace adl {

enum FileOptions {
  DIR_,
  FILE_,
};

class WritAbleFile;
class TempFile;

class FileManager {
 public:
  static bool Exists(const std::string &path);
  static bool IsDirectory(const std::string &path);
  static RC Create(const std::string &path, FileOptions options);
  static RC Destroy(const std::string &path);
  /* open */
  static RC OpenWritAbleFile(const std::string &filename,
                             WritAbleFile **result);
  static RC OpenTempFile(TempFile **result);
};

class WritAbleFile {
 public:
  WritAbleFile(const std::string &filename, int fd);
  WritAbleFile(const WritAbleFile &) = delete;
  WritAbleFile &operator=(const WritAbleFile &) = delete;
  virtual ~WritAbleFile();
  /* virtual */ RC Append(const std::string &data);
  /* virtual */ RC Close();
  /* virtual */ RC Flush();
  /* virtual */ RC Sync();

  static RC Open(const std::string &filename, WritAbleFile **result);

 public:
  static constexpr size_t kWritAbleFileBufferSize = 65536;

 protected:
  std::string filename_;
  int fd_;
  bool closed_;

  /* buffer */
  char buf_[kWritAbleFileBufferSize];
  size_t pos_;
};

class TempFile : public WritAbleFile {
 public:
  TempFile(const std::string &filename, int fd);
  RC ReName(const std::string &filename);
  static RC Open(TempFile **result);
};

ssize_t write_n(int fd, const char *buf, size_t len);

}  // namespace adl

#endif  // ADL_LSM_TREE_FILE_UTIL_H__