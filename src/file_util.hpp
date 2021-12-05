#ifndef ADL_LSM_TREE_FILE_UTIL_H__
#define ADL_LSM_TREE_FILE_UTIL_H__

#include <string>
#include <string_view>
#include "rc.hpp"

namespace adl {

using namespace std;

enum FileOptions {
  DIR_,
  FILE_,
};

class WritAbleFile;
class TempFile;

class FileManager {
 public:
  static bool Exists(string_view path);
  static bool IsDirectory(string_view path);
  static RC Create(string_view path, FileOptions options);
  static RC Destroy(string_view path);
  /* open */
  static RC OpenWritAbleFile(string_view filename,
                             WritAbleFile **result);
  static RC OpenTempFile(TempFile **result);
};

class WritAbleFile {
 public:
  WritAbleFile(string_view filename, int fd);
  WritAbleFile(const WritAbleFile &) = delete;
  WritAbleFile &operator=(const WritAbleFile &) = delete;
  virtual ~WritAbleFile();
  /* virtual */ RC Append(const std::string &data);
  /* virtual */ RC Close();
  /* virtual */ RC Flush();
  /* virtual */ RC Sync();

  static RC Open(string_view filename, WritAbleFile **result);

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
  RC ReName(string_view filename);
  static RC Open(TempFile **result);
};

ssize_t write_n(int fd, const char *buf, size_t len);

}  // namespace adl

#endif  // ADL_LSM_TREE_FILE_UTIL_H__