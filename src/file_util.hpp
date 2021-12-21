#ifndef ADL_LSM_TREE_FILE_UTIL_H__
#define ADL_LSM_TREE_FILE_UTIL_H__

#include <openssl/sha.h>
#include <string>
#include <string_view>
#include "keys.hpp"
#include "rc.hpp"

namespace adl {

using namespace std;

enum FileOptions {
  DIR_,
  FILE_,
};

class WritAbleFile;
class TempFile;
class MmapReadAbleFile;
class RandomAccessFile;

class FileManager {
 public:
  static bool Exists(string_view path);
  static bool IsDirectory(string_view path);
  static RC Create(string_view path, FileOptions options);
  static RC Destroy(string_view path);
  static string FixDirName(string_view path);
  static string FixFileName(string_view path);
  static RC GetFileSize(string_view path, size_t *size);

  /* open */
  static RC OpenWritAbleFile(string_view filename, WritAbleFile **result);
  static RC OpenTempFile(string_view dir_path, string_view subfix,
                         TempFile **result);
  static RC OpenMmapReadAbleFile(string_view filename,
                                 MmapReadAbleFile **result);
  static RC OpenRandomAccessFile(string_view filename,
                                 RandomAccessFile **result);
  static RC ReadFileToString(string_view filename, string &result);
};

class WritAbleFile {
 public:
  WritAbleFile(string_view filename, int fd);
  WritAbleFile(const WritAbleFile &) = delete;
  WritAbleFile &operator=(const WritAbleFile &) = delete;
  virtual ~WritAbleFile();
  /* virtual */ RC Append(string_view data);
  /* virtual */ RC Close();
  /* virtual */ RC Flush();
  /* virtual */ RC Sync();

  static RC Open(string_view filename, WritAbleFile **result);

 public:
  static constexpr size_t kWritAbleFileBufferSize = 65536;

 protected:
  std::string file_path_;
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
  static RC Open(string_view dir_path, string_view subfix, TempFile **result);
};

ssize_t write_n(int fd, const char *buf, size_t len);

class MmapReadAbleFile {
 public:
  MmapReadAbleFile(string_view file_name, char *base_addr, size_t file_size);
  ~MmapReadAbleFile();
  RC Read(size_t offset, size_t len, string_view &buffer);
  auto Size() { return file_size_; }

 private:
  char *base_addr_;
  size_t file_size_;
  string file_name_;
};

class RandomAccessFile {
 public:
  RandomAccessFile(string_view filename, int fd);
  RC Read(size_t offset, size_t len, string_view &buffer,
          bool use_extra_buffer = false);
  ~RandomAccessFile();

 private:
  string file_name_;
  int fd_;
};

/**
 * @brief 文件元数据
 */
struct FileMetaData {
  FileMetaData() : file_size(0), num_keys(0), belong_to_level(-1) {}

  size_t file_size;
  int num_keys;
  int belong_to_level;
  MemKey max_inner_key;
  MemKey min_inner_key;
  string sstable_path;
  unsigned char sha256[SHA256_DIGEST_LENGTH];

  bool operator<(const FileMetaData &f) {
    return min_inner_key < f.min_inner_key;
  }
};

string LevelDir(string_view dbname);
string LevelDir(string_view dbname, int n);
string LevelFile(string_view level_dir, string_view sha256_hex);
string RevDir(string_view dbname);
string RevFile(string_view rev_dir, string_view sha256_hex);
string CurrentFile(string_view dbname);
string SstDir(string_view dbname);
string SstFile(string_view sst_dir, string_view sha256_hex);

}  // namespace adl

#endif  // ADL_LSM_TREE_FILE_UTIL_H__