#ifndef ADL_LSM_TREE_FILE_UTIL_H__
#define ADL_LSM_TREE_FILE_UTIL_H__

#include <openssl/sha.h>
#include <functional>
#include <string>
#include <string_view>
#include <vector>
#include "keys.hpp"
#include "rc.hpp"
#include <fmt/ostream.h>

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
class WAL;
class WALReader;
class SeqReadFile;

class FileManager {
 public:
  static bool Exists(string_view path);
  static bool IsDirectory(string_view path);
  static RC Create(string_view path, FileOptions options);
  static RC Destroy(string_view path);
  static string FixDirName(string_view path);
  static string FixFileName(string_view path);
  static RC GetFileSize(string_view path, size_t *size);
  static RC ReName(string_view old_path, string_view new_path);
  static string HandleHomeDir(string_view path);
  /* open */
  static RC OpenWritAbleFile(string_view filename, WritAbleFile **result);
  static RC OpenTempFile(string_view dir_path, string_view subfix,
                         TempFile **result);
  static RC OpenAppendOnlyFile(string_view filename, WritAbleFile **result);

  static RC OpenSeqReadFile(string_view filename, SeqReadFile **result);

  static RC OpenMmapReadAbleFile(string_view filename,
                                 MmapReadAbleFile **result);
  static RC OpenRandomAccessFile(string_view filename,
                                 RandomAccessFile **result);
  static RC ReadFileToString(string_view filename, string &result);

  static RC OpenWAL(string_view dbname, int64_t log_number, WAL **result);
  static RC OpenWALReader(string_view dbname, int64_t log_number,
                          WALReader **result);
  static RC OpenWALReader(string_view wal_file_path, WALReader **result);

  static RC ReadDir(string_view directory_path, vector<string> &result);
  static RC ReadDir(string_view directory_path,
                    const std::function<bool(string_view)> &filter,
                    const std::function<void(string_view)> &handle_result);
};

/* 缓冲顺序写 */
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
  RC ReName(string_view filename);

  string GetPath();
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

/* 顺序读 */
class SeqReadFile {
 public:
  SeqReadFile(string_view filename, int fd);
  ~SeqReadFile();
  RC Read(size_t len, string &buffer, string_view &result);
  RC Skip(size_t n);
  string GetPath();
  RC Close();

 private:
  string filename_;
  int fd_;
  bool closed_;
};

/* 临时写文件 */
class TempFile : public WritAbleFile {
 public:
  TempFile(const std::string &filename, int fd);
  static RC Open(string_view dir_path, string_view subfix, TempFile **result);
};

/* mmap 读 */
class MmapReadAbleFile {
 public:
  MmapReadAbleFile(string_view file_name, char *base_addr, size_t file_size);
  ~MmapReadAbleFile();
  RC Read(size_t offset, size_t len, string_view &buffer);
  auto Size() { return file_size_; }
  string GetFileName() { return file_name_; }

 private:
  char *base_addr_;
  size_t file_size_;
  string file_name_;
};

/* 随机读 */
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
  unsigned char sha256[SHA256_DIGEST_LENGTH];

  /* 获取在 db 中的 file 路径 */
  string GetSSTablePath(string_view dbname);
  string GetOid() const;
  bool operator<(const FileMetaData &f) {
    return min_inner_key < f.min_inner_key;
  }
};

ostream &operator<<(ostream &os, const FileMetaData &meta);

ssize_t write_n(int fd, const char *buf, size_t len);
ssize_t read_n(int fd, const char *buf, size_t len);

string LevelDir(string_view dbname);
string LevelDir(string_view dbname, int n);
string LevelFile(string_view level_dir, string_view sha256_hex);
string RevDir(string_view dbname);
string RevFile(string_view rev_dir, string_view sha256_hex);
string CurrentFile(string_view dbname);
string SstDir(string_view dbname);
string SstFile(string_view sst_dir, string_view sha256_hex);
string WalDir(string_view dbname);
string WalFile(string_view wal_dir, int64_t log_number);

RC ParseWalFile(string_view filename, int64_t &seq);

}  // namespace adl

#endif  // ADL_LSM_TREE_FILE_UTIL_H__