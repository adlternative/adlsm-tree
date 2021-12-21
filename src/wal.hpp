#ifndef ADL_LSM_TREE_WAL_H__
#define ADL_LSM_TREE_WAL_H__

#include <memory>
#include <string>
#include <string_view>
#include "file_util.hpp"
#include "rc.hpp"
namespace adl {
using namespace std;
enum WALDataType { wal_kv_ };
class WAL {
 public:
  WAL(WritAbleFile *wal_file);
  virtual RC AddRecord(string_view data);
  RC Sync();
  RC Close();
  RC Drop();
  virtual ~WAL() = default;

 protected:
  /* 预写日志文件 */
  unique_ptr<WritAbleFile> wal_file_;
};

class SeqReadFile;
class WALReader {
 public:
  WALReader(SeqReadFile *wal_file);
  RC ReadRecord(string &record);
  RC Drop();
  RC Close();

 private:
  /* 预写日志文件 */
  unique_ptr<SeqReadFile> wal_file_;
};

}  // namespace adl
#endif  // ADL_LSM_TREE_WAL_H__