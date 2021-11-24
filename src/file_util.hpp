#ifndef ADL_LSM_TREE_FILE_UTIL_H__
#define ADL_LSM_TREE_FILE_UTIL_H__

#include <string>
#include "rc.hpp"

namespace adl {

enum FileOptions {
  DIR_,
  FILE_,
};

class FileManager {
 public:
  static bool Exists(const std::string &path);
  static bool IsDirectory(const std::string &path);
  static RC Create(const std::string &path, FileOptions options);
  static RC Destroy(const std::string &path);

};

}  // namespace adl

#endif  // ADL_LSM_TREE_FILE_UTIL_H__