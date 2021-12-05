#ifndef ADL_LSM_TREE_RC_H__
#define ADL_LSM_TREE_RC_H__

#include <string>

namespace adl {

enum RC {
  OK,
  NOT_FOUND,
  IS_NOT_DIRECTORY,
  CREATE_DIRECTORY_FAILED,
  DESTROY_DIRECTORY_FAILED,
  DESTROY_FILE_FAILED,
  UN_IMPLEMENTED,
  EXISTED,
  OPEN_FILE_ERROR,
  IO_ERROR,
  CLOSE_FILE_ERROR,
  RENAME_FILE_ERROR,
  MAKESTEMP_ERROR,
  FILTER_BLOCK_ERROR,
  DB_CLOSED,
};

inline std::string strrc(RC rc) {
  switch (rc) {
    case OK:
      return "ok";
    case NOT_FOUND:
      return "not found";
    case IS_NOT_DIRECTORY:
      return "is not directory";
    case CREATE_DIRECTORY_FAILED:
      return "create directory failed";
    case EXISTED:
      return "have existed";
    case UN_IMPLEMENTED:
      return "unimplemented feature";
    default:
      return "unknown error";
  }
}

}  // namespace adl

#endif  // ADL_LSM_TREE_RC_H__