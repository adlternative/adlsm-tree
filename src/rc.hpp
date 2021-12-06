#ifndef ADL_LSM_TREE_RC_H__
#define ADL_LSM_TREE_RC_H__

#include <string_view>

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
  STAT_FILE_ERROR,
  MMAP_ERROR,
  OUT_OF_RANGE,
};

inline std::string_view strrc(RC rc) {
  switch (rc) {
    case OK:
      return "ok";
    case NOT_FOUND:
      return "not found";
    case IS_NOT_DIRECTORY:
      return "is not directory";
    case CREATE_DIRECTORY_FAILED:
      return "create directory failed";
    case DESTROY_DIRECTORY_FAILED:
      return "destroy directory failed";
    case DESTROY_FILE_FAILED:
      return "destroy file failed";
    case UN_IMPLEMENTED:
      return "un implemented";
    case EXISTED:
      return "existed";
    case OPEN_FILE_ERROR:
      return "open file error";
    case IO_ERROR:
      return "io error";
    case CLOSE_FILE_ERROR:
      return "close file error";
    case RENAME_FILE_ERROR:
      return "rename file error";
    case MAKESTEMP_ERROR:
      return "make temp error";
    case FILTER_BLOCK_ERROR:
      return "filter block error";
    case DB_CLOSED:
      return "db closed";
    case STAT_FILE_ERROR:
      return "stat file error";
    case MMAP_ERROR:
      return "mmap error";
    case OUT_OF_RANGE:
      return "out of range";
    default:
      return "unknown error";
  }
}

}  // namespace adl

#endif  // ADL_LSM_TREE_RC_H__