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
  FOOTER_BLOCK_ERROR,
  UN_SUPPORTED_FORMAT,
  DB_CLOSED,
  STAT_FILE_ERROR,
  MMAP_ERROR,
  OUT_OF_RANGE,
  BAD_LEVEL,
  BAD_REVISION,
  BAD_FILE_META,
  BAD_RECORD,
  FILE_EOF,
  CHECK_SUM_ERROR,
  NOEXCEPT_SIZE,
  BAD_FILE_PATH,
  BAD_CURRENT_FILE,
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
    case FOOTER_BLOCK_ERROR:
      return "footer block error";
    case UN_SUPPORTED_FORMAT:
      return "unsupported format";
    case DB_CLOSED:
      return "db closed";
    case STAT_FILE_ERROR:
      return "stat file error";
    case MMAP_ERROR:
      return "mmap error";
    case OUT_OF_RANGE:
      return "out of range";
    case BAD_LEVEL:
      return "bad level";
    case BAD_REVISION:
      return "bad revision";
    case BAD_FILE_META:
      return "bad file meta";
    case CHECK_SUM_ERROR:
      return "check sum error";
    case FILE_EOF:
      return "file eof";
    case NOEXCEPT_SIZE:
      return "noexcept size";
    case BAD_FILE_PATH:
      return "bad file path";
    case BAD_CURRENT_FILE:
      return "bad current file";
    default:
      return "unknown error";
  }
}

}  // namespace adl

#endif  // ADL_LSM_TREE_RC_H__