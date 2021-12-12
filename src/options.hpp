#ifndef ADL_LSM_TREE_OPTIONS_H__
#define ADL_LSM_TREE_OPTIONS_H__

#ifndef SPDLOG_FMT_EXTERNAL
#define SPDLOG_FMT_EXTERNAL
#endif

#ifndef SPDLOG_ACTIVE_LEVEL
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE
#endif

#include <spdlog/common.h>
#include <stddef.h>

namespace adl {

struct DBOptions {
  /* DB OPERATION */
  bool create_if_not_exists = false;

  /* SSTABLE */
  /* 布隆过滤器 */
  int bits_per_key = 10;

  /* MEMTABLE */
  /* 内存表最大大小，超过了则应该冻结内存表 */
  size_t mem_table_max_size = 4 * 1024 * 1024;

  /* BACKGROUND */
  int background_workers_number = 1;

  /* LOG */
  const char *log_pattern = "[%Y-%m-%d %H:%M:%S.%e] [%l] [%n] %v";
  spdlog::level::level_enum log_level = spdlog::level::trace;
  const char *logger_name = "monitor_logger";
  const char *log_file_name = "monitor.log";
};

}  // namespace adl

#endif  // ADL_LSM_TREE_OPTIONS_H__