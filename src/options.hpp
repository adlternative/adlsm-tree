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
#include <cassert>

namespace adl {

struct DBOptions {
  /* DB OPERATION */
  bool create_if_not_exists = false;

  /* SSTABLE */
  /* 布隆过滤器 */
  int bits_per_key = 10;

  /* MEMTABLE */
  /* 内存表最大大小，超过了则应该冻结内存表 */
  size_t mem_table_max_size = 1UL << 22; /* 4MB */

  size_t block_cache_size = 1UL << 11; /* 2048 个 BLOCK */

  /* BACKGROUND */
  int background_workers_number = 1;

  /* LOG */
  const char *log_pattern = "[%Y-%m-%d %H:%M:%S.%e] [%l] [%n] %v";
  spdlog::level::level_enum log_level = spdlog::level::err;
  const char *logger_name = "monitor_logger";
  const char *log_file_name = "monitor.log";

  /* sync */
  bool sync = false;

  /* major compaction */
  int level_files_limit = 4;
};

}  // namespace adl

#endif  // ADL_LSM_TREE_OPTIONS_H__