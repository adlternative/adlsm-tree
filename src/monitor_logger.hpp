#ifndef ADL_LSM_TREE_MONITOR_LOGGER_H__
#define ADL_LSM_TREE_MONITOR_LOGGER_H__

#define SPDLOG_FMT_EXTERNAL
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE

#include <spdlog/spdlog.h>
#include <string_view>
#include "options.hpp"
#include "spdlog/async.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"
namespace adl {
using namespace std;

class MonitorLogger {
 public:
  /* 默认 */
  static MonitorLogger &Logger();

  void SetDbNameAndOptions(const string &db_name, const DBOptions *options);

  MonitorLogger();

  std::shared_ptr<spdlog::logger> logger;
  // atomic<bool> default_log_;
};

#define MLog MonitorLogger::Logger()
#define MLogger MonitorLogger::Logger().logger

}  // namespace adl

#endif  // ADL_LSM_TREE_MONITOR_LOGGER_H__