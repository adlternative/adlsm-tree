#include "monitor_logger.hpp"
#include <iostream>

namespace adl {
MonitorLogger &MonitorLogger::Logger() {
  static MonitorLogger instance;
  return instance;
}

/* 目前的语意 是 第一次调用成功，后面都失败 */
void MonitorLogger::SetDbNameAndOptions(const string &db_name,
                                        const DBOptions *options) {
  try {
    logger = spdlog::basic_logger_mt<spdlog::async_factory>(
        options->logger_name, db_name + '/' + options->log_file_name);
    // std::cout << db_name + '/' + options->log_file_name << std::endl;
    logger->set_pattern(options->log_pattern);
    logger->set_level(options->log_level);
    spdlog::flush_every(std::chrono::seconds(1));
    spdlog::flush_on(spdlog::level::info);
    spdlog::flush_on(spdlog::level::debug);
    // then spdloge::get("logger_name")
    // spdlog::register_logger(logger);
  } catch (std::exception &ex) {
    std::cerr << ex.what() << std::endl;
    exit(-1);
  }
}

MonitorLogger::MonitorLogger() : logger(spdlog::stdout_color_mt("console")) {}
}  // namespace adl