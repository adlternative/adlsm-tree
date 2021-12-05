#ifndef ADL_LSM_TREE_OPTIONS_H__
#define ADL_LSM_TREE_OPTIONS_H__

#include <stddef.h>

namespace adl {

struct DBOptions {
  bool create_if_not_exists = false;
  int bits_per_key = 10;
  size_t mem_table_max_size = 4 * 1024 * 1024;  // 4MB
  int background_workers_number = 1;            /* 目前就一个够用了 */
};

}  // namespace adl

#endif  // ADL_LSM_TREE_OPTIONS_H__