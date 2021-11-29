#ifndef ADL_LSM_TREE_OPTIONS_H__
#define ADL_LSM_TREE_OPTIONS_H__

namespace adl {

struct DBOptions {
  bool create_if_not_exists = false;
  int bits_per_key = 10;
};

}  // namespace adl

#endif  // ADL_LSM_TREE_OPTIONS_H__