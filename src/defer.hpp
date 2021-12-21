#ifndef ADL_LSM_TREE_DEFER_H__
#define ADL_LSM_TREE_DEFER_H__
#include <functional>
namespace adl {

class defer {
 public:
  defer(std::function<void()> &&fn) : fn_(std::move(fn)) {}
  ~defer() { fn_(); }
  std::function<void()> fn_;
};

}  // namespace adl
#endif  // ADL_LSM_TREE_DEFER_H__