#ifndef ADL_LSM_TREE_BACK_GROUND_WORKER_H__
#define ADL_LSM_TREE_BACK_GROUND_WORKER_H__
#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>

namespace adl {
using namespace std;

class Worker {
 public:
  static Worker *NewBackgroundWorker();

  Worker();
  void Stop();

  void Add(std::function<void()> &&function) noexcept;

  void Run();
  void operator()();

 private:
  queue<function<void()>> work_queue_;

  mutex work_queue_mutex_;
  condition_variable work_queue_cond_;
  bool closed_;
};

}  // namespace adl

#endif  // ADL_LSM_TREE_BACK_GROUND_WORKER_H__