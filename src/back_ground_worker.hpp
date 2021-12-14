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
  ~Worker();
  Worker(const Worker &) = delete;
  Worker &operator=(const Worker &) = delete;
  void Stop();

  void Add(std::function<void()> &&function) noexcept;

  void Run();
  void Join();
  void operator()();

 private:
  queue<function<void()>> work_queue_;

  mutex work_queue_mutex_;
  condition_variable work_queue_cond_;
  std::thread *thread_;
  bool closed_;
};

}  // namespace adl

#endif  // ADL_LSM_TREE_BACK_GROUND_WORKER_H__