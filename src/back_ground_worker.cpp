#include "back_ground_worker.hpp"
//#include <fmt/format.h>

namespace adl {

Worker *Worker::NewBackgroundWorker() {
  auto worker = new Worker();
  std::thread t([](Worker *w) { w->Run(); }, worker);
  t.detach();
  return worker;
}

void Worker::Stop() {
  lock_guard<mutex> lock(work_queue_mutex_);
  work_queue_cond_.notify_all();
  closed_ = true;
}
void Worker::Add(std::function<void()> &&function) noexcept {
  // fmt::print("Want Add work to queue\n");
  lock_guard<mutex> lock(work_queue_mutex_);
  // fmt::print("Add work to queue\n");
  work_queue_.push(std::move(function));
  /* 唤醒消费者 */
  work_queue_cond_.notify_one();
}

void Worker::Run() { return this->operator()(); }
void Worker::operator()() {
  while (!closed_) {
    // fmt::print("Worker want get work_queue_mutex_\n");
    unique_lock<mutex> lock(work_queue_mutex_);
    // fmt::print("Worker get work_queue_mutex_\n");
    // fmt::print("Worker wait for tasks...\n");
    while (!closed_ && work_queue_.empty()) work_queue_cond_.wait(lock);
    // fmt::print("Worker wake up...\n");
    if (closed_) break;
    auto task = work_queue_.front();
    work_queue_.pop();
    lock.unlock();
    // fmt::print("Worker unlock work_queue_mutex_\n");
    /* 注意任务得在无锁的情况下跑，不然可能会死锁的 */
    // fmt::print("Worker Before do task\n");
    task();
    // fmt::print("Worker After do task\n");
  }
}

Worker::Worker() : closed_(false) {}
}  // namespace adl