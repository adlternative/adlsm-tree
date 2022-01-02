#ifndef ADL_LSM_TREE_CACHE_H__
#define ADL_LSM_TREE_CACHE_H__

#include <algorithm>
#include <cstdint>
#include <list>
#include <mutex>
#include <thread>
#include <unordered_map>

namespace adl {
/*
 * a noop lockable concept that can be used in place of std::mutex
 */
class NullLock {
 public:
  void lock() {}
  void unlock() {}
  bool try_lock() { return true; }
};

/* Lock = std::mutex */
template <class Key, class Value, class Lock = NullLock>
class LRUCache {
 public:
  explicit LRUCache(size_t maxSize = 64) : maxSize_(maxSize) {}
  virtual ~LRUCache() = default;
  size_t Size() const {
    std::lock_guard<Lock> g(lock_);
    return cache_.size();
  }
  bool Empty() const {
    std::lock_guard<Lock> g(lock_);
    return cache_.empty();
  }
  void Clear() {
    std::lock_guard<Lock> g(lock_);
    cache_.clear();
    keys_.clear();
  }
  void Put(const Key &k, const Value &v) {
    std::lock_guard<Lock> g(lock_);
    const auto iter = cache_.find(k);
    /* 如果已经有了该 key 则设置 v 并将迭代器移动到链表头  */
    if (iter != cache_.end()) {
      iter->second->second = v;
      keys_.splice(keys_.begin(), keys_, iter->second);
      return;
    }
    /* 否则在链表头构造新节点 */
    keys_.emplace_front(k, v);
    cache_[k] = keys_.begin();
    /* 缓存已经满了 */
    if (cache_.size() >= maxSize_) Prune();
  }

  bool Get(const Key &k, Value &v) {
    std::lock_guard<Lock> g(lock_);
    const auto iter = cache_.find(k);
    if (iter == cache_.end()) return false;
    /* 将找到的 k 的迭代器移动到 keys 的 begin */
    keys_.splice(keys_.begin(), keys_, iter->second);
    v = iter->second->second;
    return true;
  }

  bool Remove(const Key &k) {
    std::lock_guard<Lock> g(lock_);
    auto iter = cache_.find(k);
    if (iter == cache_.end()) return false;
    keys_.erase(iter->second);
    cache_.erase(iter);
    return true;
  }

 protected:
  /* 淘汰 链表最后的 kv */
  void Prune() {
    while (cache_.size() > maxSize_) {
      cache_.erase(keys_.back().first);
      keys_.pop_back();
    }
  }

 private:
  LRUCache(const LRUCache &) = delete;
  LRUCache &operator=(const LRUCache &) = delete;

  mutable Lock lock_;
  std::list<std::pair<Key, Value>> keys_;
  std::unordered_map<Key, typename decltype(keys_)::iterator> cache_;
  size_t maxSize_;
};

}  // namespace adl

#endif  // ADL_LSM_TREE_CACHE_H__