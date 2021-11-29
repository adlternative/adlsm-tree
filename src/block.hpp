#ifndef ADL_LSM_TREE_BLOCK_H__
#define ADL_LSM_TREE_BLOCK_H__

#include <string>
#include <vector>
#include "rc.hpp"

namespace adl {
using namespace std;

class Block {
 public:
  Block();
  RC Add(const string &key, const string &value);
  RC Finish(string &ret);

 private:
  std::vector<int> restarts_;  //  重启点
  string last_key_;
  int entries_len_;
  string buffer_;
};

}  // namespace adl

#endif  // ADL_LSM_TREE_BLOCK_H__