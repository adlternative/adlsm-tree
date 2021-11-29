#ifndef ADL_LSM_TREE_FOOTER_BLOCK_H__
#define ADL_LSM_TREE_FOOTER_BLOCK_H__

#include <string>
#include "rc.hpp"

namespace adl {
using namespace std;

class FooterBlock {
 public:
  RC Add(const string &meta_block_handle, const string &index_block_handle);

  RC Final(string &result);

 private:
  string_view meta_block_handle_;
  string_view index_block_handle_;
};

}  // namespace adl

#endif  // ADL_LSM_TREE_FOOTER_BLOCK_H__