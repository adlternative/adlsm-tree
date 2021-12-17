#ifndef ADL_LSM_TREE_ENCODE_H__
#define ADL_LSM_TREE_ENCODE_H__
#include <string_view>

namespace adl {
using namespace std;

void Decode32(const char *src, int *dest);
void Encode32(int src, char *dest);
void EncodeWithPreLen(string_view data, string &dest);

}  // namespace adl

#endif  // ADL_LSM_TREE_ENCODE_H__