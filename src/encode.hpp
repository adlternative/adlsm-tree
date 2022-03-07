#ifndef ADL_LSM_TREE_ENCODE_H__
#define ADL_LSM_TREE_ENCODE_H__
#include <string_view>

namespace adl {
using namespace std;

void Decode32(const char *src, int *dest);
void Encode32(int src, char *dest);
void EncodeWithPreLen(string &dest, string_view data);
int DecodeWithPreLen(string &dest, string_view data);
void Decode64(const char *src, int64_t *dest) ;

}  // namespace adl

#endif  // ADL_LSM_TREE_ENCODE_H__