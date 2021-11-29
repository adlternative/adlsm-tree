#ifndef ADL_LSM_TREE_ENCODE_H__
#define ADL_LSM_TREE_ENCODE_H__

namespace adl {

void Decode32(const char *src, int *dest);
void Encode32(int src, char *dest);

}  // namespace adl

#endif  // ADL_LSM_TREE_ENCODE_H__