#ifndef ADL_LSM_TREE_MURMUR3_HASH_H__
#define ADL_LSM_TREE_MURMUR3_HASH_H__
#include <stddef.h>
#include <stdint.h>

namespace adl {

/*  https://en.wikipedia.org/wiki/MurmurHash#Algorithm */
uint32_t murmur3_hash(uint32_t seed, const char *data, size_t len);

}  // namespace adl

#endif  // ADL_LSM_TREE_MURMUR3_HASH_H__