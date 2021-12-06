#include "encode.hpp"
#include "string.h"

namespace adl {
void Decode32(const char *src, int *dest) { *dest = *(int *)(src); }
void Encode32(int src, char *dest) { memcpy(dest, &src, sizeof(int)); }

}  // namespace adl