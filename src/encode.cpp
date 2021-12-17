#include "encode.hpp"
#include <string.h>
#include <string>

namespace adl {
void Decode32(const char *src, int *dest) { *dest = *(int *)(src); }
void Encode32(int src, char *dest) { memcpy(dest, &src, sizeof(int)); }
void EncodeWithPreLen(string_view data, string &dest) {
  int len = data.size();
  dest.resize(len + sizeof(int));
  dest.append(reinterpret_cast<const char *>(&len), sizeof(int));
  dest.append(data.data(), len);
}

}  // namespace adl