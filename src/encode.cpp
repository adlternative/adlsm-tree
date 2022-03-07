#include "encode.hpp"
#include <string.h>
#include <string>

namespace adl {
void Decode32(const char *src, int *dest) { *dest = *(int *)(src); }
void Encode32(int src, char *dest) { memcpy(dest, &src, sizeof(int)); }
void EncodeWithPreLen(string &dest, string_view data) {
  int len = data.size();
  dest.append(reinterpret_cast<const char *>(&len), sizeof(int));
  dest.append(data.data(), len);
}

int DecodeWithPreLen(string &dest, string_view data) {
  int len;
  const char *buf = data.data();
  Decode32(buf, &len);
  dest.assign(buf + sizeof(int), len);
  return len + sizeof(int);
}

void Decode64(const char *src, int64_t *dest) { *dest = *(int64_t *)(src); }

}  // namespace adl