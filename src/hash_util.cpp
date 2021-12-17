#include "hash_util.hpp"
#include <iomanip>

namespace adl {

string sha256_digit_to_hex(const unsigned char hash[]) {
  stringstream ss;
  for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
    ss << hex << setw(2) << setfill('0') << (int)hash[i];
  }
  return ss.str();
}
}  // namespace adl