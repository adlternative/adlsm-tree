#include "hash_util.hpp"
#include <charconv>
#include <iomanip>
#include <string>

namespace adl {

string sha256_digit_to_hex(const unsigned char hash[]) {
  stringstream ss;
  for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
    ss << hex << setw(2) << setfill('0') << (int)hash[i];
  }
  return ss.str();
}

void hex_to_sha256_digit(string_view hex, unsigned char *hash) {
  for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
    hash[i] = (unsigned char)stoi(string(hex.substr(2 * i, 2)), nullptr, 16);
  }
}

}  // namespace adl