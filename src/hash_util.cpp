#include "hash_util.hpp"
#include <charconv>
#include <iomanip>

namespace adl {

std::optional<int> hex_string_to_int(const std::string_view &input) {
  unsigned int out;
  const std::from_chars_result result =
      std::from_chars(input.data(), input.data() + input.size(), out, 16);
  if (result.ec == std::errc::invalid_argument ||
      result.ec == std::errc::result_out_of_range) {
    return std::nullopt;
  }
  return out;
}

string sha256_digit_to_hex(const unsigned char hash[]) {
  stringstream ss;
  ss.setf(std::ios::hex, std::ios::basefield);

  for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
    ss.width(2);
    ss.fill('0');
    ss << static_cast<unsigned int>(hash[i]);
  }
  ss.setf(std::ios::dec, std::ios::basefield);
  return ss.str();
}

void hex_to_sha256_digit(string_view hex, unsigned char *hash) {
  for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
    hash[i] = (unsigned char)hex_string_to_int(hex.substr(i * 2, 2)).value();
  }
}

}  // namespace adl