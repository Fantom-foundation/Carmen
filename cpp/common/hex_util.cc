#include "hex_util.h"

#include <cstdint>
#include <ostream>
#include <span>

namespace carmen::hex_util {

namespace {
const std::array<char, 16> kHexMap = {'0', '1', '2', '3', '4', '5', '6', '7',
                                      '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
}  // namespace

// Write hex representation of given sequence prefixed with "0x" into ostream.
void WriteTo(std::ostream& out, std::span<const std::uint8_t> data) {
  out << "0x";
  for (const auto& i : data) {
    out << kHexMap[i >> 4];
    out << kHexMap[i & 0xF];
  }
}

}  // namespace carmen::hex_util
