#include "hex_util.h"

#include <cstdint>
#include <ostream>
#include <span>

namespace carmen::hex_util {

namespace {
const std::array<char, 16> HEX_MAP = {'0', '1', '2', '3', '4', '5', '6', '7',
                                      '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
}  // namespace

// Write hex representation of given sequence prefixed with "0x" into ostream.
void WriteTo(std::ostream& out, std::span<std::uint8_t> data) {
  out << "0x";
  for (const auto& i : data) {
    out << HEX_MAP[i >> 4];
    out << HEX_MAP[i & 0xF];
  }
}

}  // namespace carmen::hex_util
