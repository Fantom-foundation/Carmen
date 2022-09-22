#pragma once

#include <array>
#include <string>

namespace carmen::hex_util {

namespace {
const std::array<char, 16> MAP = {'0', '1', '2', '3', '4', '5', '6', '7', '8',
                                  '9', 'a', 'b', 'c', 'd', 'e', 'f'};
} // namespace

// Returns hex representation of given array prefixed with "0x"
template <std::size_t N>
std::string ToString(const std::array<std::uint8_t, N>& data) {
  std::string str = "0x";

  for (auto i: data) {
    str += MAP[i >> 4];
    str += MAP[i & 0xF];
  }

  return str;
}
} // namespace carmen::hex_util
