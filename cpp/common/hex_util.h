#pragma once

#include <ostream>
#include <cstdint>
#include <span>

namespace carmen::hex_util {

// Returns hex representation of given array prefixed with "0x"
void WriteTo(std::span<std::uint8_t> data, std::ostream& out);

} // namespace carmen::hex_util
