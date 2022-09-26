#pragma once

#include <ostream>
#include <cstdint>
#include <span>

namespace carmen::hex_util {

// Write hex representation of given sequence prefixed with "0x" into ostream.
void WriteTo(std::ostream& out, std::span<std::uint8_t> data);

} // namespace carmen::hex_util
