#pragma once

#include <array>
#include <cstdint>
#include <iostream>

#include "hex_util.h"

namespace carmen {

// Class template for all types based on byte array value.
template <std::size_t N> class ByteValue {
public:
  ByteValue() = default;

  // Class constructor populating data with given list of values.
  ByteValue(std::initializer_list<std::uint8_t> il) {
    std::copy(il.begin(), il.end(), std::begin(_data));
  }

  // Overload of << operator to make class printable.
  friend std::ostream &operator<<(std::ostream& out,
                                  const ByteValue<N>& hexContainer) {
    hex_util::WriteTo(*const_cast<std::array<std::uint8_t, N>*>
                      (&hexContainer._data),out);
    return out;
  }

  // Ensure default three-way comparison.
  friend auto operator<=>(const ByteValue<N>& containerA,
                          const ByteValue<N>& containerB) = default;

private:
  std::array<std::uint8_t, N> _data{};
};

class Hash : public ByteValue<32> {
public:
  Hash(): ByteValue<32>() {};
  Hash(std::initializer_list<std::uint8_t> il): ByteValue<32>(il) {}
};
}
