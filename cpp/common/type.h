#pragma once

#include <array>
#include <cstdint>
#include <iostream>
#include "hex_util.h"

namespace carmen {

// Class template for all types based on data in hexadecimal format.
template <std::size_t N> class HexContainer {
public:
  // Class constructor populating data with given list of values.
  HexContainer(std::initializer_list<std::uint8_t> il): _data{} {
    std::copy(il.begin(), il.end(), std::begin(_data));
  }

  // Overload of << operator to make class printable.
  friend std::ostream &operator<<(std::ostream& out,
                                  const HexContainer& hexContainer) {
    return out << hex_util::ToString(hexContainer._data);
  }

  // Ensure default three-way comparison.
  friend auto operator<=>(const HexContainer& containerA,
                          const HexContainer& containerB) = default;

private:
  std::array<std::uint8_t, N> _data;
};

class Hash : public HexContainer<32> {
public:
  Hash(std::initializer_list<std::uint8_t> il): HexContainer(il) {}
};
}
