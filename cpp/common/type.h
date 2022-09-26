#pragma once

#include <array>
#include <cstdint>
#include <iostream>

#include "hex_util.h"

namespace carmen {

const int HASH_LENGTH = 32;
const int ADDRESS_LENGTH = 20;

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
    hex_util::WriteTo(out, *const_cast<std::array<std::uint8_t, N>*>
                      (&hexContainer._data));
    return out;
  }

  // Ensure default three-way comparison.
  friend auto operator<=>(const ByteValue<N>& containerA,
                          const ByteValue<N>& containerB) = default;

private:
  std::array<std::uint8_t, N> _data{};
};

// Hash represents the 32 byte hash of data
class Hash : public ByteValue<HASH_LENGTH> {
public:
  Hash(): ByteValue<HASH_LENGTH>() {};
  Hash(std::initializer_list<std::uint8_t> il): ByteValue<HASH_LENGTH>(il) {}
};

// Address represents the 20 byte address of an account.
class Address : public ByteValue<ADDRESS_LENGTH> {
public:
    Address(): ByteValue<ADDRESS_LENGTH>() {};
    Address(std::initializer_list<std::uint8_t> il)
        : ByteValue<ADDRESS_LENGTH>(il) {}
};
}
