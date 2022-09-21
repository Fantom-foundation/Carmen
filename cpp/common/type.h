#pragma once

#include <array>
#include <cstdint>
#include <iostream>

namespace carmen {

class Hash {

public:
    friend std::ostream& operator<<(std::ostream& out, const Hash& hash);
    friend auto operator<=>(const Hash& hashA, const Hash& hashB) = default;
    Hash(std::array<std::uint8_t, 32> data): _data(data) {}
private:
  std::array<std::uint8_t, 32> _data;
};


} // namespace carmen