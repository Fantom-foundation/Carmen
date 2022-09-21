#pragma once

#include <array>
#include <cstdint>
#include <iostream>

namespace carmen {

class Hash {

public:
private:
  std::array<std::uint8_t, 32> _data;
};

std::ostream& operator<<(std::ostream& out, const Hash& hash);

} // namespace carmen