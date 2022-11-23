#include "common/memory_usage.h"

#include <cmath>
#include <iomanip>
#include <ostream>
#include <string_view>

namespace carmen {

std::ostream& operator<<(std::ostream& out, const Memory& memory) {
  const std::string_view prefixes = " KMGTPE";
  const auto base = 1024;

  std::int64_t value = memory.bytes();
  if (value < 0) {
    out << "-";
    value = -value;
  }

  if (value == 0) {
    return out << "0 byte";
  }

  int exp =
      std::min<int>(std::log(value) / std::log(base), prefixes.size() - 1);
  if (exp == 0) {
    return out << value << " byte";
  }

  return out << std::fixed << std::setprecision(1)
             << value / std::pow(base, exp) << " " << prefixes[exp] << "iB";
}

}  // namespace carmen
