#pragma once

#include <sstream>
#include <string>

namespace carmen {

template <typename T>
std::string Print(const T& value) {
  std::stringstream out;
  out << value;
  return out.str();
}

}  // namespace carmen
