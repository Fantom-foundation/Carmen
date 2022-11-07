#pragma once


#include <span>

namespace carmen {

template <typename T>
std::span<const char> AsRawData(const T& value) {
  auto bytes = AsBytes(value);
  return {reinterpret_cast<const char*>(bytes.data()), sizeof(T)};
}

template <typename T>
std::span<const std::byte> AsBytes(const T& value) {
  return std::as_bytes(std::span<const T>(&value, 1));
}

}  // namespace carmen::hex_util
