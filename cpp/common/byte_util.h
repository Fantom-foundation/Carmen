#pragma once

#include <span>

#include "absl/status/statusor.h"

namespace carmen {

// Return a span of bytes representing the given value. Given value has to be
// trivial.
template <typename T>
std::span<const std::byte> AsBytes(const T& value) {
  return std::as_bytes(std::span<const T>(&value, 1));
}

// Return a span of chars representing the given value. Given value has to be
// trivial.
template <typename T>
std::span<const char> AsChars(const T& value) {
  auto bytes = AsBytes(value);
  return {reinterpret_cast<const char*>(bytes.data()), sizeof(T)};
}

// Return a value from a span of bytes. Target value has to be trivial.
template <typename T>
absl::StatusOr<T> FromChars(std::span<const char> data) {
  if (data.size() != sizeof(T)) return absl::InternalError("Invalid data size");
  return *reinterpret_cast<const T*>(data.data());
}

// Return a value from a span of bytes. Target value has to be trivial.
template <typename T>
absl::StatusOr<T> FromBytes(std::span<const std::byte> data) {
  if (data.size() != sizeof(T)) return absl::InternalError("Invalid data size");
  return *reinterpret_cast<const T*>(data.data());
}
}  // namespace carmen
