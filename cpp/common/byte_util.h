/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3 
 */

#pragma once

#include <span>

#include "absl/status/statusor.h"

namespace carmen {

// Return a span of bytes representing the given value. Given value has to be
// trivially copyable.
template <typename T>
std::span<const std::byte> AsBytes(const T& value) {
  static_assert(std::is_trivially_copyable_v<T>);
  return std::as_bytes(std::span<const T>(&value, 1));
}

// Return a span of chars representing the given value. Given value has to be
// trivially copyable.
template <typename T>
std::span<const char> AsChars(const T& value) {
  auto bytes = AsBytes(value);
  return {reinterpret_cast<const char*>(bytes.data()), sizeof(T)};
}

// Return a span of chars representing the given bytes span.
inline std::span<const char> AsChars(std::span<const std::byte> bytes) {
  return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

// Return a value from a span of bytes. Target value has to be trivially
// copyable.
template <typename T>
absl::StatusOr<T> FromChars(std::span<const char> data) {
  static_assert(std::is_trivially_copyable_v<T>);
  if (data.size() != sizeof(T)) return absl::InternalError("Invalid data size");
  return *reinterpret_cast<const T*>(data.data());
}

// Return a value from a span of bytes. Target value has to be trivially
// copyable.
template <typename T>
absl::StatusOr<T> FromBytes(std::span<const std::byte> data) {
  static_assert(std::is_trivially_copyable_v<T>);
  if (data.size() != sizeof(T)) return absl::InternalError("Invalid data size");
  return *reinterpret_cast<const T*>(data.data());
}
}  // namespace carmen
