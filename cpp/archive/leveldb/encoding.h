// Copyright (c) 2024 Fantom Foundation
// 
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
// 
// Change Date: 2028-4-16
// 
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

#pragma once

#include <cstdint>
#include <span>

#include "common/type.h"

namespace carmen::archive::leveldb {

// This file provides a few data encoding utilities, in particular for numerical
// and trivial types. It is intended to be used for encoding keys and values in
// LevelDB, such that a natural numeric ordering is achived. To that end,
// integer values need to be encoded using the big-endian format.

// Writes the given value into the provided target span.
void Write(std::uint32_t value, std::span<char, 4> trg);

// Writes the given trivial value (e.g. Balance, Nonce, Value) into the provided
// target span. Trivial values are encoded as is.
template <Trivial T>
requires(!std::is_integral_v<T>) void Write(const T& value,
                                            std::span<char, sizeof(T)> trg) {
  std::memcpy(trg.data(), &value, sizeof(T));
}

// Reads a 32-bit unsigned integer from the given span, decoding it from its
// big-endian encoding.
std::uint32_t ReadUint32(std::span<const char, 4> src);

// Interprets the provided data span as a trivial value.
template <Trivial T>
requires(!std::is_integral_v<T>) T& Read(std::span<char, sizeof(T)> trg) {
  return *reinterpret_cast<T*>(trg.data());
}

// Interprets the provided data span as a constant trivial value.
template <Trivial T>
requires(!std::is_integral_v<T>) const T& Read(
    std::span<const char, sizeof(T)> trg) {
  return *reinterpret_cast<const T*>(trg.data());
}

}  // namespace carmen::archive::leveldb
