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

#include <algorithm>
#include <array>
#include <compare>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <iostream>
#include <span>
#include <vector>

#include "hex_util.h"

namespace carmen {

template <typename T>
concept Trivial = std::is_trivially_default_constructible_v<T> &&
    std::is_trivially_copyable_v<T> && std::is_trivially_destructible_v<T>;

constexpr int kHashLength = 32;
constexpr int kAddressLength = 20;
constexpr int kKeyLength = 32;
constexpr int kValueLength = 32;
constexpr int kBalanceLength = 32;
constexpr int kNonceLength = 8;

// Class template for all types based on byte array value. Byte values are
// trivial objects containing a fixed length sequence of bytes. All byte
// sequences are valid.
//
// When creating a byte value, there are multiple options:
//
//   // Creates a byte value with uninitialized (random) data:
//   ByteValue<2> value;
//
//   // Create and initialize a value with some values:
//   ByteValue<2> value{1,2};
//
//   // Create a value with a partial list, rest is zero:
//   ByteValue<2> value{1};  // == {1,0}
//
// Thus, to initialize a byte value with all zeros, one can write
//   ByteValue value{};
//
// Note the difference: without {} the byte value remains uninitialized, with {}
// the value is initialized to zero. This is the same semantic as it is
// exhibited by the underlying std::array<..>.
template <std::size_t N>
class ByteValue {
 public:
  ByteValue() = default;

  // Class constructor populating data with given list of values.
  ByteValue(std::initializer_list<std::uint8_t> il) {
    SetBytes(std::as_bytes(std::span<const std::uint8_t>(il)));
  }

  // Provide mutable access to the individual bytes.
  std::uint8_t& operator[](std::size_t index) { return data_[index]; }

  // Provide read-only access to the individual bytes.
  std::uint8_t operator[](std::size_t index) const { return data_[index]; }

  // Enables the implicit conversion into fixed-length spans of bytes.
  operator std::span<const std::byte, N>() const {
    return std::as_bytes(std::span(data_));
  }

  // Enables the implicit conversion into spans of bytes with dynamic extend.
  operator std::span<const std::byte>() const {
    return std::as_bytes(std::span(data_));
  }

  // Enables the implicit conversion into fixed-length spans of char.
  operator std::span<const char, N>() const { return std::span(data_); }

  // Enables the implicit conversion into spans of char with dynamic extend.
  operator std::span<const char>() const {
    return std::span(reinterpret_cast<const char*>(data_.data()), N);
  }

  // Sets the bytes of this value to the provided data.
  void SetBytes(std::span<const std::byte, N> data) {
    std::memcpy(&data_[0], data.data(), data.size());
  }

  // Sets the bytes of this value to the data provided. Elements in the input
  // exceeding the size of this ByteValue are ignored. If the input is too
  // short, the rest of the bytes are filled with zero.
  void SetBytes(std::span<const std::byte> data) {
    std::memcpy(&data_[0], data.data(), std::min(data.size(), N));
    if (data.size() < N) {
      std::memset(&data_[0] + data.size(), 0, N - data.size());
    }
  }

  // Same as above, overing a convenience wrapper for cases where data is typed
  // as const char instead of std::byte.
  void SetBytes(std::span<const char> data) { SetBytes(std::as_bytes(data)); }

  // Overload of << operator to make class printable.
  friend std::ostream& operator<<(std::ostream& out,
                                  const ByteValue<N>& hexContainer) {
    hex_util::WriteTo(out, hexContainer.data_);
    return out;
  }

  // Add equality and inequality comparison support for all ByteValues.
  friend bool operator==(const ByteValue<N>& containerA,
                         const ByteValue<N>& containerB) = default;

  // Add comparison support for all ByteValues.
  friend std::strong_ordering operator<=>(const ByteValue<N>& containerA,
                                          const ByteValue<N>& containerB) {
    // Ideally we would just let this generate using =default, but this fails on
    // MacOS since no <=> operator for arrays can be found. This seems to be a
    // missing feature, since according to the cppreference such an operator
    // should be defined.
    int res =
        std::memcmp(containerA.data_.begin(), containerB.data_.begin(), N);
    return res < 0    ? std::strong_ordering::less
           : res == 0 ? std::strong_ordering::equal
                      : std::strong_ordering::greater;
  }

  // Support the usage of ByteValues in hash based absl containers.
  template <typename H>
  friend H AbslHashValue(H h, const ByteValue& v) {
    return H::combine(std::move(h), v.data_);
  }

 private:
  std::array<std::uint8_t, N> data_;
};

// Hash represents the 32 byte hash of data
class Hash : public ByteValue<kHashLength> {
 public:
  using ByteValue::ByteValue;
};

// Address represents the 20 byte address of an account.
class Address : public ByteValue<kAddressLength> {
 public:
  using ByteValue::ByteValue;
};

// Key represents the 32 byte key into index.
class Key : public ByteValue<kKeyLength> {
 public:
  using ByteValue::ByteValue;
};

// Value represents the 32 byte value in store.
class Value : public ByteValue<kValueLength> {
 public:
  using ByteValue::ByteValue;
};

// Balance represents the 16 byte balance of accounts.
class Balance : public ByteValue<kBalanceLength> {
 public:
  using ByteValue::ByteValue;
};

// Balance represents the 8 byte nonce of accounts.
class Nonce : public ByteValue<kNonceLength> {
 public:
  using ByteValue::ByteValue;
};

// Code represents a smart contract code.
class Code {
 public:
  Code(std::vector<std::byte> code = {}) : code_(std::move(code)) {}
  Code(std::span<const char> code) { SetBytes(code); }
  Code(std::span<const std::byte> code) { SetBytes(code); }
  Code(std::initializer_list<std::uint8_t> il) {
    SetBytes(std::as_bytes(std::span(il.begin(), il.size())));
  }

  auto Size() const { return code_.size(); }
  auto Data() const { return code_.data(); }

  void SetBytes(std::span<const std::byte> code) {
    code_.assign(code.begin(), code.end());
  }

  void SetBytes(std::span<const char> data) { SetBytes(std::as_bytes(data)); }

  friend bool operator==(const Code&, const Code&) = default;

  friend std::strong_ordering operator<=>(const Code& lhs, const Code& rhs) {
    // Ideally we would just let this generate using =default, but this fails on
    // MacOS since no <=> operator for vectors can be found. This seems to be a
    // missing feature, since according to the cppreference such an operator
    // should be defined.
    // Note: the missing feature on Mac is likely caused by the lack of an
    // implementation of std::lexicographical_compare_three_way in the standard
    // library;
    auto& l = lhs.code_;
    auto& r = rhs.code_;
    auto n = std::min(l.size(), r.size());
    for (std::size_t i = 0; i < n; i++) {
      if (l[i] < r[i]) return std::strong_ordering::less;
      if (l[i] > r[i]) return std::strong_ordering::greater;
    }
    return l.size() <=> r.size();
  }

  operator std::span<const std::byte>() const {
    return {code_.data(), code_.size()};
  }

  operator std::span<const char>() const {
    return {reinterpret_cast<const char*>(code_.data()), code_.size()};
  }

 private:
  std::vector<std::byte> code_;
};

// A type alias for block numbers.
using BlockId = std::uint32_t;

}  // namespace carmen
