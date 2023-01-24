#pragma once

#include <array>
#include <cstdint>
#include <cstring>
#include <iostream>

#include "hex_util.h"

namespace carmen {

template <typename T>
concept Trivial = std::is_trivially_copyable_v<T>;

constexpr int kHashLength = 32;
constexpr int kAddressLength = 20;
constexpr int kKeyLength = 32;
constexpr int kValueLength = 32;
constexpr int kBalanceLength = 16;
constexpr int kNonceLength = 8;

// Class template for all types based on byte array value.
template <std::size_t N>
class ByteValue {
 public:
  ByteValue() = default;

  // Class constructor populating data with given list of values.
  ByteValue(std::initializer_list<std::uint8_t> il) {
    std::copy(il.begin(), il.begin() + std::min(il.size(), data_.size()),
              std::begin(data_));
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

  // Sets the bytes of this value to the provided data.
  void SetBytes(std::span<const std::byte, N> data) {
    std::memcpy(&data_[0], data.data(), data.size());
  }

  // Sets the bytes of this value to the data provided. Elements in the input
  // exceeding the size of this ByteValue are ignored. If the input is too
  // short, the rest of the bytes are filled with zero.
  void SetBytes(std::span<const std::byte> data) {
    std::memcpy(&data_[0], data.data(), std::min(data.size(), data_.size()));
    if (data.size() < data_.size()) {
      std::memset(&data_[0] + data.size(), 0, data_.size() - data.size());
    }
  }

  // Overload of << operator to make class printable.
  friend std::ostream& operator<<(std::ostream& out,
                                  const ByteValue<N>& hexContainer) {
    hex_util::WriteTo(
        out, *const_cast<std::array<std::uint8_t, N>*>(&hexContainer.data_));
    return out;
  }

  // Add equality and inequality comparison support for all ByteValues.
  friend bool operator==(const ByteValue<N>& containerA,
                         const ByteValue<N>& containerB) = default;

  // Add comparison support for all ByteValues.
  friend int operator<=>(const ByteValue<N>& containerA,
                         const ByteValue<N>& containerB) {
    // Ideally we would just let this generate using =default, but this fails on
    // MacOS since no <=> operator for arrays can be found. This seems to be a
    // missing feature, since according to the cppreference such an operator
    // should be defined.
    return std::memcmp(containerA.data_.begin(), containerB.data_.begin(), N);
  }

  // Support the usage of ByteValues in hash based absl containers.
  template <typename H>
  friend H AbslHashValue(H h, const ByteValue& v) {
    return H::combine(std::move(h), v.data_);
  }

 private:
  std::array<std::uint8_t, N> data_{};
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

}  // namespace carmen
