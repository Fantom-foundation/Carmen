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

#include <array>
#include <concepts>
#include <cstdint>
#include <utility>

#include "absl/numeric/int128.h"

namespace carmen::backend::index {

namespace internal {

// Implements a stable hash that will not change over time. This can be used
// for computing hashes of values required for indexing data objects in
// persistent storage.
//
// The implementation is derived from the absl::Hash infrastructure.
class StableHashState {
 public:
  // --------------------------------------------------------------------------
  //                        hashing of individual types
  // --------------------------------------------------------------------------

  // Support hashing for integral types.
  template <std::integral I>
  static std::size_t hash(I value) {
    return Mix(0, static_cast<uint64_t>(value));
  }

  // Support hashing for arrays of types.
  template <typename A, typename B>
  static std::size_t hash(const std::pair<A, B>& value) {
    return combine(StableHashState(), value.first, value.second).state_;
  }

  // Support hashing for arrays of types.
  template <typename T, std::size_t N>
  static std::size_t hash(const std::array<T, N>& value) {
    std::uint64_t res = 0;
    for (const T& cur : value) {
      res = Mix(res, hash(cur));
    }
    return res;
  }

  // The fall-back support for types implementing the Absl hashing interface.
  template <typename T>
  requires(!std::is_integral_v<T>) static std::size_t hash(const T& value) {
    return AbslHashValue(internal::StableHashState(), value).state_;
  }

  // --------------------------------------------------------------------------
  //                        combination of hashes
  // --------------------------------------------------------------------------

  static StableHashState combine(StableHashState state) { return state; }

  template <typename T>
  static StableHashState combine(StableHashState state, const T& value) {
    state.state_ = Mix(state.state_, hash(value));
    return state;
  }

  template <typename V, typename... Vs>
  static StableHashState combine(StableHashState state, const V& value,
                                 const Vs&... values) {
    return combine(combine(state, value), values...);
  }

 private:
  // A magic constant taken from asl::Hash used to produce value spread when
  // hashing integers.
  static constexpr std::uint64_t kMul = sizeof(std::size_t) == 4
                                            ? std::uint64_t{0xcc9e2d51}
                                            : std::uint64_t{0x9ddfea08eb382d69};

  static std::uint64_t Mix(std::uint64_t a, std::uint64_t b) {
    // Taken from absl's MixingHashState implementation.
    // Though the 128-bit product on AArch64 needs two instructions, it is
    // still a good balance between speed and hash quality.
    using MultType =
        std::conditional_t<sizeof(std::size_t) == 4, uint64_t, absl::uint128>;
    // We do the addition in 64-bit space to make sure the 128-bit
    // multiplication is fast. If we were to do it as MultType the compiler has
    // to assume that the high word is non-zero and needs to perform 2
    // multiplications instead of one.
    MultType m = a + b;
    m *= kMul;
    return static_cast<uint64_t>(m ^ (m >> (sizeof(m) * 8 / 2)));
  }

  // The hash tracked by this class while being used through the Absl hash
  // infrastructure.
  std::uint64_t state_ = 0;
};

}  // namespace internal

// A utility class implementing the hashing of type T. The provided hash is
// stable, thus it will not change over time and can be used for hash based
// persistent storage.
template <typename T>
struct StableHash {
  std::size_t operator()(const T& value) const {
    return internal::StableHashState::hash(value);
  }
};

}  // namespace carmen::backend::index
