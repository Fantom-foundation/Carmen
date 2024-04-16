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

#include <compare>
#include <cstdint>
#include <ostream>

namespace carmen {

// An enum of schema featurs that implementations may offer.
enum class StateFeature : std::uint8_t {
  // An implementation offering this feature is indexing account addresses
  // internally. This additional address index forms a part of the state that
  // needs to be hashed and synced. Thus, implementations with this
  // feature are not compatible with implementations without this feature.
  kAddressId = 1 << 0,

  // An implementation offering this feature is indexing storage slot keys
  // internally. This additional address index forms a part of the state that
  // needs to be hashed and synced. Thus, implementations with this
  // feature are not compatible with implementations without this feature.
  kKeyId = 1 << 1,

  // An implementation using account reincarnation is tracking the number of
  // times an account has been recreated, in addition to its basic properties.
  // Reincarnation numbers provide a cheaper way to clear the storage of deleted
  // accounts. However, the additional information to be tracked causes
  // different state hashes to be produced. Thus, implementations with this
  // feature are not compatible with implementations without this feature.
  kAccountReincarnation = 1 << 2,
};

// A state Schema is a description of the internal organization of Carmen State
// implementation. Each implementations encorperates a set of features, leading
// to incompatbilities tracked through schemas.
class Schema {
 public:
  // A constructor supporting the combination of a list of features.
  template <typename... Features>
  constexpr Schema(Features... features)
      : features_((std::uint8_t(0) | ... | std::uint8_t(features))) {}

  // Supports testing whether a given feature is part of this schema.
  constexpr bool HashFeature(StateFeature feature) const {
    return features_ & static_cast<std::uint8_t>(feature);
  }

  // A list of operations supporting the composition of features to form
  // schemas. Schemas and individual features can be combined using & (and) to
  // form the union of features.

  constexpr friend Schema operator&(Schema s, StateFeature f) {
    return s & Schema(f);
  }

  constexpr friend Schema operator&(StateFeature f, Schema s) {
    return s & Schema(f);
  }

  constexpr friend Schema operator&(Schema a, Schema b) {
    Schema res;
    res.features_ = a.features_ | b.features_;
    return res;
  }

  auto operator<=>(const Schema&) const = default;

  friend std::ostream& operator<<(std::ostream&, const Schema&);

 private:
  std::uint8_t features_;
};

constexpr Schema operator&(StateFeature a, StateFeature b) {
  return Schema(a, b);
}

}  // namespace carmen
