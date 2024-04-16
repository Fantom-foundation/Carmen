/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use of
 * this software will be governed by the GNU Lesser General Public Licence v3.
 */

#pragma once

#include "absl/status/statusor.h"
#include "backend/structure.h"

namespace carmen::backend::multimap {

// Defines the interface expected for a MultiMap M, mapping keys to sets of
// values. It serves as a specialized index structure enabling the fast
// accessing of a set of values associated to a given key.
template <typename M>
concept MultiMap = requires(M a, const M b) {
  // A multi map must expose its key type.
  typename M::key_type;
  // A multi map must expose its value type.
  typename M::value_type;
  // Insert a new Key/Value pair in the multimap. Duplicates are ignored.
  // Returns true if pair was not present before, false if it was, and an error
  // if the operation failed.
  {
    a.Insert(std::declval<typename M::key_type>(),
             std::declval<typename M::value_type>())
    } -> std::same_as<absl::StatusOr<bool>>;
  // Erases a single Key/Value pair from the multimap. Returns true if the
  // element was present and is gone, false if it was not present, and an error
  // if the operation failed.
  {
    a.Erase(std::declval<typename M::key_type>(),
            std::declval<typename M::value_type>())
    } -> std::same_as<absl::StatusOr<bool>>;
  // Erases all Key/Value pairs with the given key from the multimap. Returns
  // an error if the operation failed.
  {
    a.Erase(std::declval<typename M::key_type>())
    } -> std::same_as<absl::Status>;
  // Applies the given function to every value associated to the given key.
  {
    b.ForEach(std::declval<typename M::key_type>(),
              [](const typename M::value_type&) {})
    } -> std::same_as<absl::Status>;
}
// Indexes must satisfy the requirements for backend data structures.
&&Structure<M>;

}  // namespace carmen::backend::multimap
