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

#include <concepts>
#include <optional>
#include <utility>

#include "absl/status/statusor.h"
#include "backend/structure.h"

namespace carmen::backend::index {

// A snapshot of the state of an index providing access to the contained data
// frozen at it creation time. This defines an interface for index
// implementation specific implementations.
//
// The life cycle of a snapshot defines the duration of its availability.
// Snapshots are volatile, thus not persistent over application restarts. A
// snapshot is created by a call to `CreateSnapshot()` on an index instance, and
// destroyed upon destruction. It does not (need) to persist beyond the lifetime
// of the current process.
template <typename K>
class IndexSnapshot {
 public:
  virtual ~IndexSnapshot() {}

  // Obtains the number of keys stored in the snapshot.
  virtual std::size_t GetSize() const = 0;

  // Obtains a sub-range [from, .., to) of the keys stored in this snapshot. The
  // reference to the container underlying the resulting span may only be valid
  // until the next call to this function or the snapshot destruction.
  virtual std::span<const K> GetKeys(std::size_t from,
                                     std::size_t to) const = 0;
};

// Defines the interface expected for an Index I, mapping keys of type K to
// integral values of type V.
template <typename I>
concept Index = requires(I a, const I b) {
  // An index must expose a key type.
  typename I::key_type;
  // An index must expose an integral value type.
  std::integral<typename I::value_type>;
  // Looks up the given key and adds it to the index if not present. Returns the
  // status of operation. On success returns associated value and a boolean
  // set to true if the provided key was new, false otherwise.
  {
    a.GetOrAdd(std::declval<typename I::key_type>())
    } -> std::same_as<absl::StatusOr<std::pair<typename I::value_type, bool>>>;
  // Retrieves the key from the index if present, not found status otherwise.
  {
    b.Get(std::declval<typename I::key_type>())
    } -> std::same_as<absl::StatusOr<typename I::value_type>>;
}
// Indexes must satisfy the requirements for backend data structures.
&&HashableStructure<I>;

}  // namespace carmen::backend::index
