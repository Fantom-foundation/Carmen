/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use of
 * this software will be governed by the GNU Lesser General Public License v3.
 */

#pragma once

#include <concepts>
#include <span>
#include <type_traits>

#include "absl/status/status.h"
#include "backend/common/page_id.h"
#include "backend/structure.h"
#include "common/status_util.h"

namespace carmen::backend::store {

// A snapshot of the state of a store providing access to the contained data
// frozen at it creation time. This defines an interface for store
// implementation specific implementations.
//
// The life cycle of a snapshot defines the duration of its availability.
// Snapshots are volatile, thus not persistent over application restarts. A
// snapshot is created by a call to `CreateSnapshot()` on a store instance, and
// destroyed upon destruction. It does not (need) to persist beyond the lifetime
// of the current process.
class StoreSnapshot {
 public:
  virtual ~StoreSnapshot() {}

  // The total number of pages captured by this snapshot.
  virtual std::size_t GetNumPages() const = 0;

  // Gains read access to an individual page in the range [0,..,GetNumPages()).
  // The provided page data is only valid until the next call to this function
  // or destruction of the snapshot.
  virtual std::span<const std::byte> GetPageData(PageId) const = 0;
};

// Defines the interface expected for a Store S providing an unbound array-like
// data structure.
template <typename S>
concept Store = requires(S a, const S b) {
  // A store must expose an integral key type.
  std::integral<typename S::key_type>;
  // A store must expose a trivial value type.
  Trivial<typename S::value_type>;
  // Updates the value associated to the given key.
  {
    a.Set(std::declval<typename S::key_type>(),
          std::declval<typename S::value_type>())
    } -> std::same_as<absl::Status>;
  // Retrieves the value associated to the given key. If no values has
  // been previously set using the Set(..) function above, a zero-initialized
  // value is returned. The returned reference might only be valid until the
  // next operation on the store.
  {
    b.Get(std::declval<typename S::key_type>())
    } -> std::same_as<absl::StatusOr<typename S::value_type>>;
}
// Stores must satisfy the requirements for backend data structures.
&&HashableStructure<S>;

}  // namespace carmen::backend::store
