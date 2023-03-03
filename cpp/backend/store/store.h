#pragma once

#include <concepts>
#include <span>
#include <type_traits>

#include "absl/status/status.h"
#include "backend/common/page_id.h"
#include "backend/snapshot.h"
#include "backend/store/snapshot.h"
#include "backend/structure.h"
#include "common/status_util.h"

namespace carmen::backend::store {

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
    } -> std::same_as<StatusOrRef<const typename S::value_type>>;

  // A store implementation must support syncing to given snapshot.
  {
    a.SyncTo(std::declval<typename S::Snapshot>())
    } -> std::same_as<absl::Status>;
}
// Stores must satisfy the requirements for backend data structures.
&&HashableStructure<S>
    // Stores must be snapshotable.
    &&Snapshotable<S>
        // The offered snapshot type must be a StoreSnapshot.
        &&std::same_as<typename S::Snapshot,
                       StoreSnapshot<typename S::value_type>>;

}  // namespace carmen::backend::store
