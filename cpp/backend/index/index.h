#pragma once

#include <concepts>
#include <utility>

#include "absl/status/statusor.h"
#include "backend/index/snapshot.h"
#include "backend/snapshot.h"
#include "backend/structure.h"

namespace carmen::backend::index {

// Defines the interface expected for an Index I, mapping keys of type K to
// integral values of type V.
template <typename I>
concept Index = requires(I a, const I b) {
  // An index must expose a key type.
  typename I::key_type;
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

  // An index implementation must support syncing to given snapshot.
  {
    a.SyncTo(std::declval<typename I::Snapshot>())
    } -> std::same_as<absl::Status>;
}
// An index must expose an integral value type.
&&std::integral<typename I::value_type>
    // Indexes must satisfy the requirements for backend data structures.
    &&HashableStructure<I>
        // Indexes must be snapshotable.
        &&Snapshotable<I>
            // The offered snapshot type must be an IndexSnapshot.
            &&std::same_as<typename I::Snapshot,
                           IndexSnapshot<typename I::key_type>>;

}  // namespace carmen::backend::index
