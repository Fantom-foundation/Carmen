#pragma once

#include <concepts>
#include <optional>
#include <utility>

#include "backend/structure.h"
#include "common/memory_usage.h"
#include "common/type.h"

namespace carmen::backend::index {

// A snapshot of the state of an index providing access to the contained data
// frozen at it creation time. This definies an interface for index
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
  // associated value and a boolean set to true if the provided key was new,
  // false otherwise.
  {
    a.GetOrAdd(std::declval<typename I::key_type>())
    } -> std::same_as<std::pair<typename I::value_type, bool>>;
  // Retrieves the key from the index if present, nullopt otherwise.
  {
    b.Get(std::declval<typename I::key_type>())
    } -> std::same_as<std::optional<typename I::value_type>>;
}
// Indexes must satisfy the requirements for backend data structures.
&&Structure<I>;

}  // namespace carmen::backend::index
