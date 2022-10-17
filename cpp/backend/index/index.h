#pragma once

#include <concepts>
#include <optional>
#include <utility>

#include "common/type.h"

namespace carmen::backend::index {

// Defines the interface expected for a Index I mapping keys of type K to
// integral values of type V.
template <typename I>
concept Index = requires(I a, const I b) {
  // An index must expose a key type.
  typename I::key_type;
  // An index must expose a value type.
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
  // Computes a hash over the full content of this index. The hash of an empty
  // index is defined to be zero. For every element added, the new hash is to be
  // computed as Sha256(old_hash, key).
  { a.GetHash() } -> std::same_as<Hash>;
};

}  // namespace carmen::backend::index
