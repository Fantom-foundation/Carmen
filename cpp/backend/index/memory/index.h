#pragma once

#include "absl/container/flat_hash_map.h"
#include "common/hash.h"
#include "common/type.h"

namespace carmen::backend::index {

// The InMemoryIndex implementation implements an append-only
// index for a set of values, mapping each added new element to
// a unique ordinal number.
//
// The type parameter K, the key type, can be any type that can
// be hashed and compared. The type I is the type used for the
// ordinal numbers and must be implicitly constructable from a
// std::size_t.
template <Trivial K, typename I>
class InMemoryIndex {
 public:
  // Retrieves the ordinal number for the given key. If the key
  // is known, it it will return a previously established value
  // for the key. If the key has not been encountered before,
  // a new ordinal value is assigned to the key and stored
  // internally such that future lookups will return the same
  // value.
  I GetOrAdd(const K& key) {
    auto [pos, is_new] = _data.insert({key, I{}});
    if (is_new) {
      pos->second = _data.size() - 1;
      _hash = GetSha256Hash(_hash, key);
    }
    return pos->second;
  }

  // Tests whether the given key is indexed by this container.
  bool Contains(const K& key) const { return _data.contains(key); }

  // Computes a hash over the full content of this store.
  Hash GetHash() const { return _hash; }

 private:
  absl::flat_hash_map<K, I> _data;
  Hash _hash;
};

}  // namespace carmen::backend::index
