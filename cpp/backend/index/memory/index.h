#pragma once

#include <concepts>
#include <optional>
#include <queue>

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
template <Trivial K, std::integral I>
class InMemoryIndex {
 public:
  // The type of the indexed key.
  using key_type = K;
  // The value type of ordinal values mapped to keys.
  using value_type = I;

  // Retrieves the ordinal number for the given key. If the key
  // is known, it it will return a previously established value
  // for the key. If the key has not been encountered before,
  // a new ordinal value is assigned to the key and stored
  // internally such that future lookups will return the same
  // value.
  std::pair<I, bool> GetOrAdd(const K& key) {
    auto [pos, is_new] = data_.insert({key, I{}});
    if (is_new) {
      pos->second = data_.size() - 1;
      unhashed_keys_.push(key);
    }
    return {pos->second, is_new};
  }

  // Retrieves the ordinal number for the given key if previously registered.
  // Otherwise std::nullopt is returned.
  std::optional<I> Get(const K& key) const {
    auto pos = data_.find(key);
    if (pos == data_.end()) {
      return std::nullopt;
    }
    return pos->second;
  }

  // Tests whether the given key is indexed by this container.
  bool Contains(const K& key) const { return data_.contains(key); }

  // Computes a hash over the full content of this index.
  Hash GetHash() const {
    while (!unhashed_keys_.empty()) {
      hash_ = carmen::GetHash(hasher_, hash_, unhashed_keys_.front());
      unhashed_keys_.pop();
    }
    return hash_;
  }

 private:
  absl::flat_hash_map<K, I> data_;
  mutable std::queue<K> unhashed_keys_;
  mutable Sha256Hasher hasher_;
  mutable Hash hash_;
};

}  // namespace carmen::backend::index
