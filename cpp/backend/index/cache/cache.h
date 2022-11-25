#pragma once

#include "absl/container/flat_hash_map.h"
#include "backend/common/cache/lru_cache.h"
#include "backend/index/index.h"

namespace carmen::backend::index {

// A CachedIndex wraps another index implementation and maintains an interal
// cache of key/value pairs for faster access.
template <Index I>
class Cached {
 public:
  // The type of the indexed key.
  using key_type = typename I::key_type;
  // The value type of ordinal values mapped to keys.
  using value_type = typename I::value_type;

  // Creates a new cached index wrapping the given index and using the given
  // maximum cache size.
  Cached(I index = {}, std::size_t max_entries = kDefaultSize)
      : index_(std::move(index)), cache_(max_entries) {}

  // Retrieves the ordinal number for the given key. If the key
  // is known, it it will return a previously established value
  // for the key. If the key has not been encountered before,
  // a new ordinal value is assigned to the key and stored
  // internally such that future lookups will return the same
  // value.
  std::pair<value_type, bool> GetOrAdd(const key_type& key) {
    const std::optional<value_type>* value = cache_.Get(key);
    if (value != nullptr && *value != std::nullopt) {
      return {**value, false};
    }
    auto res = index_.GetOrAdd(key);
    cache_.Set(key, res.first);
    // If this is a new key, the cached hash needs to be invalidated.
    if (res.second) {
      hash_ = std::nullopt;
    }
    return res;
  }

  // Retrieves the ordinal number for the given key if previously registered.
  // Otherwise std::nullopt is returned.
  std::optional<value_type> Get(const key_type& key) const {
    const std::optional<value_type>* value = cache_.Get(key);
    if (value != nullptr) {
      return *value;
    }
    auto res = index_.Get(key);
    cache_.Set(key, res);
    return res;
  }

  // Computes a hash over the full content of this index.
  Hash GetHash() {
    if (hash_.has_value()) {
      return *hash_;
    }
    // Cache the hash of the wrapped index.
    auto hash = index_.GetHash();
    hash_ = hash;
    return hash;
  }

  // Flush unsafed index keys to disk.
  void Flush() { index_.Flush(); }

  // Close this index and release resources.
  void Close() { index_.Close(); }

  // Summarizes the memory usage of this instance.
  MemoryFootprint GetMemoryFootprint() const {
    MemoryFootprint res(*this);
    res.Add("index", index_.GetMemoryFootprint());
    res.Add("cache", cache_.GetMemoryFootprint());
    return res;
  }

 private:
  constexpr static std::size_t kDefaultSize = 1 << 20;  // ~1 million

  // The underlying index to be wrapped.
  I index_;

  // The maintained in-memory value cache.
  mutable LeastRecentlyUsedCache<key_type, std::optional<value_type>> cache_;

  // Set if the hash is up-to-date.
  std::optional<Hash> hash_;
};

}  // namespace carmen::backend::index
