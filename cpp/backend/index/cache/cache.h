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

#include <filesystem>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/common/cache/lru_cache.h"
#include "backend/index/index.h"
#include "backend/structure.h"
#include "common/status_util.h"

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

  // A factory function creating an instance of this index type.
  template <typename... Args>
  static absl::StatusOr<Cached> Open(Context& context,
                                     const std::filesystem::path& path,
                                     Args&&... args) {
    ASSIGN_OR_RETURN(auto index,
                     I::Open(context, path, std::forward<Args>(args)...));
    return Cached(std::move(index));
  }

  // Creates a new cached index wrapping the given index and using the given
  // maximum cache size.
  Cached(I index = {}, std::size_t max_entries = kDefaultSize)
      : index_(std::move(index)), cache_(max_entries) {}

  // Retrieves the ordinal number for the given key. If the key
  // is known, it will return a previously established value
  // for the key. If the key has not been encountered before,
  // a new ordinal value is assigned to the key and stored
  // internally such that future lookups will return the same
  // value.
  absl::StatusOr<std::pair<value_type, bool>> GetOrAdd(const key_type& key) {
    const absl::StatusOr<value_type>* value = cache_.Get(key);
    if (value != nullptr && !absl::IsNotFound(value->status())) {
      if (value->ok()) {
        return std::pair{**value, false};
      }
      return value->status();
    }
    ASSIGN_OR_RETURN(auto res, index_.GetOrAdd(key));
    cache_.Set(key, res.first);
    // If this is a new key, the cached hash needs to be invalidated.
    if (res.second) {
      hash_ = std::nullopt;
    }
    return res;
  }

  // Retrieves the ordinal number for the given key if previously registered.
  // Otherwise, returns a not found status.
  absl::StatusOr<value_type> Get(const key_type& key) const {
    const absl::StatusOr<value_type>* value = cache_.Get(key);
    if (value != nullptr) {
      return *value;
    }
    auto res = index_.Get(key);
    if (absl::IsNotFound(res.status())) {
      cache_.Set(key, res.status());
      return res.status();
    }
    RETURN_IF_ERROR(res);
    cache_.Set(key, *res);
    return *res;
  }

  // Computes a hash over the full content of this index.
  absl::StatusOr<Hash> GetHash() {
    if (hash_.has_value()) {
      return *hash_;
    }
    // Cache the hash of the wrapped index.
    ASSIGN_OR_RETURN(hash_, index_.GetHash());
    return *hash_;
  }

  // Flush unsaved index keys to disk.
  absl::Status Flush() { return index_.Flush(); }

  // Close this index and release resources.
  absl::Status Close() { return index_.Close(); }

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

  // The maintained in-memory value cache. Only Ok and NotFound values are
  // cached. Ok status defines a valid value, NotFound status defines a
  // previously unknown key.
  mutable LeastRecentlyUsedCache<key_type, absl::StatusOr<value_type>> cache_;

  // Set if the hash is up-to-date.
  std::optional<Hash> hash_;
};

}  // namespace carmen::backend::index
