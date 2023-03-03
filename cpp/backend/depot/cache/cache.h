#pragma once

#include <filesystem>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/common/cache/lru_cache.h"
#include "backend/depot/depot.h"
#include "backend/depot/snapshot.h"
#include "backend/structure.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen::backend::depot {

// A CachedDepot wraps another depot implementation and maintains an internal
// cache of key/value pairs for faster access.
template <Depot D>
class Cached {
 public:
  // The type of the depot key.
  using key_type = typename D::key_type;

  // The snapshot type offered by this depot implementation.
  using Snapshot = DepotSnapshot;

  // A factory function creating an instance of this depot type.
  static absl::StatusOr<Cached> Open(Context& context,
                                     const std::filesystem::path& directory) {
    ASSIGN_OR_RETURN(auto depot, D::Open(context, directory));
    return Cached(std::move(depot));
  }

  // Creates a new cached depot wrapping the given depot and using the given
  // maximum cache size.
  Cached(D depot, std::size_t max_entries = kDefaultSize)
      : depot_(std::move(depot)), cache_(max_entries) {}

  // Retrieves the value for the given key. If the key
  // is known, it will return a previously established value
  // for the key. If the key has not been encountered before,
  // it will try to fetch from underlying depot. Otherwise,
  // abseil status not found is returned.
  absl::StatusOr<std::span<const std::byte>> Get(const key_type& key) const {
    auto cached_value = cache_.Get(key);
    if (cached_value != nullptr) {
      return *cached_value;
    }
    auto result = depot_.Get(key);
    if (absl::IsNotFound(result.status())) {
      cache_.Set(key, result.status());
      return result.status();
    }
    if (!result.ok()) {
      return result.status();
    }
    cache_.Set(key, std::vector<std::byte>((*result).begin(), (*result).end()));
    return result;
  }

  // Retrieves the code size for the given key. If the key
  // is known, it will return a previously established value
  // for the key. If the key has not been encountered before,
  // it will try to fetch from underlying depot. Otherwise,
  // abseil status not found is returned.
  absl::StatusOr<std::uint32_t> GetSize(const key_type& key) const {
    ASSIGN_OR_RETURN(auto value, Get(key));
    return value.size();
  }

  absl::Status Set(const key_type& key, std::span<const std::byte> data) {
    RETURN_IF_ERROR(depot_.Set(key, data));
    cache_.Set(key, std::vector<std::byte>(data.begin(), data.end()));
    hash_ = std::nullopt;
    return absl::OkStatus();
  }

  // Computes a hash over the full content of this depot.
  absl::StatusOr<Hash> GetHash() const {
    if (hash_.has_value()) {
      return *hash_;
    }
    // Cache the hash of the wrapped depot.
    ASSIGN_OR_RETURN(hash_, depot_.GetHash());
    return *hash_;
  }

  // Retrieves the proof a snapshot of the current state would exhibit.
  absl::StatusOr<DepotProof> GetProof() const {
    ASSIGN_OR_RETURN(auto hash, GetHash());
    return DepotProof(hash);
  }

  // Creates a snapshot of the data maintained in this depot. Snapshots may be
  // used to transfer state information between instances without the need of
  // blocking other operations on the depot.
  // The resulting snapshot references content in this depot and must not
  // outlive the depot instance.
  absl::StatusOr<Snapshot> CreateSnapshot() const {
    return absl::UnimplementedError("to be implemented");
  }

  // Updates this depot to match the content of the given snapshot. This
  // invalidates all former snapshots taken from this depot before starting to
  // sync. Thus, instances can not sync to a former version of itself.
  absl::Status SyncTo(const Snapshot&) {
    return absl::UnimplementedError("to be implemented");
  }

  // Flush unsaved depot data to disk.
  absl::Status Flush() { return depot_.Flush(); }

  // Close this depot and release resources.
  absl::Status Close() { return depot_.Close(); }

  // Summarizes the memory usage of this instance.
  MemoryFootprint GetMemoryFootprint() const {
    MemoryFootprint res(*this);
    res.Add("depot", depot_.GetMemoryFootprint());
    res.Add("cache", cache_.GetMemoryFootprint());
    return res;
  }

 private:
  constexpr static std::size_t kDefaultSize = 1 << 18;  // ~260k

  // The underlying depot to be wrapped.
  D depot_;

  // The maintained in-memory value cache.
  mutable LeastRecentlyUsedCache<key_type,
                                 absl::StatusOr<std::vector<std::byte>>>
      cache_;

  // Set if the hash is up-to-date.
  mutable std::optional<Hash> hash_;
};

}  // namespace carmen::backend::depot
