#pragma once

#include <filesystem>
#include <optional>
#include <queue>

#include "absl/hash/hash.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/index/memory/linear_hash_map.h"
#include "backend/index/snapshot.h"
#include "backend/structure.h"
#include "common/hash.h"
#include "common/memory_usage.h"
#include "common/type.h"

namespace carmen::backend::index {

template <Trivial K, std::integral I, std::size_t elements_in_bucket = 256>
class InMemoryLinearHashIndex {
 public:
  using key_type = K;
  using value_type = I;
  using Snapshot = IndexSnapshot<K>;

  // A factory function creating an instance of this index type.
  static absl::StatusOr<InMemoryLinearHashIndex> Open(
      Context&, const std::filesystem::path&) {
    return InMemoryLinearHashIndex();
  }

  absl::StatusOr<std::pair<I, bool>> GetOrAdd(const K& key) {
    auto [entry, new_entry] = data_.Insert({key, 0});
    if (new_entry) {
      entry->second = data_.Size() - 1;
      unhashed_keys_.push(key);
    }
    return std::pair{entry->second, new_entry};
  }

  absl::StatusOr<I> Get(const K& key) const {
    auto pos = data_.Find(key);
    if (pos == nullptr) {
      return absl::NotFoundError("Key not found.");
    }
    return pos->second;
  }

  absl::StatusOr<Hash> GetHash() const {
    while (!unhashed_keys_.empty()) {
      hash_ = carmen::GetHash(hasher_, hash_, unhashed_keys_.front());
      unhashed_keys_.pop();
    }
    return hash_;
  }

  absl::StatusOr<typename Snapshot::Proof> GetProof() const {
    ASSIGN_OR_RETURN(auto hash, GetHash());
    return typename Snapshot::Proof(hash);
  }

  absl::StatusOr<Snapshot> CreateSnapshot() const {
    return absl::UnimplementedError("to be implemented");
  }

  absl::Status SyncTo(const Snapshot&) {
    return absl::UnimplementedError("to be implemented");
  }

  absl::Status Flush() { return absl::OkStatus(); }

  absl::Status Close() { return absl::OkStatus(); }

  MemoryFootprint GetMemoryFootprint() const {
    MemoryFootprint res(*this);
    res.Add("data", data_.GetMemoryFootprint());
    res.Add("unhashed", SizeOf(unhashed_keys_));
    return res;
  }

 private:
  LinearHashMap<K, I, elements_in_bucket> data_;
  mutable std::queue<K> unhashed_keys_;
  mutable Sha256Hasher hasher_;
  mutable Hash hash_;
};

}  // namespace carmen::backend::index
