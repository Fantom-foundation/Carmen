#pragma once

#include <concepts>
#include <deque>
#include <filesystem>
#include <memory>
#include <optional>
#include <queue>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/index/index.h"
#include "backend/structure.h"
#include "common/hash.h"
#include "common/memory_usage.h"
#include "common/status_util.h"
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
  // The type of snapshot produced by this index.
  using Snapshot = IndexSnapshot<K>;

  // A factory function creating an instance of this index type.
  static absl::StatusOr<InMemoryIndex> Open(Context&,
                                            const std::filesystem::path&);

  // Initializes an empty index.
  InMemoryIndex();

  // Retrieves the ordinal number for the given key. If the key
  // is known, it will return a previously established value
  // for the key. If the key has not been encountered before,
  // a new ordinal value is assigned to the key and stored
  // internally such that future lookups will return the same
  // value.
  absl::StatusOr<std::pair<I, bool>> GetOrAdd(const K& key) {
    auto [pos, is_new] = data_.insert({key, I{}});
    if (is_new) {
      pos->second = data_.size() - 1;
      list_->push_back(key);
    }
    return std::pair{pos->second, is_new};
  }

  // Retrieves the ordinal number for the given key if previously registered.
  // Otherwise, returns a not found status.
  absl::StatusOr<I> Get(const K& key) const {
    auto pos = data_.find(key);
    if (pos == data_.end()) {
      return absl::NotFoundError("Key not found");
    }
    return pos->second;
  }

  // Tests whether the given key is indexed by this container.
  bool Contains(const K& key) const { return data_.contains(key); }

  // Computes a hash over the full content of this index.
  absl::StatusOr<Hash> GetHash() const {
    auto& list = *list_;
    while (next_to_hash_ != list.size()) {
      hash_ = carmen::GetHash(hasher_, hash_, list[next_to_hash_++]);
      if (next_to_hash_ % kKeysPerPart == 0) {
        hashes_->push_back(hash_);
      }
    }
    return hash_;
  }

  // Retrieves the proof a snapshot of the current state would exhibit.
  absl::StatusOr<typename Snapshot::Proof> GetProof() const {
    ASSIGN_OR_RETURN(auto hash, GetHash());
    return typename Snapshot::Proof(hash);
  }

  // Creates a snapshot of this index shielded from future additions that can be
  // safely accessed concurrently to other operations. It internally references
  // state of this index and thus must not outlive this index object.
  absl::StatusOr<Snapshot> CreateSnapshot() const;

  // Updates this index to match the content of the given snapshot. This
  // invalidates all former snapshots taken from this index before starting to
  // sync. Thus, instances can not sync to a former version of itself.
  absl::Status SyncTo(const Snapshot&);

  // Flush unsafed index keys to disk.
  absl::Status Flush() { return absl::OkStatus(); }

  // Close this index and release resources.
  absl::Status Close() { return absl::OkStatus(); }

  // Summarizes the memory usage of this instance.
  MemoryFootprint GetMemoryFootprint() const {
    MemoryFootprint res(*this);
    res.Add("list", SizeOf(*list_));
    res.Add("hashes", SizeOf(*hashes_));
    res.Add("index", SizeOf(data_));
    return res;
  }

 private:
  static constexpr auto kKeysPerPart = IndexSnapshotDataSource<K>::kKeysPerPart;

  class SnapshotDataSource final : public IndexSnapshotDataSource<K> {
   public:
    SnapshotDataSource(Hash hash, const std::deque<K>& list,
                       const std::deque<Hash>& hashes)
        : IndexSnapshotDataSource<K>(list.size()),
          hash_(hash),
          num_keys_(list.size()),
          list_(list),
          hashes_(hashes) {}

    absl::StatusOr<IndexProof> GetProof(
        std::size_t part_number) const override {
      Hash begin = part_number == 0 ? Hash{} : hashes_[part_number - 1];
      Hash end =
          part_number == this->GetSize() - 1 ? hash_ : hashes_[part_number];
      return IndexProof(begin, end);
    }

    absl::StatusOr<IndexPart<K>> GetPart(
        std::size_t part_number) const override {
      ASSIGN_OR_RETURN(auto proof, GetProof(part_number));

      auto begin = part_number * kKeysPerPart;
      auto end = std::min(num_keys_, begin + kKeysPerPart);
      std::vector<K> keys;
      keys.reserve(end - begin);
      for (std::size_t i = begin; i < end; i++) {
        keys.push_back(list_[i]);
      }
      return IndexPart<K>(proof, std::move(keys));
    }

   private:
    // The hash of the index at time of the snapshot creation.
    const Hash hash_;
    // The number of known keys when the snapshot was created.
    const std::size_t num_keys_;
    // A reference to the index's main key list which might get extended after
    // the snapshot was created.
    const std::deque<K>& list_;
    // A reference to the index's history of hashes which might get extended
    // after the snapshot was created.
    const std::deque<Hash>& hashes_;
  };

  // The full list of keys in order of insertion. Thus, a key at position i is
  // mapped to value i. It is required for implementing snapshots. The list is
  // wrapped into a unique_ptr to support pointer stability under move
  // operations.
  std::unique_ptr<std::deque<K>> list_;

  // A list of historic hashes observed at regular intervals. Those hashes are
  // required for synchronization.
  std::unique_ptr<std::deque<Hash>> hashes_;

  // An index mapping keys to their identifier values.
  absl::flat_hash_map<K, I> data_;

  mutable std::size_t next_to_hash_ = 0;
  mutable Sha256Hasher hasher_;
  mutable Hash hash_{};
};

template <Trivial K, std::integral I>
absl::StatusOr<InMemoryIndex<K, I>> InMemoryIndex<K, I>::Open(
    Context&, const std::filesystem::path&) {
  return InMemoryIndex();
}

template <Trivial K, std::integral I>
InMemoryIndex<K, I>::InMemoryIndex()
    : list_(std::make_unique<std::deque<K>>()),
      hashes_(std::make_unique<std::deque<Hash>>()) {}

template <Trivial K, std::integral I>
absl::StatusOr<IndexSnapshot<K>> InMemoryIndex<K, I>::CreateSnapshot() const {
  ASSIGN_OR_RETURN(auto hash, GetHash());
  return IndexSnapshot<K>(
      hash, std::make_unique<SnapshotDataSource>(hash, *list_, *hashes_));
}

template <Trivial K, std::integral I>
absl::Status InMemoryIndex<K, I>::SyncTo(const Snapshot& snapshot) {
  // Reset the content of this Index.
  list_->clear();
  hashes_->clear();
  data_.clear();
  next_to_hash_ = 0;
  hash_ = Hash{};

  // Load data from the snapshot.
  for (std::size_t i = 0; i < snapshot.GetSize(); i++) {
    ASSIGN_OR_RETURN(auto part, snapshot.GetPart(i));
    for (const auto& key : part.GetKeys()) {
      data_[key] = list_->size();
      list_->push_back(key);
    }
    if (part.GetKeys().size() == kKeysPerPart) {
      ASSIGN_OR_RETURN(auto proof, snapshot.GetProof(i));
      hashes_->push_back(proof.end);
    }
  }

  // Refresh the hash.
  hash_ = snapshot.GetProof().end;
  next_to_hash_ = list_->size();
  return absl::OkStatus();
}

}  // namespace carmen::backend::index
