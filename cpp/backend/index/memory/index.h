/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3.
 */

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

  // A factory function creating an instance of this index type.
  static absl::StatusOr<InMemoryIndex> Open(Context&,
                                            const std::filesystem::path&);

  // Initializes an empty index.
  InMemoryIndex();

  // Initializes an index based on the content of the given snapshot.
  InMemoryIndex(const IndexSnapshot<K>& snapshot);

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
    }
    return hash_;
  }

  // Creates a snapshot of this index shielded from future additions that can be
  // safely accessed concurrently to other operations. It internally references
  // state of this index and thus must not outlive this index object.
  std::unique_ptr<IndexSnapshot<K>> CreateSnapshot() const;

  // Flush unsafed index keys to disk.
  absl::Status Flush() { return absl::OkStatus(); }

  // Close this index and release resources.
  absl::Status Close() { return absl::OkStatus(); }

  // Summarizes the memory usage of this instance.
  MemoryFootprint GetMemoryFootprint() const {
    MemoryFootprint res(*this);
    res.Add("list", SizeOf(*list_));
    res.Add("index", SizeOf(data_));
    return res;
  }

 private:
  // Implements a snapshot by capturing the current index's size and a view to
  // its key list.
  class Snapshot final : public IndexSnapshot<K> {
   public:
    Snapshot(const std::deque<K>& list) : size_(list.size()), list_(list) {}

    // Obtains the number of keys stored in the snapshot.
    std::size_t GetSize() const override { return size_; }

    std::span<const K> GetKeys(std::size_t from,
                               std::size_t to) const override {
      from = std::max<std::size_t>(0, std::min(from, size_));
      to = std::max<std::size_t>(from, std::min(to, size_));
      if (from == to) {
        return {};
      }
      buffer_.clear();
      buffer_.reserve(to - from);
      for (auto i = from; i < to; i++) {
        buffer_.push_back(list_[i]);
      }
      return std::span<const K>(buffer_).subspan(0, to - from);
    }

   private:
    // The number of elements in the index when the snapshot was created.
    const std::size_t size_;
    // A reference to the index's main key list which might get extended after
    // the snapshot was created.
    const std::deque<K>& list_;
    // An internal buffer used to retain ownership while accessing keys.
    mutable std::vector<K> buffer_;
  };

  // The full list of keys in order of insertion. Thus, a key at position i is
  // mapped to value i. It is required for implementing snapshots. The list is
  // wrapped into a unique_ptr to support pointer stability under move
  // operations.
  std::unique_ptr<std::deque<K>> list_;

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
    : list_(std::make_unique<std::deque<K>>()) {}

template <Trivial K, std::integral I>
InMemoryIndex<K, I>::InMemoryIndex(const IndexSnapshot<K>& snapshot)
    : InMemoryIndex() {
  // Insert all the keys in order.
  constexpr static const std::size_t kBlockSize = 1024;
  auto num_elements = snapshot.GetSize();
  for (std::size_t i = 0; i < num_elements; i += kBlockSize) {
    auto to = std::min(i + kBlockSize, num_elements);
    for (const auto& key : snapshot.GetKeys(i, to)) {
      // TODO: Handle errors.
      GetOrAdd(key).IgnoreError();
    }
  }
  // Refresh the hash.
  GetHash().IgnoreError();
}

template <Trivial K, std::integral I>
std::unique_ptr<IndexSnapshot<K>> InMemoryIndex<K, I>::CreateSnapshot() const {
  return std::make_unique<Snapshot>(*list_);
}

}  // namespace carmen::backend::index
