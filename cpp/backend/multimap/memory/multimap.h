#pragma once

#include <concepts>

#include "absl/container/btree_set.h"
#include "absl/status/statusor.h"
#include "backend/structure.h"
#include "common/memory_usage.h"
#include "common/type.h"

namespace carmen::backend::multimap {

// Implements an in-memory version of a MultiMap using a btree based set.
// To facilitate effecient search, only integral key types are supported.
// As a in-memory implementation no internal state is persisted at any point
// nor can persistent state be loaded from disk.
template <std::integral K, Trivial V>
class InMemoryMultiMap {
 public:
  using key_type = K;
  using value_type = V;

  // Opens a new, empty in-memory instance for this type, ignoring the
  // filesystem content. This operation never fails.
  static absl::StatusOr<InMemoryMultiMap> Open(Context&,
                                               const std::filesystem::path&) {
    return InMemoryMultiMap();
  }

  // Inserts the given key/value pair and returns true if the element has not
  // been present before, false otherwise. This operation never fails.
  absl::StatusOr<bool> Insert(const K& key, const V& value) {
    return set_.insert({key, value}).second;
  }

  // Tests whether the given key/value pair is present in this set. This
  // operation never fails.
  absl::StatusOr<bool> Contains(const K& key, const V& value) const {
    return set_.contains({key, value});
  }

  // Erases all entries with the given key. This operation never fails.
  absl::Status Erase(const K& key) {
    auto [from, to] = GetKeyRange(key);
    set_.erase(from, to);
    return absl::OkStatus();
  }

  // Erases a single key/value entry and indicates whether the entry has been
  // present. This operaton never fails.
  absl::StatusOr<bool> Erase(const K& key, const V& value) {
    return set_.erase({key, value}) != 0;
  }

  // Applies the given operation on each value associated to the given key.
  // This operaton never fails.
  template <typename Op>
  absl::Status ForEach(const K& key, const Op& op) const {
    auto [from, to] = GetKeyRange(key);
    for (auto it = from; it != to; ++it) {
      op(it->second);
    }
    return absl::OkStatus();
  }

  // Does nothing.
  absl::Status Flush() { return absl::OkStatus(); }

  // Does nothing.
  absl::Status Close() { return absl::OkStatus(); }

  // Computes the memory footprint of the
  MemoryFootprint GetMemoryFootprint() const {
    MemoryFootprint res(*this);
    res.Add("data", SizeOf<std::pair<K, V>>() * set_.size());
    return res;
  }

  // For testing only.
  template <typename Op>
  void ForEach(const Op& op) const {
    for (auto it = set_.begin(); it != set_.end(); ++it) {
      op(it->first, it->second);
    }
  }

 private:
  // A internal utility to get the range of the given key. The result is a pair
  // of iterators ranging from the begin (inclusive) to the end (exclusive) of
  // the corresponding key range.
  auto GetKeyRange(const K& key) const {
    return std::make_pair(set_.lower_bound({key, V{}}),
                          set_.lower_bound({key + 1, V{}}));
  }

  // The actual storage of the key/value pairs, sorted in lexicographical order.
  absl::btree_set<std::pair<K, V>> set_;
};

}  // namespace carmen::backend::multimap
