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

#include <concepts>
#include <fstream>
#include <utility>

#include "absl/container/btree_set.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "backend/common/file.h"
#include "backend/structure.h"
#include "common/fstream.h"
#include "common/memory_usage.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen::backend::multimap {

// Implements an in-memory version of a MultiMap using a btree based set.
// To facilitate efficient search, only integral key types are supported.
// All index data for this implementation is loaded upon opening and resides
// fully in memory.
template <std::integral K, Trivial V>
class InMemoryMultiMap {
 public:
  using key_type = K;
  using value_type = V;

  // Loads the multimap stored in the given directory.
  static absl::StatusOr<InMemoryMultiMap> Open(
      Context&, const std::filesystem::path& directory) {
    auto file = directory / "data.dat";

    // If there is no such file,
    if (!std::filesystem::exists(file)) {
      return InMemoryMultiMap({}, std::move(file));
    }

    // Load data from file.
    ASSIGN_OR_RETURN(auto in,
                     FStream::Open(file, std::ios::binary | std::ios::in));

    uint64_t size;
    RETURN_IF_ERROR(in.Read(size));

    absl::btree_set<std::pair<K, V>> set;
    for (uint64_t i = 0; i < size; i++) {
      std::pair<K, V> entry;
      RETURN_IF_ERROR(in.Read(entry));
      set.emplace(std::move(entry));
    }

    RETURN_IF_ERROR(in.Close());

    return InMemoryMultiMap(std::move(set), std::move(file));
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
  // present. This operation never fails.
  absl::StatusOr<bool> Erase(const K& key, const V& value) {
    return set_.erase({key, value}) != 0;
  }

  // Applies the given operation on each value associated to the given key.
  // This operation never fails.
  template <typename Op>
  absl::Status ForEach(const K& key, const Op& op) const {
    auto [from, to] = GetKeyRange(key);
    for (auto it = from; it != to; ++it) {
      op(it->second);
    }
    return absl::OkStatus();
  }

  // Writes all data to the underlying file.
  absl::Status Flush() {
    // Start by creating the directory.
    RETURN_IF_ERROR(CreateDirectory(file_.parent_path()));

    ASSIGN_OR_RETURN(auto out,
                     FStream::Open(file_, std::ios::binary | std::ios::out));

    uint64_t num_elements = set_.size();
    RETURN_IF_ERROR(out.Write(num_elements));

    for (const auto& cur : set_) {
      RETURN_IF_ERROR(out.Write(cur));
    }

    return out.Close();
  }

  // Flushes all data to the underlying file.
  absl::Status Close() { return Flush(); }

  // Estimates the memory footprint of this map.
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
  // Internal constructor for the in-memory map accepting its data set and
  // backup file.
  InMemoryMultiMap(absl::btree_set<std::pair<K, V>> set,
                   std::filesystem::path file)
      : set_(std::move(set)), file_(std::move(file)) {}

  // An internal utility to get the range of the given key. The result is a pair
  // of iterators ranging from the beginning (inclusive) to the end (exclusive)
  // of the corresponding key range.
  auto GetKeyRange(const K& key) const {
    return std::make_pair(set_.lower_bound({key, V{}}),
                          set_.lower_bound({key + 1, V{}}));
  }

  // The actual storage of the key/value pairs, sorted in lexicographical order.
  absl::btree_set<std::pair<K, V>> set_;

  // The file this index is backed up to.
  std::filesystem::path file_;
};

}  // namespace carmen::backend::multimap
