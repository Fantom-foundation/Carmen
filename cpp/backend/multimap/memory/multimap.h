#pragma once

#include <concepts>
#include <filesystem>
#include <fstream>
#include <utility>

#include "absl/container/btree_set.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "backend/common/file.h"
#include "backend/structure.h"
#include "common/memory_usage.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen::backend::multimap {

// Implements an in-memory version of a MultiMap using a btree based set.
// To facilitate effecient search, only integral key types are supported.
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
    std::fstream in(file, std::ios::binary | std::ios::in);
    auto check_stream = [&]() -> absl::Status {
      if (in.good()) return absl::OkStatus();
      return absl::InternalError(absl::StrFormat(
          "Failed to read data from file %s: %s", file, std::strerror(errno)));
    };

    uint64_t size;
    in.read(reinterpret_cast<char*>(&size), 8);
    RETURN_IF_ERROR(check_stream());

    absl::btree_set<std::pair<K, V>> set;
    for (uint64_t i = 0; i < size; i++) {
      std::pair<K, V> entry;
      in.read(reinterpret_cast<char*>(&entry), sizeof(entry));
      RETURN_IF_ERROR(check_stream());
      set.emplace(std::move(entry));
    }

    in.close();
    RETURN_IF_ERROR(check_stream());

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

  // Writes all data to the underlying file.
  absl::Status Flush() {
    // Start by creating the directory.
    if (!CreateDirectory(file_.parent_path())) {
      return absl::InternalError(absl::StrFormat(
          "Unable to create parent directory: %s", file_.parent_path()));
    }

    std::fstream out(file_, std::ios::binary | std::ios::out);
    auto check_stream = [&]() -> absl::Status {
      if (out.good()) return absl::OkStatus();
      return absl::InternalError(absl::StrFormat(
          "Failed to write data to file %s: %s", file_, std::strerror(errno)));
    };

    uint64_t num_elements = set_.size();
    out.write(reinterpret_cast<const char*>(&num_elements), 8);
    RETURN_IF_ERROR(check_stream());

    for (const auto& cur : set_) {
      out.write(reinterpret_cast<const char*>(&cur), sizeof(cur));
      RETURN_IF_ERROR(check_stream());
    }

    out.close();
    return check_stream();
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

  // A internal utility to get the range of the given key. The result is a pair
  // of iterators ranging from the begin (inclusive) to the end (exclusive) of
  // the corresponding key range.
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
