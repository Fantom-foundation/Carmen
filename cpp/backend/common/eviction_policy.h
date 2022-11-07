#pragma once

#include <concepts>
#include <cstddef>
#include <deque>
#include <optional>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"

namespace carmen::backend {

// A concept for an eviction policy to be used in a page pool.
template <typename P>
concept EvictionPolicy = requires(P a) {
  // Policies must be initializable by the pool size.
  {P(std::size_t{})};
  // Informs the policy that a page slot has been read.
  { a.Read(std::size_t{}) } -> std::same_as<void>;
  // Informs the policy that a page slot has been updated.
  { a.Written(std::size_t{}) } -> std::same_as<void>;
  // Informs the policy that a page slot has been removed.
  { a.Removed(std::size_t{}) } -> std::same_as<void>;
  // Requests a slot to be evicted.
  { a.GetPageToEvict() } -> std::same_as<std::optional<std::size_t>>;
};

// Implements a random eviction policy. Pages are grouped into two categories:
// dirty pages and clean pages. When picking a page to be evicted, the clean
// pages are considered first. If there are clean pages, a random entry is
// selected. If there are non, a random entry from the dirty pages is selected.
class RandomEvictionPolicy {
 public:
  RandomEvictionPolicy(std::size_t size = 100);
  void Read(std::size_t);
  void Written(std::size_t);
  void Removed(std::size_t);
  std::optional<std::size_t> GetPageToEvict();

 private:
  absl::flat_hash_set<std::size_t> clean_;
  absl::flat_hash_set<std::size_t> dirty_;
};

// Implements a least-recently-used eviction policy. When selecting a page to be
// evicted, the least recently used page is elected -- not considering whether
// the page is clean or dirty.
class LeastRecentlyUsedEvictionPolicy {
 public:
  LeastRecentlyUsedEvictionPolicy(std::size_t size = 100);
  void Read(std::size_t);
  void Written(std::size_t);
  void Removed(std::size_t);
  std::optional<std::size_t> GetPageToEvict();

  void Dump();

 private:
  // Entries used to form a double-linked list of least-recently-used positions.
  struct Entry {
    Entry* succ = nullptr;
    Entry* pred = nullptr;
  };

  // A list of all entries, indexed by the pool position.
  std::vector<Entry> entries_;

  // A pointer to the most recently used entry.
  Entry* head_ = nullptr;

  // A pointer to the least recently used entry to be evicted next. The element
  // pointed to is owned by the entries_ container.
  Entry* tail_ = nullptr;
};

}  // namespace carmen::backend
