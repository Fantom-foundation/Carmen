/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3 
 */

#pragma once

#include <concepts>
#include <cstddef>
#include <deque>
#include <optional>

#include "absl/container/btree_set.h"
#include "backend/common/access_pattern.h"

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
  absl::btree_set<std::size_t> clean_;
  absl::btree_set<std::size_t> dirty_;
  Uniform eviction_pattern_;
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
