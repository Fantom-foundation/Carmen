/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3.
 */

#pragma once

#include <memory>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "common/memory_usage.h"

namespace carmen::backend {

// A simple cache implementation retaining a fixed number of least-recently-used
// elements. This cache will never retain more than a specified capacity +1
// elements, and thus has fixed upper limits on memory consumption.
template <typename K, typename V>
class LeastRecentlyUsedCache {
 public:
  // Creates a new LRU cache with the given maximum capacity of elements.
  LeastRecentlyUsedCache(std::size_t capacity = 10)
      : entries_(capacity),
        index_(capacity + 1),
        head_(&entries_.front()),
        tail_(&entries_.back()) {
    Entry* last = nullptr;
    for (Entry& cur : entries_) {
      if (last) {
        last->succ = &cur;
      }
      cur.pred = last;
      last = &cur;
    }
  }

  // Returns a pointer to the value mapped to the given key or nullptr, if there
  // is no such value in this cache. The access to the key is considered a use,
  // promoting the value in the LRU order.
  const V* Get(const K& key) {
    auto pos = index_.find(key);
    if (pos == index_.end()) {
      return nullptr;
    }
    Touch(pos->second);
    return &pos->second->value;
  }

  // Adds or updates the value associated to the given key to this cache. If the
  // key is already present, the value will be updated and the key marked as
  // being used. If the value is not present, a new entry is added to this
  // cache. This may cause another entry to be removed if the cache size would
  // be exceeded.
  void Set(const K& key, const V& value) {
    auto [pos, new_entry] = index_.insert({key, nullptr});
    // Create the entry if it is new.
    if (new_entry) {
      pos->second = GetFreeEntry();
      pos->second->key = key;

      // Also, make the new entry the head of the LRU queue.
      pos->second->pred = nullptr;
      pos->second->succ = head_;
      if (head_) {
        head_->pred = pos->second;
      }
      head_ = pos->second;
    }
    pos->second->value = value;
    Touch(pos->second);
  }

  // For testing only: returns the list of contained keys in LRU order.
  std::vector<K> GetOrderedKeysForTesting() const {
    std::vector<K> keys;
    keys.reserve(index_.size());
    auto cur = head_;
    while (cur && keys.size() < index_.size()) {
      keys.push_back(cur->key);
      cur = cur->succ;
    }
    return keys;
  }

  // Summarizes the memory usage of this instance.
  MemoryFootprint GetMemoryFootprint() const {
    MemoryFootprint res(*this);
    res.Add("entries", SizeOf(entries_));
    res.Add("index", SizeOf(index_));
    return res;
  }

 private:
  // The entry wrapping each maintained value to form a double-linked list for a
  // O(1) LRU policy.
  struct Entry {
    K key;
    V value;
    Entry* pred = nullptr;
    Entry* succ = nullptr;
  };

  // Registers an access to an entry by moving it to the front of the LRU queue.
  void Touch(Entry* entry) {
    assert(entry);
    if (entry == head_) return;

    // Remove entry from current position in list.
    assert(entry->pred);
    entry->pred->succ = entry->succ;

    if (entry->succ) {
      entry->succ->pred = entry->pred;
    } else {
      tail_ = entry->pred;
    }

    // Make the entry the new head.
    entry->pred = nullptr;
    entry->succ = head_;
    head_->pred = entry;
    head_ = entry;
  }

  Entry* GetFreeEntry() {
    assert(tail_ != nullptr);
    auto new_tail = tail_->pred;
    new_tail->succ = nullptr;
    if (index_.size() > entries_.size()) {
      index_.erase(tail_->key);
    }
    auto result = tail_;
    tail_ = new_tail;
    return result;
  }

  // The entries stored in this cache.
  std::vector<Entry> entries_;

  // An index to the stored entries.
  absl::flat_hash_map<K, Entry*> index_;

  // The list of entries in LRU order.
  Entry* head_ = nullptr;
  Entry* tail_ = nullptr;
};

}  // namespace carmen::backend
