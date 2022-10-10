#pragma once

#include <memory>
#include <vector>

#include "absl/container/flat_hash_map.h"

namespace carmen::backend::index {

// A simple cache implementation retaining a fixed number of least-recently-used
// elements. This cache will never retain more than a specified capacity +1
// elements, and thus has fixed upper limits on memory consumption.
template <typename K, typename V>
class LeastRecentlyUsedCache {
 public:
  // Creates a new LRU cache with the given maximum capacity of elements.
  LeastRecentlyUsedCache(std::size_t capacity = 10)
      : capacity_(capacity), cache_(capacity + 1) {}

  // Returns a pointer to the value mapped to the given key or nullptr, if there
  // is no such value in this cache. The access to the key is considered a use,
  // promoting the value in the LRU order.
  const V* Get(const K& key) {
    auto pos = cache_.find(key);
    if (pos == cache_.end()) {
      return nullptr;
    }
    assert(head_);
    assert(tail_);
    Touch(pos->second.get());
    return &pos->second->value;
  }

  // Adds or updates the value associated to the given key to this cache. If the
  // key is already present, the value will be updated and the key marked as
  // being used. If the value is not present, a new entry is added to this
  // cache. This may cause another entry to be removed if the cache size would
  // be exceeded.
  void Set(const K& key, const V& value) {
    auto [pos, new_entry] = cache_.insert({key, nullptr});
    // Create the entry if it is new.
    if (new_entry) {
      pos->second = std::make_unique<Entry>();
      pos->second->key = key;

      // Also, make the new entry the head of the LRU queue.
      pos->second->pred = nullptr;
      pos->second->succ = head_;
      if (head_) {
        head_->pred = pos->second.get();
      }
      head_ = pos->second.get();

      // The very first entry is head and tail at the same time.
      if (tail_ == nullptr) {
        tail_ = head_;
      }
    }
    pos->second->value = value;
    Touch(pos->second.get());
    if (new_entry && cache_.size() > capacity_) {
      DropLast();
    }
  }

  // For testing only: returns the list of contained keys in LRU order.
  std::vector<K> GetOrderedKeysForTesting() const {
    std::vector<K> keys;
    keys.reserve(cache_.size());
    auto cur = head_;
    while (cur) {
      keys.push_back(cur->key);
      cur = cur->succ;
    }
    return keys;
  }

 private:
  // The entry wrapping each maintained value to form a double-linked list for a
  // O(1) LRU policy.
  struct Entry {
    K key;
    V value;
    Entry* pred;
    Entry* succ;
  };

  const std::size_t capacity_;

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

  void DropLast() {
    if (tail_ == nullptr) return;
    auto new_tail = tail_->pred;
    new_tail->succ = nullptr;
    cache_.erase(tail_->key);
    tail_ = new_tail;
  }

  // The maintained in-memory value cache.
  absl::flat_hash_map<K, std::unique_ptr<Entry>> cache_;

  Entry* head_ = nullptr;
  Entry* tail_ = nullptr;
};

}  // namespace carmen::backend::index
