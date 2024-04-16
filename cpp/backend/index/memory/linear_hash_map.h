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

#include <cstdint>
#include <iostream>

#include "absl/hash/hash.h"
#include "common/memory_usage.h"

namespace carmen::backend::index {

// A hash based, unordered, Key/Value map implementing a linear hash map.
// Unlike classical hash maps, which depend on rehashing when growing beyond
// capacity limits, a linear hash map grows 'linearly' (=gradually), and thus
// distributes the costs of rehashing among a larger number of insert
// operations.
//
// This map was implemented as a prototype map for a file based, persistent hash
// map. The property of gradually growing in size and not depending on costly
// rehashing is one of the key adavantages of this type of hash map when being
// mapped to disk storage compared to classical hash map implementations.
template <typename K, typename V, std::size_t elements_in_bucket = 128>
class LinearHashMap {
 public:
  using key_type = K;
  using value_type = V;
  using entry_type = std::pair<K, V>;

  LinearHashMap()
      : low_mask_((1 << kInitialHashLength) - 1),
        high_mask_((low_mask_ << 1) | 0x1),
        buckets_(1 << kInitialHashLength) {}

  // Inserts the given key/value pair into the map. As for STL maps, if the key
  // is already present, the value will not be updated. The operation returns a
  // pointer to the entry associated to the given key after the insert and a
  // boolean indicating whether an insert occured (=true) or the key has already
  // been present (=false).
  std::pair<entry_type*, bool> Insert(const entry_type& new_entry) {
    auto [hash, bucket, entry] = FindInternal(new_entry.first);
    if (entry != nullptr) {
      return {&entry->value, false};
    }

    size_++;

    // Trigger a split if the bucket is full.
    if (bucket.IsFull()) {
      Split();

      // After the split, the target bucket may be a different one.
      auto bucket_position = GetBucket(hash);
      auto trg_entry = buckets_[bucket_position].Insert(hash, new_entry);
      return {&trg_entry->value, true};
    }

    // Insert a new entry.
    auto trg_entry = bucket.Insert(hash, new_entry);
    return {&trg_entry->value, true};
  }

  // Same as above, but will in any way update the value associated to the given
  // key to the given value. The second value returned indicates whether the
  // element was inserted (=true) or updated (=false).
  std::pair<entry_type*, bool> InsertOrAssign(const K& key, const V& value) {
    auto [pos, new_entry] = Insert({key, value});
    pos->second = value;
    return {pos, new_entry};
  }

  // Locates the entry associated to the given key in this map, or returns
  // nullptr if there is no such element.
  const entry_type* Find(const K& key) const {
    auto entry = std::get<const Entry*>(FindInternal(key));
    if (entry == nullptr) {
      return nullptr;
    }
    return &entry->value;
  }

  // Same as above, but for non-const instances.
  entry_type* Find(const K& key) {
    // Reuse const version of find operation to get entry to be mutated.
    return const_cast<entry_type*>(
        const_cast<const LinearHashMap*>(this)->Find(key));
  }

  // Determines whether the given key is present in this map.
  bool Contains(const K& key) const { return Find(key) != nullptr; }

  // Determines the number of different key/value pairs stored in this map.
  std::size_t Size() const { return size_; }

  // Support the subscript operator to access map elements.
  V& operator[](const K& key) { return Insert({key, V{}}).first->second; }

  // For debugging: dumps the content of this map to std::cout.
  void Dump() {
    for (std::size_t i = 0; i < buckets_.size(); i++) {
      std::cout << "Bucket " << i << ":\n";
      buckets_[i].Dump();
    }
    std::cout << "\n";
  }

  // Summarizes the memory usage fo this object.
  MemoryFootprint GetMemoryFootprint() const {
    MemoryFootprint res(*this);
    res.Add("buckets", SizeOf(buckets_));
    return res;
  }

 private:
  constexpr static const std::uint8_t kInitialHashLength = 2;

  // An entry is the type of single line in the table underlying this map.
  struct Entry {
    // Entries need to be sorted by their hash within pages.
    bool operator<(const Entry& other) const { return hash < other.hash; }
    // The cached hash of this entry.
    std::size_t hash;
    // The key/value a pair of this entry.
    entry_type value;
  };

  // A page is a sorted list of entries (sorted by their hash), and part of a
  // linked list representing a single bucket of the hash map. All entries in
  // this list of pages share the same hash suffix.
  // Note: while entries within a single page are sorted, no sorting criteria is
  // enforced accross pages of a single bucket.
  struct Page {
    // Locates an entry within a list of pages. If found in the current page, a
    // pointer to the corresponding entry is returned. If not found, the
    // following page in the list is consulted. If there is no such page, a
    // nullptr is returned.
    const Entry* Find(std::size_t hash, const K& key) const {
      Entry should;
      should.hash = hash;
      auto range =
          std::equal_range(entries.begin(), entries.begin() + size, should);
      for (auto cur = range.first; cur != range.second; ++cur) {
        if (cur->value.first == key) {
          return &*cur;
        }
      }
      if (next) {
        return next->Find(hash, key);
      }
      return nullptr;
    }

    // Inserts a new entry in this list of pages. If there is space in the
    // current page it will be inserted here, otherwise the insertion will be
    // delegated to the next page.
    // Returns the page in which the entry ended up + a pointer to the new
    // entry.
    std::pair<Page*, Entry*> Insert(std::size_t hash, const entry_type& entry) {
      if (size >= entries.size()) {
        assert(!next);
        next = std::make_unique<Page>();
        return next->Insert(hash, entry);
      }

      // Locate insertion position.
      Entry should;
      should.hash = hash;
      auto end = entries.begin() + size;
      auto pos = std::upper_bound(entries.begin(), end, should);

      // Move remaining data one step to the right.
      std::memmove(pos + 1, pos, sizeof(Entry) * (end - pos));

      // Write new element to proper location.
      pos->hash = hash;
      pos->value = entry;

      // Now there is one element more.
      size++;
      return {this, pos};
    }

    // Determines whether this page is full.
    bool IsFull() const { return size == entries.size(); }

    void Dump() {
      std::cout << "\tPage of size " << std::uint32_t(size) << ":\n";
      for (std::size_t i = 0; i < size; i++) {
        Entry& cur = entries[i];
        std::cout << "\t\t" << cur.hash << " | " << cur.value.first << " => "
                  << cur.value.second << "\n";
      }
      if (next) {
        next->Dump();
      }
    }

    // The entries owned by this page, sorted by their hash.
    std::array<Entry, elements_in_bucket> entries;
    // The number of entries containing valid elements.
    std::uint8_t size = 0;
    // A pointer to the next page in the list of pages of a bucket. Null if this
    // is the last page.
    std::unique_ptr<Page> next;
  };

  // A bucket contains a linked list of pages storing elements with a common
  // hash prefix.
  struct Bucket {
    // Tries to locate a entry based on a hash and key. Returns null if there is
    // no such entry.
    const Entry* Find(std::size_t hash, const K& key) const {
      return head ? head->Find(hash, key) : nullptr;
    }

    // Same as above but for non-const buckets.
    Entry* Find(std::size_t hash, const K& key) {
      // Reuse the const version of the find.
      return const_cast<Entry*>(
          const_cast<const Bucket*>(this)->Find(hash, key));
    }

    // Returns true if after the insert this bucket uses an overflow bucket.
    Entry* Insert(std::size_t hash, const entry_type& entry) {
      if (!head) {
        head = std::make_unique<Page>();
        tail = head.get();
      }
      auto [new_tail, trg_entry] = tail->Insert(hash, entry);
      tail = new_tail;
      return trg_entry;
    }

    // A bucket is considered full if it has a first page and this page is full.
    bool IsFull() const { return head && head->IsFull(); }

    void Dump() {
      if (tail == nullptr) {
        std::cout << "\t<empty>\n";
        return;
      }
      head->Dump();
    }

    std::unique_ptr<Page> head;
    Page* tail;
  };

  // A helper function to locate a entry in this map. Returns a tuple containing
  // the key's hash, the containing bucket, and the containing entry. Only if
  // the entry pointer is not-null the entry has been found.
  std::tuple<std::size_t, const Bucket&, const Entry*> FindInternal(
      const K& key) const {
    auto hash = hasher_(key);
    auto bucket_position = GetBucket(hash);
    const Bucket& bucket = buckets_[bucket_position];
    return {hash, bucket, bucket.Find(hash, key)};
  }

  // Same as above, but for non-const instances.
  std::tuple<std::size_t, Bucket&, Entry*> FindInternal(const K& key) {
    auto hash = hasher_(key);
    auto bucket_position = GetBucket(hash);
    Bucket& bucket = buckets_[bucket_position];
    return {hash, bucket, bucket.Find(hash, key)};
  }

  // Performs a split of a bucket resulting in the linear growth of the table.
  // In each split one bucket is selected and divided into two buckets. While
  // doing so, the old bucket is reused and one additional bucket is created.
  // Buckets are split in a round-robbing order.
  void Split() {
    assert(next_to_split_ < buckets_.size());

    // When a full cycle is completed ...
    if (next_to_split_ > low_mask_) {
      // ... increase the hash mask by one bit ...
      low_mask_ = high_mask_;
      high_mask_ = (high_mask_ << 1) | 0x1;
      // ... and start at zero again.
      next_to_split_ = 0;
    }

    // Add a new bucket at the end.
    buckets_.emplace_back();
    Bucket& old_bucket = buckets_[next_to_split_++];
    Bucket& new_bucket = buckets_.back();

    // If the bucket to be split is empty, we are done.
    if (old_bucket.head == nullptr) {
      return;
    }

    // Prepare utility to append entries in the old bucket.
    std::size_t old_append_pos = 0;
    Page* old_page_tail = old_bucket.head.get();
    auto append_to_old = [&](const Entry& entry) {
      if (old_append_pos >= old_page_tail->entries.size()) {
        old_page_tail->size = old_append_pos;
        std::sort(old_page_tail->entries.begin(), old_page_tail->entries.end());
        assert(old_page_tail->next);
        old_page_tail = old_page_tail->next.get();
        old_append_pos = 0;
      }
      old_page_tail->entries[old_append_pos++] = entry;
    };

    // Prepare utility to append entries in new bucket.
    std::size_t new_append_pos = 0;
    Page* new_page_tail = nullptr;
    auto append_to_new = [&](const Entry& entry) {
      if (new_page_tail == nullptr) {
        new_bucket.head = std::make_unique<Page>();
        new_page_tail = new_bucket.head.get();
      }
      if (new_append_pos >= new_page_tail->entries.size()) {
        new_page_tail->size = new_append_pos;
        std::sort(new_page_tail->entries.begin(), new_page_tail->entries.end());
        new_page_tail->next = std::make_unique<Page>();
        new_page_tail = new_page_tail->next.get();
        new_append_pos = 0;
      }
      new_page_tail->entries[new_append_pos++] = entry;
    };

    // Distribute keys between old and new bucket.
    const auto mask = low_mask_ ^ high_mask_;
    for (Page* cur = old_bucket.head.get(); cur != nullptr;
         cur = cur->next.get()) {
      for (std::size_t i = 0; i < cur->size; i++) {
        const auto& entry = cur->entries[i];
        if (cur->entries[i].hash & mask) {
          append_to_new(entry);
        } else {
          append_to_old(entry);
        }
      }
    }

    // Clean up old bucket.
    old_page_tail->size = old_append_pos;
    std::sort(old_page_tail->entries.begin(),
              old_page_tail->entries.begin() + old_append_pos);
    old_page_tail->next.reset();
    old_bucket.tail = old_page_tail;

    // Clean up new bucket.
    if (new_page_tail != nullptr) {
      new_page_tail->size = new_append_pos;
      std::sort(new_page_tail->entries.begin(),
                new_page_tail->entries.begin() + new_append_pos);
      new_bucket.tail = new_page_tail;
    }
  }

  // Obtains the index of the bucket the given hash key is supposed to be
  // located.
  std::size_t GetBucket(std::size_t hash_key) const {
    std::size_t bucket = hash_key & high_mask_;
    return bucket >= buckets_.size() ? hash_key & low_mask_ : bucket;
  }

  // A hasher to compute hashes for keys.
  absl::Hash<K> hasher_;

  // The number of elements in this container.
  std::size_t size_ = 0;

  // The next bucket to be split.
  std::size_t next_to_split_ = 0;

  // The mask for mapping keys to buckets that have not yet been split in the
  // current bucket split iteration.
  std::size_t low_mask_;

  // The mask for mapping keys to buckets that have already been split in the
  // current bucket split iteration.
  std::size_t high_mask_;

  // The buckets in this hash, growing linearly over time.
  std::deque<Bucket> buckets_;
};

}  // namespace carmen::backend::index
