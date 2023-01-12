#pragma once

#include <assert.h>

#include <algorithm>
#include <cstring>

#include "backend/common/page.h"
#include "backend/common/page_id.h"
#include "common/type.h"

namespace carmen::backend::index {

// A HashPage defines the page format used by the file based Index to store
// key/value pairs. Each page maintains a list of key/value pairs, ordered by
// their hash and a next-pageID pointer to chain up pages.
template <Trivial H, Trivial K, Trivial V, std::size_t page_size>
class alignas(kFileSystemPageSize) HashPage {
  // A struct describing the meta data stored in each page.
  struct Metadata {
    // The number of elements stored in this page (<=kNumEntries)
    std::uint64_t size;
    // A pointer to the next page in a bucket's page list.
    PageId next;
  };

  static_assert(sizeof(Metadata) == 16);

  // A constant providing the full size of this page in memory and on disk.
  // Note that due to alignment constraints, this may exceed the specified
  // page_size.
  constexpr static std::size_t full_page_size = GetRequiredPageSize(page_size);

 public:
  // An Entry describes a single key/value pair stored in this page + the hash
  // of the key, which is required for lookup operations. All entries in a
  // single page share a common hash value suffix, however, the length of it
  // depends on the number of pages in the same file.
  struct Entry {
    H hash;
    K key;
    V value;

    // A total order based on the hash, to support sorting.
    bool operator<(const Entry& other) const { return hash < other.hash; }
  };

  // The maximum number of key/value pairs stored per page.
  constexpr const static std::size_t kNumEntries =
      (page_size - sizeof(Metadata)) / (sizeof(Entry));

  // Make sure there is at least space for a single key/value pair per page.
  static_assert(
      kNumEntries > 0,
      "A HashPage must be at least sizeof(Metadata) + sizeof(Entry) = 16 byte "
      "+ sizeof(Entry) to fit at least a single line in each page.");

  // Resets the size and the next-page reference to zero.
  void Clear() {
    auto& metadata = GetMetadata();
    metadata.next = 0;
    metadata.size = 0;
  }

  // Retrieves the next-page ID stored in this page.
  PageId GetNext() const { return GetMetadata().next; }

  // Updates the next-page ID stored in this page.
  void SetNext(PageId page) { GetMetadata().next = page; }

  // Attempts to locate a key in this page. The provided hash value must be the
  // hash of the key. The function returns a pointer to the entry with the given
  // key if present, or nullptr if no such entry can be found.
  const Entry* Find(H hash, const K& key) const {
    auto& data = GetData();
    Entry should;
    should.hash = hash;
    auto range = std::equal_range(data.begin(), data.begin() + Size(), should);
    for (auto cur = range.first; cur != range.second; ++cur) {
      if (cur->key == key) {
        return &*cur;
      }
    }
    return nullptr;
  }

  // Same as above, but for mutable pages.
  Entry* Find(H hash, const K& key) {
    return const_cast<Entry*>(
        const_cast<const HashPage*>(this)->Find(hash, key));
  }

  // Inserts a new entry into this page. This function does not verify that the
  // provided key is not yet present. Returns a pointer to the new entry or
  // nullptr if this page is full.
  Entry* Insert(std::size_t hash, const K& key, const V& value) {
    auto& data = GetData();
    auto size = Size();
    if (size >= data.size()) {
      return nullptr;
    }

    // Locate insertion position.
    Entry should;
    should.hash = hash;
    auto end = data.begin() + size;
    auto pos = std::upper_bound(data.begin(), end, should);

    // Move remaining data one step to the right.
    std::memmove(pos + 1, pos, sizeof(Entry) * (end - pos));

    // Write new element to proper location.
    pos->hash = hash;
    pos->key = key;
    pos->value = value;

    // Now there is one element more.
    IncrementSize();
    return pos;
  }

  // Determines whether this page is full.
  bool IsFull() const { return Size() == kNumEntries; }

  // Gets the number of elements in this page.
  std::size_t Size() const { return GetMetadata().size; }

  // Updates the size of this page. If the new size is less, then the current
  // size, entries are dropped. If the new size is larger, the additional
  // elements will have an unspecified, yet valid value.
  void Resize(std::size_t new_size) {
    assert(0 <= new_size && new_size <= kNumEntries);
    GetMetadata().size = new_size;
  }

  // Provides subscript access to a single entry.
  const Entry& operator[](std::size_t i) const { return GetData()[i]; }

  // Same as above, for non-const pages.
  Entry& operator[](std::size_t i) { return GetData()[i]; }

  // Returns a raw data view on this page that can be used for writting the page
  // to secondary storage.
  std::span<const std::byte, full_page_size> AsRawData() const { return data_; }

  // Returns a mutable raw data view on this page that can be used to replace
  // its content with data read from secondary storage.
  std::span<std::byte, full_page_size> AsRawData() { return data_; }

  // Returns a span of the raw data of this page, as required by the page
  // concept.
  operator std::span<const std::byte, full_page_size>() const {
    return AsRawData();
  }

  // Returns a span of the raw data of this page, as required by the page
  // concept.
  operator std::span<std::byte, full_page_size>() { return AsRawData(); }

  // Debug utility to print the content of a single page.
  void Dump() {
    auto size = Size();
    std::cout << "\t\tPage of size " << page_size << " with " << size << " of "
              << kNumEntries << " elements:\n";
    auto& data = GetData();
    for (std::size_t i = 0; i < size; i++) {
      Entry& cur = data[i];
      std::cout << "\t\t\t" << cur.hash << " | " << cur.key << " => "
                << cur.value << "\n";
    }
    std::cout << "\t\tNext: " << GetNext() << "\n";
  }

 private:
  // Obtains a reference to the array of entries stored in this page.
  const std::array<Entry, kNumEntries>& GetData() const {
    return const_cast<HashPage*>(this)->GetData();
  }

  // Same as above, but for non-const pages.
  std::array<Entry, kNumEntries>& GetData() {
    return *reinterpret_cast<std::array<Entry, kNumEntries>*>(&data_[0]);
  }

  // Obtains access to the metadata stored in this page.
  const Metadata& GetMetadata() const {
    return const_cast<HashPage*>(this)->GetMetadata();
  }

  // Same as above, but for non-const pages.
  Metadata& GetMetadata() {
    return *reinterpret_cast<Metadata*>(&data_[page_size - sizeof(Metadata)]);
  }

  // Increments the number of elements stored in this page by 1.
  void IncrementSize() { GetMetadata().size++; }

  // The raw data containing kNumEntries entries + metadata at the end.
  std::array<std::byte, full_page_size> data_;
};

}  // namespace carmen::backend::index
