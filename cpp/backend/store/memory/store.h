#pragma once

#include <cmath>
#include <deque>
#include <limits>
#include <memory>
#include <type_traits>
#include <vector>

#include "common/hash.h"
#include "common/type.h"

namespace carmen::backend::store {

// A page of the InMemory storage holding a fixed length array of values.
template <Trivial V, std::size_t size>
class Page {
 public:
  // Provides read only access to individual elements. No bounds are checked.
  const V& operator[](int pos) const { return _data[pos]; }

  // Provides mutable access to individual elements. No bounds are checked.
  V& operator[](int pos) { return _data[pos]; }

  // Appends the content of this page to the provided hasher instance.
  void AppendTo(Sha256Hasher& hasher) {
    hasher.Ingest(reinterpret_cast<const std::byte*>(&_data[0]),
                  sizeof(V) * size);
  }

 private:
  std::array<V, size> _data;
};

// The InMemoryStore is an in-memory implementation of a mutable key/value
// store. It maps provided mutation and lookup support, as well as global
// state hashing support enabling to obtain a quick hash for the entire
// content.
template <typename K, Trivial V, std::size_t page_size = 32>
class InMemoryStore {
 public:
  // Creates a new InMemoryStore using the provided value as the
  // default value for all its storage cells. Any get for an uninitialized
  // key will return the provided default value.
  InMemoryStore(V default_value = {})
      : _default_value(std::move(default_value)) {}

  // Instances can not be copied.
  InMemoryStore(const InMemoryStore&) = delete;

  // Updates the value associated to the given key.
  void Set(const K& key, V value) {
    auto page_number = key / page_size;
    while (_pages.size() <= page_number) {
      _pages.push_back(std::make_unique<Page>());
    }
    (*_pages[page_number])[key % page_size] = value;
  }

  // Retrieves the value associated to the given key. If no values has
  // been previously set using a the Set(..) function above, the default
  // value defined during the construction of a store instance is returned.
  const V& Get(const K& key) const {
    auto page_number = key / page_size;
    if (page_number >= _pages.size()) {
      return _default_value;
    }
    return (*_pages[page_number])[key % page_size];
  }

  // Computes a hash over the full content of this store.
  Hash GetHash() const;

 private:
  using Page = Page<V, page_size>;

  const V _default_value;

  // An indexed list of pages containing the actual values.
  std::deque<std::unique_ptr<Page>> _pages;
};

template <typename K, Trivial V, std::size_t page_size>
Hash InMemoryStore<K, V, page_size>::GetHash() const {
  // The computation of the full store hash is comprising two
  // steps:
  //   - step 1: hashing of individual pages
  //   - step 2: an iterative, tree-shaped reduction to a single value
  //
  // Step 1: The content of each page is hashed and the result stored
  // as a vector of hashes. This vector provides the input layer of the
  // second step.
  //
  // Step 2: To aggregate the vector of hashes into a single hash, the
  // following steps are executed:
  //   - the hashes of the previous iteration are grouped into
  //     fixed-size buckets, the last one padded with zero hashes as needed
  //   - a hash is computed for the content of each bucket
  // Thus, in every step the number of hashes is reduced by a factor of
  // size of the buckets (=branch_width).
  // This process is repeated until the number of hashes produced in one
  // iteration is reduced to one. This hash is then the resulting overall
  // hash of the store.
  //
  // Future improvements:
  //  - track access to pages and maintain list of dirty pages
  //  - keep a record of all hashes of the aggregation states
  //  - when computing state hashes, only re-compute hashes affected
  //    by dirty pages.

  constexpr int branch_width = 32;

  if (_pages.empty()) {
    return Hash();
  }

  Sha256Hasher hasher;
  std::vector<Hash> hashes;

  // Reserver maximum padded size.
  const auto padded_size =
      (_pages.size() % branch_width == 0)
          ? _pages.size()
          : (_pages.size() / branch_width + 1) * branch_width;
  hashes.reserve(padded_size);

  // Hash individual pages, forming the leaf level.
  for (const auto& page : _pages) {
    hasher.Reset();
    page->AppendTo(hasher);
    hashes.push_back(hasher.GetHash());
  }

  // Perform a reduction on the tree.
  while (hashes.size() > 1) {
    // Add padding to the current input level.
    if (hashes.size() % branch_width != 0) {
      hashes.resize(((hashes.size() / branch_width) + 1) * branch_width);
    }
    // Perform one round of hashing in the tree.
    for (std::size_t i = 0; i < hashes.size() / branch_width; i++) {
      hasher.Reset();
      hasher.Ingest(
          reinterpret_cast<const std::byte*>(&hashes[i * branch_width]),
          sizeof(Hash) * branch_width);
      hashes[i] = hasher.GetHash();
    }
    hashes.resize(hashes.size() / branch_width);
  }

  return hashes[0];
}

}  // namespace carmen::backend::store
