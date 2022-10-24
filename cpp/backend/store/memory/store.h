#pragma once

#include <cmath>
#include <deque>
#include <filesystem>
#include <limits>
#include <memory>
#include <type_traits>
#include <vector>

#include "common/hash.h"
#include "common/type.h"

namespace carmen::backend::store {

// The InMemoryStore is an in-memory implementation of a mutable key/value
// store. It maps provided mutation and lookup support, as well as global
// state hashing support enabling to obtain a quick hash for the entire
// content.
template <typename K, Trivial V, std::size_t page_size = 32>
class InMemoryStore {
 public:
  // The page size in byte used by this store.
  constexpr static std::size_t kPageSize = page_size;

  // Creates a new InMemoryStore using the provided value as the
  // branching factor for hash computation.
  InMemoryStore(std::size_t hash_branching_factor = 32)
      : hash_branching_factor_(hash_branching_factor) {}

  // Instances can not be copied.
  InMemoryStore(const InMemoryStore&) = delete;

  // Updates the value associated to the given key.
  void Set(const K& key, V value) {
    auto page_number = key / elements_per_page;
    while (pages_.size() <= page_number) {
      pages_.push_back(std::make_unique<Page>());
    }
    (*pages_[page_number])[key % elements_per_page] = value;
  }

  // Retrieves the value associated to the given key. If no values has
  // been previously set using a the Set(..) function above, the default
  // value defined during the construction of a store instance is returned.
  const V& Get(const K& key) const {
    static const V default_value{};
    auto page_number = key / elements_per_page;
    if (page_number >= pages_.size()) {
      return default_value;
    }
    return (*pages_[page_number])[key % elements_per_page];
  }

  // Computes a hash over the full content of this store.
  Hash GetHash() const;

  // Ignored, since store is not backed by disk storage.
  void Flush() {}

  // Ignored, since store does not maintain any resources.
  void Close() {}

 private:
  constexpr static auto elements_per_page = page_size / sizeof(V);
  // A page of the InMemory storage holding a fixed length array of values.
  class Page {
   public:
    // Provides read only access to individual elements. No bounds are checked.
    const V& operator[](int pos) const { return data_[pos]; }

    // Provides mutable access to individual elements. No bounds are checked.
    V& operator[](int pos) { return data_[pos]; }

    // Appends the content of this page to the provided hasher instance.
    void AppendTo(Sha256Hasher& hasher) { hasher.Ingest(data_); }

   private:
    std::array<V, elements_per_page> data_;
  };

  // An indexed list of pages containing the actual values.
  std::deque<std::unique_ptr<Page>> pages_;

  // The branching factor used for aggregating hashes.
  const std::size_t hash_branching_factor_;
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

  const std::size_t branch_width = hash_branching_factor_;

  if (pages_.empty()) {
    return Hash();
  }

  Sha256Hasher hasher;
  std::vector<Hash> hashes;

  // Reserver maximum padded size.
  const auto padded_size =
      (pages_.size() % branch_width == 0)
          ? pages_.size()
          : (pages_.size() / branch_width + 1) * branch_width;
  hashes.reserve(padded_size);

  // Hash individual pages, forming the leaf level.
  for (const auto& page : pages_) {
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
