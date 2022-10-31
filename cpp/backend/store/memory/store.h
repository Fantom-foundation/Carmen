#pragma once

#include <cmath>
#include <deque>
#include <filesystem>
#include <limits>
#include <memory>
#include <type_traits>
#include <vector>

#include "backend/store/hash_tree.h"
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
      : pages_(std::make_unique<Pages>()),
        hashes_(std::make_unique<PageProvider>(*pages_),
                hash_branching_factor) {}

  // Instances can not be copied.
  InMemoryStore(const InMemoryStore&) = delete;

  // Updates the value associated to the given key.
  void Set(const K& key, V value) {
    auto page_number = key / elements_per_page;
    if (pages_->size() <= page_number) {
      pages_->resize(page_number + 1);
    }
    (*pages_)[page_number][key % elements_per_page] = value;
    hashes_.MarkDirty(page_number);
  }

  // Retrieves the value associated to the given key. If no values has
  // been previously set using a the Set(..) function above, the default
  // value defined during the construction of a store instance is returned.
  const V& Get(const K& key) const {
    static const V default_value{};
    auto page_number = key / elements_per_page;
    hashes_.RegisterPage(page_number);
    if (page_number >= pages_->size()) {
      return default_value;
    }
    return (*pages_)[page_number][key % elements_per_page];
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

    // Provides byte-level asscess to the maintained data.
    std::span<const std::byte> AsBytes() const {
      return std::as_bytes(std::span<const V>(data_));
    }

   private:
    std::array<V, elements_per_page> data_;
  };

  // The container type used to maintain the actual pages.
  using Pages = std::deque<Page>;

  // A page source providing the owned hash tree access to the stored pages.
  class PageProvider : public PageSource {
   public:
    PageProvider(Pages& pages) : pages_(pages) {}

    std::span<const std::byte> GetPageData(PageId id) override {
      static const Page empty{};
      if (id >= pages_.size()) {
        return empty.AsBytes();
      }
      return pages_[id].AsBytes();
    }

   private:
    Pages& pages_;
  };

  // An indexed list of pages containing the actual values. The container is
  // wrapped in a unique pointer to facilitate pointer stability under move.
  std::unique_ptr<Pages> pages_;

  // The data structure hanaging the hashing of states.
  mutable HashTree hashes_;
};

template <typename K, Trivial V, std::size_t page_size>
Hash InMemoryStore<K, V, page_size>::GetHash() const {
  return hashes_.GetHash();
}

}  // namespace carmen::backend::store
