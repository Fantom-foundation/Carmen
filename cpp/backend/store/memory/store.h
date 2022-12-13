#pragma once

#include <cmath>
#include <cstring>
#include <deque>
#include <filesystem>
#include <limits>
#include <memory>
#include <type_traits>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/store/hash_tree.h"
#include "backend/store/store.h"
#include "common/hash.h"
#include "common/memory_usage.h"
#include "common/type.h"

namespace carmen::backend::store {

// The InMemoryStore is an in-memory implementation of a mutable key/value
// store. It maps provided mutation and lookup support, as well as global
// state hashing support enabling to obtain a quick hash for the entire
// content.
template <typename K, Trivial V, std::size_t page_size = 32>
class InMemoryStore {
 public:
  // The value type used to index elements in this store.
  using key_type = K;

  // The type of value stored in this store.
  using value_type = V;

  // The page size in byte used by this store.
  constexpr static std::size_t kPageSize = page_size;

  // A factory function creating an instance of this store type.
  static absl::StatusOr<InMemoryStore> Open(Context&,
                                            const std::filesystem::path&) {
    return InMemoryStore();
  }

  // Creates a new InMemoryStore using the provided value as the
  // branching factor for hash computation.
  InMemoryStore(std::size_t hash_branching_factor = 32)
      : pages_(std::make_unique<Pages>()),
        hashes_(std::make_unique<PageProvider>(*pages_),
                hash_branching_factor) {}

  // Instances can not be copied.
  InMemoryStore(const InMemoryStore&) = delete;

  InMemoryStore(InMemoryStore&&) = default;

  // Initializes a new store instance based on the given snapshot data.
  InMemoryStore(const StoreSnapshot&, std::size_t hash_branching_factor = 32);

  // Updates the value associated to the given key.
  absl::Status Set(const K& key, V value) {
    auto page_number = key / elements_per_page;
    if (pages_->size() <= page_number) {
      pages_->resize(page_number + 1);
    }
    (*pages_)[page_number][key % elements_per_page] = value;
    hashes_.MarkDirty(page_number);
    return absl::OkStatus();
  }

  // Retrieves the value associated to the given key. If no values has
  // been previously set using the Set(..) function above, the default
  // value defined during the construction of a store instance is returned.
  StatusOrRef<const V> Get(const K& key) const {
    constexpr static const V default_value{};
    auto page_number = key / elements_per_page;
    hashes_.RegisterPage(page_number);
    if (page_number >= pages_->size()) {
      return default_value;
    }
    return (*pages_)[page_number][key % elements_per_page];
  }

  // Creates a snapshot of the data maintained in this store. Snapshots may be
  // used to transfer state information between instances without the need of
  // blocking other operations on the store.
  // The resulting snapshot references content in this store and must not
  // outlive the store instance.
  std::unique_ptr<StoreSnapshot> CreateSnapshot() const;

  // Computes a hash over the full content of this store.
  absl::StatusOr<Hash> GetHash() const;

  // Ignored, since store is not backed by disk storage.
  absl::Status Flush() { return absl::OkStatus(); }

  // Ignored, since store does not maintain any resources.
  absl::Status Close() { return absl::OkStatus(); }

  // Summarizes the memory usage of this instance.
  MemoryFootprint GetMemoryFootprint() const {
    MemoryFootprint res(*this);
    res.Add("pages", SizeOf(*pages_));
    res.Add("hashes", hashes_.GetMemoryFootprint());
    return res;
  }

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

    // Provides mutable byte-level access to the maintained data.
    std::span<std::byte> AsBytes() {
      return std::as_writable_bytes(std::span<V>(data_));
    }

   private:
    std::array<V, elements_per_page> data_;
  };

  // The container type used to maintain the actual pages.
  using Pages = std::deque<Page>;

  // A naive snapshot implementation accepting a deep copy of all the data in
  // the store.
  class DeepSnapshot : public StoreSnapshot {
   public:
    DeepSnapshot(Pages pages) : pages_(std::move(pages)) {}

    std::size_t GetNumPages() const override { return pages_.size(); }

    std::span<const std::byte> GetPageData(PageId id) const override {
      static const Page empty{};
      if (id >= pages_.size()) {
        return empty.AsBytes();
      }
      return pages_[id].AsBytes();
    }

   private:
    Pages pages_;
  };

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
absl::StatusOr<Hash> InMemoryStore<K, V, page_size>::GetHash() const {
  return hashes_.GetHash();
}

template <typename K, Trivial V, std::size_t page_size>
InMemoryStore<K, V, page_size>::InMemoryStore(const StoreSnapshot& snapshot,
                                              std::size_t hash_branching_factor)
    : InMemoryStore(hash_branching_factor) {
  // Load all pages from the snapshot.
  auto num_pages = snapshot.GetNumPages();
  for (std::size_t i = 0; i < num_pages; i++) {
    pages_->emplace_back();
    auto dest = pages_->back().AsBytes();
    auto src = snapshot.GetPageData(i);
    std::memcpy(dest.data(), src.data(), dest.size());
    hashes_.MarkDirty(i);
  }
  // Refresh the hashes.
  hashes_.GetHash();
}

template <typename K, Trivial V, std::size_t page_size>
std::unique_ptr<StoreSnapshot> InMemoryStore<K, V, page_size>::CreateSnapshot()
    const {
  return std::make_unique<DeepSnapshot>(*pages_);
}

}  // namespace carmen::backend::store
