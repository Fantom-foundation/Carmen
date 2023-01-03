#pragma once

#include <concepts>
#include <filesystem>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/store/hash_tree.h"
#include "backend/structure.h"
#include "common/hash.h"
#include "common/memory_usage.h"
#include "common/type.h"

namespace carmen::backend::depot {

// In memory implementation of a Depot.
template <std::integral K>
class InMemoryDepot {
 public:
  // The type of the depot key.
  using key_type = K;

  // A factory function creating an instance of this depot type.
  static absl::StatusOr<InMemoryDepot> Open(Context&,
                                            const std::filesystem::path&) {
    return InMemoryDepot();
  }

  // Creates a new InMemoryDepot using the provided branching factor and
  // number of items per group for hash computation.
  InMemoryDepot(std::size_t hash_branching_factor = 32,
                std::size_t hash_box_size = 4)
      : hash_box_size_(hash_box_size),
        items_(std::make_unique<Items>()),
        hashes_(std::make_unique<PageProvider>(*items_, hash_box_size),
                hash_branching_factor) {}

  // Updates the value associated to the given key. The value is copied
  // into the depot.
  absl::Status Set(const K& key, std::span<const std::byte> data) {
    if (key >= items_->size()) {
      items_->resize(key + 1);
    }
    (*items_)[key] = Item{data.begin(), data.end()};
    hashes_.MarkDirty(GetBoxHashGroup(key));
    return absl::OkStatus();
  }

  // Retrieves the value associated to the given key. If no values has
  // been previously set using the Set(..) function above, not found status
  // is returned.
  absl::StatusOr<std::span<const std::byte>> Get(const K& key) const {
    if (key >= items_->size()) {
      return absl::NotFoundError("Key not found");
    }
    return (*items_)[key];
  }

  // Retrieves the size of data associated to the given key. If no values has
  // been previously set using the Set(..) function above, not found status
  // is returned.
  absl::StatusOr<std::uint32_t> GetSize(const K& key) const {
    if (key >= items_->size()) {
      return absl::NotFoundError("Key not found");
    }
    return (*items_)[key].size();
  }

  // Computes a hash over the full content of this depot.
  absl::StatusOr<Hash> GetHash() const { return hashes_.GetHash(); }

  // Ignored, since depot is not backed by disk storage.
  absl::Status Flush() { return absl::OkStatus(); }

  // Ignored, since depot does not maintain any resources.
  absl::Status Close() { return absl::OkStatus(); }

  // Summarizes the memory usage of this instance.
  MemoryFootprint GetMemoryFootprint() const {
    MemoryFootprint res(*this);
    Memory sum;
    for (const auto& box : *items_) {
      sum += Memory(box.size());
    }
    res.Add("items", sum);
    res.Add("hashes", hashes_.GetMemoryFootprint());
    return res;
  }

 private:
  using Item = std::vector<std::byte>;
  using Items = std::deque<Item>;
  using ItemLength = std::uint32_t;

  // Get hash group for the given key.
  std::size_t GetBoxHashGroup(const K& key) const {
    return key / hash_box_size_;
  }

  // A page source providing the owned hash tree access to the stored pages.
  class PageProvider : public store::PageSource {
   public:
    explicit PageProvider(Items& items, std::size_t hash_box_size)
        : items_(items), hash_box_size_(hash_box_size) {}

    // Get data for given page. The data is valid until the next call to
    // this function.
    std::span<const std::byte> GetPageData(PageId id) override {
      static const auto empty = Item{};
      const std::size_t lengths_size = hash_box_size_ * sizeof(ItemLength);

      // calculate start and end of the hash group
      auto start = items_.begin() + id * hash_box_size_;
      auto end = items_.begin() +
                 std::min(id * hash_box_size_ + hash_box_size_, items_.size());

      // calculate the size of the hash group
      std::size_t len = lengths_size;
      for (auto it = start; it != end; ++it) {
        len += it->size();
      }

      page_data_.resize(len);

      // set lengths to zero default value
      std::fill_n(page_data_.begin(), lengths_size, std::byte{0});

      std::size_t pos = lengths_size;
      for (auto it = start; it != end; ++it) {
        if (it->empty()) continue;
        // set the length of the item
        reinterpret_cast<ItemLength*>(page_data_.data())[it - start] =
            it->size();
        // copy the item
        std::memcpy(page_data_.data() + pos, it->data(), it->size());
        pos += it->size();
      }

      return page_data_;
    }

   private:
    Items& items_;
    std::size_t hash_box_size_;
    std::vector<std::byte> page_data_;
  };

  // The amount of items that will be grouped into a single hashing group.
  const std::size_t hash_box_size_;

  // An indexed list of items containing the actual values. The container is
  // wrapped in a unique pointer to facilitate pointer stability under move.
  std::unique_ptr<Items> items_;

  // The data structure managing the hashing of states.
  mutable store::HashTree hashes_;
};

}  // namespace carmen::backend::depot
