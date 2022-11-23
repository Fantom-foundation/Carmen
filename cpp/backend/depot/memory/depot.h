#pragma once

#include <concepts>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/store/hash_tree.h"
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

  // Creates a new InMemoryDepot using the provided branching factor and
  // number of boxes per group for hash computation.
  InMemoryDepot(std::size_t hash_branching_factor = 32,
                std::size_t num_hash_boxes = 4)
      : num_hash_boxes_(num_hash_boxes),
        boxes_(std::make_unique<Boxes>()),
        hashes_(std::make_unique<PageProvider>(*boxes_, num_hash_boxes),
                hash_branching_factor) {}

  // Updates the value associated to the given key. The value is copied
  // into the depot.
  absl::Status Set(const K& key, std::span<const std::byte> data) {
    if (key >= boxes_->size()) {
      boxes_->resize(key + 1);
    }
    (*boxes_)[key] = Box{data.begin(), data.end()};
    hashes_.MarkDirty(GetBoxHashGroup(key));
    return absl::OkStatus();
  }

  // Retrieves the value associated to the given key. If no values has
  // been previously set using the Set(..) function above, not found status
  // is returned.
  absl::StatusOr<std::span<const std::byte>> Get(const K& key) const {
    if (key >= boxes_->size()) {
      return absl::NotFoundError("Key not found");
    }
    return (*boxes_)[key];
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
    for (const auto& box : *boxes_) {
      sum += Memory(box.size());
    }
    res.Add("boxes", sum);
    res.Add("hashes", hashes_.GetMemoryFootprint());
    return res;
  }

 private:
  using Box = std::vector<std::byte>;
  using Boxes = std::deque<Box>;

  // Get hash group for the given key.
  std::size_t GetBoxHashGroup(const K& key) const {
    return key / num_hash_boxes_;
  }

  // A page source providing the owned hash tree access to the stored pages.
  class PageProvider : public store::PageSource {
   public:
    explicit PageProvider(Boxes& boxes, std::size_t num_hash_boxes)
        : boxes_(boxes), num_hash_boxes_(num_hash_boxes) {}

    // Get data for given page. The data is valid until the next call to
    // this function.
    std::span<const std::byte> GetPageData(PageId id) override {
      static auto empty = Box{};
      // calculate start and end of the hash group
      auto start = boxes_.begin() + id * num_hash_boxes_;
      auto end =
          boxes_.begin() +
          std::min(id * num_hash_boxes_ + num_hash_boxes_, boxes_.size());

      if (start >= end) return empty;

      // calculate the size of the hash group
      std::size_t len = 0;
      for (auto it = start; it != end; ++it) {
        len += it->size();
      }

      page_data_.resize(len);
      std::size_t pos = 0;
      for (auto it = start; it != end; ++it) {
        if (it->empty()) continue;
        std::memcpy(page_data_.data() + pos, it->data(), it->size());
        pos += it->size();
      }

      return {page_data_.data(), len};
    }

   private:
    Boxes& boxes_;
    std::size_t num_hash_boxes_;
    std::vector<std::byte> page_data_;
  };

  // The amount of boxes that will be grouped into a single hashing group.
  const std::size_t num_hash_boxes_;

  // An indexed list of boxes containing the actual values. The container is
  // wrapped in a unique pointer to facilitate pointer stability under move.
  std::unique_ptr<Boxes> boxes_;

  // The data structure managing the hashing of states.
  mutable store::HashTree hashes_;
};

}  // namespace carmen::backend::depot
