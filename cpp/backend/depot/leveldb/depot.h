#pragma once

#include <concepts>
#include <filesystem>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/common/leveldb/level_db.h"
#include "backend/store/hash_tree.h"
#include "common/byte_util.h"
#include "common/hash.h"
#include "common/memory_usage.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen::backend::depot {

// LevelDB implementation of a Depot.
template <std::integral K>
class LevelDBDepot {
 public:
  // The type of the depot key.
  using key_type = K;

  // Open connection to the depot. If the depot does not exist, it will be
  // created. If the depot exists, it will be opened.
  static absl::StatusOr<LevelDBDepot> Open(
      const std::filesystem::path& path, std::size_t hash_branching_factor = 32,
      std::size_t num_hash_boxes = 4) {
    auto is_new =
        !std::filesystem::exists(path) || std::filesystem::is_empty(path);
    ASSIGN_OR_RETURN(auto db, LevelDB::Open(path, /*create_if_missing=*/true));
    auto depot =
        LevelDBDepot(std::move(db), hash_branching_factor, num_hash_boxes);

    if (!is_new) {
      RETURN_IF_ERROR(depot.hashes_.LoadFromLevelDB(*depot.db_));
    }

    return depot;
  }

  // Updates the value associated to the given key. The value is copied
  // into the depot.
  absl::Status Set(const K& key, std::span<const std::byte> data) {
    RETURN_IF_ERROR(db_->Add({AsChars(key), AsChars(data)}));
    hashes_.MarkDirty(GetBoxHashGroup(key));
    return absl::OkStatus();
  }

  // Retrieves the value associated to the given key. The data is valid
  // until the next call to this function.
  absl::StatusOr<std::span<const std::byte>> Get(const K& key) const {
    ASSIGN_OR_RETURN(auto value, db_->Get(AsChars(key)));
    get_data_.resize(value.size());
    std::memcpy(get_data_.data(), value.data(), value.size());
    return std::span{get_data_.data(), value.size()};
  }

  // Computes a hash over the full content of this depot.
  absl::StatusOr<Hash> GetHash() const { return hashes_.GetHash(); }

  // Flush all pending changes to disk.
  absl::Status Flush() { return hashes_.SaveToLevelDB(*db_); }

  // Close the depot.
  absl::Status Close() {
    RETURN_IF_ERROR(Flush());
    return absl::OkStatus();
  }

  // Summarizes the memory usage of this instance.
  MemoryFootprint GetMemoryFootprint() const {
    MemoryFootprint res(*this);
    res.Add("db", db_->GetMemoryFootprint());
    res.Add("hashes", hashes_);
    res.Add("buffer", SizeOf(get_data_));
    return res;
  }

 private:
  // Creates a new LevelDBDepot using the provided leveldb path, branching
  // factor and number of boxes per group for hash computation.
  LevelDBDepot(LevelDB level_db, std::size_t hash_branching_factor,
               std::size_t num_hash_boxes)
      : db_(std::make_unique<LevelDB>(std::move(level_db))),
        num_hash_boxes_(num_hash_boxes),
        hashes_(std::make_unique<PageProvider>(num_hash_boxes, *db_),
                hash_branching_factor) {}

  // Get hash group for the given key.
  std::size_t GetBoxHashGroup(const K& key) const {
    return key / num_hash_boxes_;
  }

  // A page source providing the owned hash tree access to the stored pages.
  class PageProvider : public store::PageSource {
   public:
    PageProvider(std::size_t num_hash_boxes, const LevelDB& db)
        : db_(db), num_hash_boxes_(num_hash_boxes) {}

    // Get data for given page. The data is valid until the next call to
    // this function.
    std::span<const std::byte> GetPageData(PageId id) override {
      static auto empty = std::array<std::byte, 0>{};
      auto start = id * num_hash_boxes_;
      auto end = start + num_hash_boxes_ - 1;

      if (start > end) return empty;

      std::size_t size = 0;
      for (K i = start; i <= end; ++i) {
        auto result = db_.Get(AsChars(i));
        switch (result.status().code()) {
          case absl::StatusCode::kOk:
            page_data_.resize(size + result->size());
            std::memcpy(page_data_.data() + size, (*result).data(),
                        (*result).size());
            size += result->size();
            break;
          case absl::StatusCode::kNotFound:
            break;
          default:
            return empty;
        }
      }

      return {page_data_.data(), size};
    }

   private:
    const LevelDB& db_;
    std::size_t num_hash_boxes_;
    std::vector<std::byte> page_data_;
  };

  // The underlying LevelDB instance.
  std::unique_ptr<LevelDB> db_;

  // The amount of boxes that will be grouped into a single hashing group.
  const std::size_t num_hash_boxes_;

  // The data structure managing the hashing of states.
  mutable store::HashTree hashes_;

  // Temporary storage for the result of Get().
  mutable std::vector<std::byte> get_data_;
};

}  // namespace carmen::backend::depot
