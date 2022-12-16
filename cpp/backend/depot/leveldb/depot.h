#pragma once

#include <concepts>
#include <filesystem>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/common/leveldb/leveldb.h"
#include "backend/store/hash_tree.h"
#include "backend/structure.h"
#include "common/byte_util.h"
#include "common/hash.h"
#include "common/memory_usage.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen::backend::depot {

// LevelDb implementation of a Depot.
template <std::integral K>
class LevelDbDepot {
 public:
  // The type of the depot key.
  using key_type = K;

  // Open connection to the depot. If the depot does not exist, it will be
  // created. If the depot exists, it will be opened.
  static absl::StatusOr<LevelDbDepot> Open(Context&,
                                           const std::filesystem::path& path) {
    return Open(path);
  }

  // Open connection to the depot. If the depot does not exist, it will be
  // created. If the depot exists, it will be opened.
  static absl::StatusOr<LevelDbDepot> Open(
      const std::filesystem::path& path, std::size_t hash_branching_factor = 32,
      std::size_t hash_box_size = 4) {
    auto is_new =
        !std::filesystem::exists(path) || std::filesystem::is_empty(path);
    ASSIGN_OR_RETURN(auto db, LevelDb::Open(path, /*create_if_missing=*/true));
    auto depot =
        LevelDbDepot(std::move(db), hash_branching_factor, hash_box_size);

    if (!is_new) {
      RETURN_IF_ERROR(depot.hashes_.LoadFromLevelDb(*depot.db_));
    }

    return depot;
  }

  // Supports instances to be moved.
  LevelDbDepot(LevelDbDepot&&) noexcept = default;

  // Depot is closed when the instance is destroyed.
  ~LevelDbDepot() { Close().IgnoreError(); }

  // Updates the value associated to the given key. The value is copied
  // into the depot.
  absl::Status Set(const K& key, std::span<const std::byte> data) {
    RETURN_IF_ERROR(db_->Add({AsChars(key), AsChars(data)}));
    hashes_.MarkDirty(GetBoxHashGroup(key));
    return absl::OkStatus();
  }

  // Retrieves the value associated to the given key. The data is valid
  // until the next call to this function. If no values has been previously
  // set using the Set(..) function above, not found status is returned.
  absl::StatusOr<std::span<const std::byte>> Get(const K& key) const {
    ASSIGN_OR_RETURN(auto value, db_->Get(AsChars(key)));
    get_data_.resize(value.size());
    std::memcpy(get_data_.data(), value.data(), value.size());
    return std::span{get_data_.data(), value.size()};
  }

  // Retrieves the size of data associated to the given key. If no values has
  // been previously set using the Set(..) function above, not found status
  // is returned.
  absl::StatusOr<std::uint32_t> GetSize(const K& key) const {
    ASSIGN_OR_RETURN(auto value, db_->Get(AsChars(key)));
    return value.size();
  }

  // Computes a hash over the full content of this depot.
  absl::StatusOr<Hash> GetHash() const { return hashes_.GetHash(); }

  // Flush all pending changes to database.
  absl::Status Flush() {
    if (db_ && db_->IsOpen()) {
      RETURN_IF_ERROR(db_->Flush());
      RETURN_IF_ERROR(hashes_.SaveToLevelDb(*db_));
    }
    return absl::OkStatus();
  }

  // Close the depot.
  absl::Status Close() {
    RETURN_IF_ERROR(Flush());
    if (db_ && db_->IsOpen()) {
      RETURN_IF_ERROR(db_->Close());
    }
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
  using ItemLength = std::uint32_t;

  // Creates a new LevelDbDepot using the provided leveldb path, branching
  // factor and number of items per group for hash computation.
  LevelDbDepot(LevelDb leveldb, std::size_t hash_branching_factor,
               std::size_t hash_box_size)
      : db_(std::make_unique<LevelDb>(std::move(leveldb))),
        hash_box_size_(hash_box_size),
        hashes_(std::make_unique<PageProvider>(hash_box_size, *db_),
                hash_branching_factor) {}

  // Get hash group for the given key.
  std::size_t GetBoxHashGroup(const K& key) const {
    return key / hash_box_size_;
  }

  // A page source providing the owned hash tree access to the stored pages.
  class PageProvider : public store::PageSource {
   public:
    PageProvider(std::size_t hash_box_size, const LevelDb& db)
        : db_(db), hash_box_size_(hash_box_size) {}

    // Get data for given page. The data is valid until the next call to
    // this function.
    std::span<const std::byte> GetPageData(PageId id) override {
      static auto empty = std::array<std::byte, 0>{};
      const std::size_t lengths_size = hash_box_size_ * sizeof(ItemLength);

      auto start = id * hash_box_size_;
      auto end = start + hash_box_size_ - 1;

      if (start > end) return empty;

      // set lengths to zero default value
      if (page_data_.size() < lengths_size) {
        page_data_.resize(lengths_size);
      }
      std::memset(page_data_.data(), 0, lengths_size);

      std::size_t size = lengths_size;
      for (K i = start; i <= end; ++i) {
        auto result = db_.Get(AsChars(i));
        switch (result.status().code()) {
          case absl::StatusCode::kOk:
            page_data_.resize(size + result->size());
            // set length of item
            reinterpret_cast<ItemLength*>(page_data_.data())[i - start] =
                result->size();
            // copy item data
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
    const LevelDb& db_;
    std::size_t hash_box_size_;
    std::vector<std::byte> page_data_;
  };

  // The underlying LevelDb instance.
  std::unique_ptr<LevelDb> db_;

  // The amount of items that will be grouped into a single hashing group.
  const std::size_t hash_box_size_;

  // The data structure managing the hashing of states.
  mutable store::HashTree hashes_;

  // Temporary storage for the result of Get().
  mutable std::vector<std::byte> get_data_;
};

}  // namespace carmen::backend::depot
