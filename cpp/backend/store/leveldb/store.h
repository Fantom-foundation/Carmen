#pragma once

#include <concepts>
#include <cstring>
#include <filesystem>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/common/leveldb/level_db.h"
#include "backend/store/hash_tree.h"
#include "backend/store/store.h"
#include "common/byte_util.h"
#include "common/hash.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen::backend::store {

// The LevelDBStore is a leveldb implementation of a mutable key/value
// store. It maps provided mutation and lookup support, as well as global
// state hashing support enabling to obtain a quick hash for the entire
// content.
template <std::integral K, Trivial V, std::size_t page_size = 32>
class LevelDBStore {
 public:
  // The page size in byte used by this store.
  constexpr static std::size_t kPageSize = page_size;

  // Open connection to the store. If the store does not exist, it will be
  // created. If the depot store, it will be opened.
  static absl::StatusOr<LevelDBStore> Open(
      const std::filesystem::path& path,
      std::size_t hash_branching_factor = 32) {
    auto is_new =
        !std::filesystem::exists(path) || std::filesystem::is_empty(path);
    ASSIGN_OR_RETURN(auto db, LevelDB::Open(path, /*create_if_missing=*/true));
    auto store = LevelDBStore(std::move(db), hash_branching_factor);

    if (!is_new) {
      RETURN_IF_ERROR(store.hashes_.LoadFromLevelDB(*store.db_));
    }

    return store;
  }

  // Updates the value associated to the given key.
  absl::Status Set(const K& key, V value) {
    RETURN_IF_ERROR(db_->Add({AsChars(key), AsChars(value)}));
    hashes_.MarkDirty(GetPageId(key));
    return absl::OkStatus();
  }

  // Retrieves the value associated to the given key. If no values has
  // been previously set using the Set(..) function above, abseil not found
  // error is returned.
  absl::StatusOr<V> Get(const K& key) const {
    hashes_.RegisterPage(GetPageId(key));
    ASSIGN_OR_RETURN(auto result, db_->Get(AsChars(key)));
    return FromChars<V>(result);
  }

  // Computes a hash over the full content of this store.
  absl::StatusOr<Hash> GetHash() const { return hashes_.GetHash(); }

  // Flush all pending changes to disk.
  absl::Status Flush() { return hashes_.SaveToLevelDB(*db_); }

  // Close the store.
  absl::Status Close() {
    RETURN_IF_ERROR(Flush());
    return absl::OkStatus();
  }

 private:
  constexpr static auto elements_per_page = kPageSize / sizeof(V);

  // Creates a new LevelDBStore using the leveldb instance and provided value
  // as the branching factor for hash computation.
  LevelDBStore(LevelDB db, std::size_t hash_branching_factor)
      : db_(std::make_unique<LevelDB>(std::move(db))),
        hashes_(std::make_unique<PageProvider>(*db_), hash_branching_factor) {}

  // Get the page id for a given key.
  PageId GetPageId(const K& key) const { return (key / elements_per_page); }

  // A page source providing the owned hash tree access to the stored pages.
  class PageProvider : public PageSource {
   public:
    PageProvider(LevelDB& db) : db_(db) {}

    // Get data for given page. The data is valid until the next call to
    // this function.
    std::span<const std::byte> GetPageData(PageId id) override {
      K start = id * elements_per_page;
      K end = start + elements_per_page - 1;

      if (start > end) return {};

      static auto empty = std::array<std::byte, sizeof(V)>{};
      for (K i = start; i <= end; i++) {
        auto res = db_.Get(AsChars(i));
        auto position = (i % elements_per_page) * sizeof(V);
        if (!res.ok()) {
          std::memcpy(page_buffer_.data() + position, empty.data(), sizeof(V));
          continue;
        }
        std::memcpy(page_buffer_.data() + position, (*res).data(), sizeof(V));
      }

      return page_buffer_;
    }

   private:
    const LevelDB& db_;
    std::array<std::byte, kPageSize> page_buffer_;
  };

  // The underlying LevelDB instance. Wrapped in a unique_ptr to allow
  // for move semantics.
  std::unique_ptr<LevelDB> db_;

  // The data structure hanaging the hashing of states.
  mutable HashTree hashes_;
};
}  // namespace carmen::backend::store
