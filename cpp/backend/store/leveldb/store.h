#pragma once

#include <concepts>
#include <cstring>
#include <filesystem>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/common/leveldb/leveldb.h"
#include "backend/store/hash_tree.h"
#include "backend/store/store.h"
#include "common/byte_util.h"
#include "common/hash.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen::backend::store {

// The LevelDbStore is a leveldb implementation of a mutable key/value
// store. It maps provided mutation and lookup support, as well as global
// state hashing support enabling to obtain a quick hash for the entire
// content.
template <std::integral K, Trivial V, std::size_t page_size = 32>
class LevelDbStore {
 public:
  // The value type used to index elements in this store.
  using key_type = K;

  // The type of value stored in this store.
  using value_type = V;

  // The page size in byte used by this store.
  constexpr static std::size_t kPageSize = page_size;

  // Open connection to the store. If the store does not exist, it will be
  // created. If the depot store, it will be opened.
  static absl::StatusOr<LevelDbStore> Open(
      Context&, const std::filesystem::path& path,
      std::size_t hash_branching_factor = 32) {
    auto is_new =
        !std::filesystem::exists(path) || std::filesystem::is_empty(path);
    ASSIGN_OR_RETURN(auto db, LevelDb::Open(path, /*create_if_missing=*/true));
    auto store = LevelDbStore(std::move(db), hash_branching_factor);

    if (!is_new) {
      RETURN_IF_ERROR(store.hashes_.LoadFromLevelDb(*store.db_));
    }

    return store;
  }

  // Supports instances to be moved.
  LevelDbStore(LevelDbStore&&) noexcept = default;

  // Store is closed when the instance is destroyed.
  ~LevelDbStore() { Close().IgnoreError(); }

  // Updates the value associated to the given key.
  absl::Status Set(const K& key, V value) {
    RETURN_IF_ERROR(db_->Add({AsChars(key), AsChars(value)}));
    hashes_.MarkDirty(GetPageId(key));
    return absl::OkStatus();
  }

  // Retrieves the value associated to the given key. If no values has
  // been previously set using the Set(..) function above, default value
  // is returned.
  absl::StatusOr<V> Get(const K& key) const {
    constexpr static const V default_value{};
    auto buffer = db_->Get(AsChars(key));
    if (absl::IsNotFound(buffer.status())) {
      return default_value;
    }
    RETURN_IF_ERROR(buffer.status());
    ASSIGN_OR_RETURN(V result, FromChars<V>(*buffer));
    return result;
  }

  // Computes a hash over the full content of this store.
  absl::StatusOr<Hash> GetHash() const { return hashes_.GetHash(); }

  // Flush all pending changes to disk.
  absl::Status Flush() {
    if (db_ && db_->IsOpen()) {
      RETURN_IF_ERROR(db_->Flush());
      RETURN_IF_ERROR(hashes_.SaveToLevelDb(*db_));
    }
    return absl::OkStatus();
  }

  // Close the store.
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
    res.Add("hashes", hashes_.GetMemoryFootprint());
    return res;
  }

 private:
  constexpr static auto elements_per_page = kPageSize / sizeof(V);
  // elements per page has to be greater than 0
  static_assert(elements_per_page > 0);

  // Creates a new LevelDbStore using the leveldb instance and provided value
  // as the branching factor for hash computation.
  LevelDbStore(LevelDb db, std::size_t hash_branching_factor)
      : db_(std::make_unique<LevelDb>(std::move(db))),
        hashes_(std::make_unique<PageProvider>(*db_), hash_branching_factor) {}

  // Get the page id for a given key.
  PageId GetPageId(const K& key) const { return (key / elements_per_page); }

  // A page source providing the owned hash tree access to the stored pages.
  class PageProvider : public PageSource {
   public:
    PageProvider(LevelDb& db) : db_(db) {}

    // Get data for given page. The data is valid until the next call to
    // this function.
    absl::StatusOr<std::span<const std::byte>> GetPageData(PageId id) override {
      K start = id * elements_per_page;
      K end = start + elements_per_page;

      static auto empty = std::array<std::byte, sizeof(V)>{};
      std::size_t offset = 0;
      for (K i = start; i < end; i++) {
        auto res = db_.Get(AsChars(i));
        switch (res.status().code()) {
          case absl::StatusCode::kOk:
            std::memcpy(page_buffer_.data() + offset, res->data(), sizeof(V));
            break;
          case absl::StatusCode::kNotFound:
            std::memcpy(page_buffer_.data() + offset, empty.data(), sizeof(V));
            break;
          default:
            return res.status();
        }
        offset += sizeof(V);
      }

      return page_buffer_;
    }

   private:
    const LevelDb& db_;
    std::array<std::byte, elements_per_page * sizeof(V)> page_buffer_;
  };

  // The underlying LevelDb instance. Wrapped in a unique_ptr to allow
  // for move semantics.
  std::unique_ptr<LevelDb> db_;

  // The data structure managing the hashing of states.
  mutable HashTree hashes_;
};
}  // namespace carmen::backend::store
