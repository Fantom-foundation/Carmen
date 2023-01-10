#pragma once

#include <filesystem>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "backend/common/file.h"
#include "backend/common/page.h"
#include "backend/common/page_pool.h"
#include "backend/store/hash_tree.h"
#include "backend/structure.h"
#include "common/hash.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen::backend::store {

// ----------------------------------------------------------------------------
//                              Declarations
// ----------------------------------------------------------------------------

namespace internal {

// The FileStoreBase is the common bases of file-backed implementations of a
// mutable key/value store. It provides mutation, lookup, and global state
// hashing support. Hashing can occure eager (before evicting pages) or lazy,
// when requesting hash computations.
template <typename K, Trivial V, template <typename> class F,
          std::size_t page_size = 32, bool eager_hashing = true>
requires File<F<ArrayPage<V, page_size>>>
class FileStoreBase;

}  // namespace internal

// A FileStore implementation configured to perform eager hashing. Thus,
// before pages are evicted, hashes are computed. This slows down reads
// and updates, but improves hashing speed.
template <typename K, Trivial V, template <typename> class F,
          std::size_t page_size = 32>
requires File<F<ArrayPage<V, page_size>>>
using EagerFileStore = internal::FileStoreBase<K, V, F, page_size, true>;

// A FileStore implementation configured to perform lazy hashing. Thus,
// pages are evicted without being hashes and need to be reloaded for computing
// hashes when needed. This speeds up read/write operations at the expense of
// hash performance.
template <typename K, Trivial V, template <typename> class F,
          std::size_t page_size = 32>
requires File<F<ArrayPage<V, page_size>>>
using LazyFileStore = internal::FileStoreBase<K, V, F, page_size, false>;

// ----------------------------------------------------------------------------
//                              Definitions
// ----------------------------------------------------------------------------

namespace internal {

template <typename K, Trivial V, template <typename> class F,
          std::size_t page_size, bool eager_hashing>
requires File<F<ArrayPage<V, page_size>>>
class FileStoreBase {
 public:
  // The value type used to index elements in this store.
  using key_type = K;

  // The type of value stored in this store.
  using value_type = V;

  // The page size in byte used by this store.
  constexpr static std::size_t kPageSize = page_size;

  // The page type used by this store.
  using page_type = ArrayPage<V, page_size>;

  // A factory function creating an instance of this store type.
  static absl::StatusOr<FileStoreBase> Open(
      Context&, const std::filesystem::path& directory,
      std::size_t hash_branching_factor = 32) {
    // Make sure the directory exists.
    RETURN_IF_ERROR(CreateDirectory(directory));
    auto store = FileStoreBase(directory, hash_branching_factor);
    if (std::filesystem::exists(store.hash_file_)) {
      RETURN_IF_ERROR(store.hashes_->LoadFromFile(store.hash_file_));
    }
    return store;
  }

  // Supports instances to be moved.
  FileStoreBase(FileStoreBase&&) = default;

  // File stores are automatically closed on destruction.
  ~FileStoreBase() { Close().IgnoreError(); }

  // Updates the value associated to the given key.
  absl::Status Set(const K& key, V value);

  // Retrieves the value associated to the given key. If no values has
  // been previously set using the Set(..) function above, a zero-initialized
  // value is returned. The returned reference is only valid until the next
  // operation on the store.
  StatusOrRef<const V> Get(const K& key) const;

  // Computes a hash over the full content of this store.
  absl::StatusOr<Hash> GetHash() const;

  // Flushes internally buffered modified data to disk.
  absl::Status Flush();

  // Flushes the store and closes resource references.
  absl::Status Close();

  // Summarizes the memory usage of this instance.
  MemoryFootprint GetMemoryFootprint() const;

 private:
  using Page = ArrayPage<V, page_size>;
  using PagePool = PagePool<Page, F>;

  // A listener to pool activities to react to loaded and evicted pages and
  // perform necessary hashing steps.
  class PoolListener : public PagePoolListener<Page> {
   public:
    PoolListener(HashTree& hashes) : hashes_(hashes) {}

    void AfterLoad(PageId id, const Page&) override {
      // When a page is loaded, make sure the HashTree is aware of it.
      hashes_.RegisterPage(id);
    }

    void BeforeEvict(PageId id, const Page& page, bool is_dirty) override {
      // Before we throw away a dirty page to make space for something else we
      // update the hash to avoid having to reload it again later.
      if (eager_hashing && is_dirty) {
        hashes_.UpdateHash(id, std::as_bytes(std::span(page.AsArray())));
      }
    }

   private:
    HashTree& hashes_;
  };

  // An implementation of a PageSource passed to the HashTree to provide access
  // to pages through the page pool, and thus through its caching authority.
  class PageProvider : public PageSource {
   public:
    PageProvider(PagePool& pool) : pool_(pool) {}

    std::span<const std::byte> GetPageData(PageId id) override {
      constexpr std::span<const std::byte> const kEmpty;
      auto page = pool_.Get(id);
      if (!page.ok()) {
        return kEmpty;
      }
      return std::as_bytes(std::span(page->get().AsArray()));
    }

   private:
    PagePool& pool_;
  };

  // The number of elements per page, used for page and offset computation.
  constexpr static std::size_t kNumElementsPerPage =
      PagePool::Page::kNumElementsPerPage;

  // Creates a new file store maintaining its content in the given directory and
  // using the provided branching factor for its hash computation.
  FileStoreBase(std::filesystem::path directory,
                std::size_t hash_branching_factor);

  // The page pool handling the in-memory buffer of pages fetched from disk. The
  // pool is placed in a unique pointer to ensure pointer stability when the
  // store is moved.
  mutable std::unique_ptr<PagePool> pool_;

  // The data structure hanaging the hashing of states. The hashes are placed in
  // a unique pointer to ensure pointer stability when the store is moved.
  mutable std::unique_ptr<HashTree> hashes_;

  // The name of the file to safe hashes to.
  std::filesystem::path hash_file_;
};

template <typename K, Trivial V, template <typename> class F,
          std::size_t page_size, bool eager_hashing>
requires File<F<ArrayPage<V, page_size>>>
FileStoreBase<K, V, F, page_size, eager_hashing>::FileStoreBase(
    std::filesystem::path directory, std::size_t hash_branching_factor)
    : pool_(std::make_unique<PagePool>(
          std::make_unique<F<page_type>>(directory / "data.dat"))),
      hashes_(std::make_unique<HashTree>(std::make_unique<PageProvider>(*pool_),
                                         hash_branching_factor)),
      hash_file_(directory / "hash.dat") {
  pool_->AddListener(std::make_unique<PoolListener>(*hashes_));
}

template <typename K, Trivial V, template <typename> class F,
          std::size_t page_size, bool eager_hashing>
requires File<F<ArrayPage<V, page_size>>> absl::Status
FileStoreBase<K, V, F, page_size, eager_hashing>::Set(const K& key, V value) {
  ASSIGN_OR_RETURN(auto page, pool_->Get(key / kNumElementsPerPage));
  auto& trg = page.AsReference()[key % kNumElementsPerPage];
  if (trg != value) {
    trg = value;
    pool_->MarkAsDirty(key / kNumElementsPerPage);
    hashes_->MarkDirty(key / kNumElementsPerPage);
  }
  return absl::OkStatus();
}

template <typename K, Trivial V, template <typename> class F,
          std::size_t page_size, bool eager_hashing>
requires File<F<ArrayPage<V, page_size>>> StatusOrRef<const V>
FileStoreBase<K, V, F, page_size, eager_hashing>::Get(const K& key)
const {
  ASSIGN_OR_RETURN(auto page, pool_->Get(key / kNumElementsPerPage));
  return page.AsReference()[key % kNumElementsPerPage];
}

template <typename K, Trivial V, template <typename> class F,
          std::size_t page_size, bool eager_hashing>
requires File<F<ArrayPage<V, page_size>>> absl::StatusOr<Hash>
FileStoreBase<K, V, F, page_size, eager_hashing>::GetHash()
const { return hashes_->GetHash(); }

template <typename K, Trivial V, template <typename> class F,
          std::size_t page_size, bool eager_hashing>
requires File<F<ArrayPage<V, page_size>>> absl::Status
FileStoreBase<K, V, F, page_size, eager_hashing>::Flush() {
  if (pool_) {
    RETURN_IF_ERROR(pool_->Flush());
  }
  if (hashes_) {
    RETURN_IF_ERROR(hashes_->SaveToFile(hash_file_));
  }
  return absl::OkStatus();
}

template <typename K, Trivial V, template <typename> class F,
          std::size_t page_size, bool eager_hashing>
requires File<F<ArrayPage<V, page_size>>> absl::Status
FileStoreBase<K, V, F, page_size, eager_hashing>::Close() {
  RETURN_IF_ERROR(Flush());
  if (pool_) {
    RETURN_IF_ERROR(pool_->Close());
  }
  return absl::OkStatus();
}

template <typename K, Trivial V, template <typename> class F,
          std::size_t page_size, bool eager_hashing>
requires File<F<ArrayPage<V, page_size>>> MemoryFootprint
FileStoreBase<K, V, F, page_size, eager_hashing>::GetMemoryFootprint()
const {
  MemoryFootprint res(*this);
  res.Add("pool", pool_->GetMemoryFootprint());
  res.Add("hashes", hashes_->GetMemoryFootprint());
  return res;
}

}  // namespace internal
}  // namespace carmen::backend::store
