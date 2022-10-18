#pragma once

#include "backend/common/file.h"
#include "backend/common/page.h"
#include "backend/common/page_pool.h"
#include "backend/store/file/hash_tree.h"
#include "common/hash.h"
#include "common/type.h"

namespace carmen::backend::store {

// The FileStore is a file-backed implementation of a mutable key/value store.
// It provides mutation, lookup, and global state hashing support.
template <typename K, Trivial V, template <typename> class F,
          std::size_t page_size = 32>
requires File<F<ArrayPage<V, page_size>>>
class FileStore {
 public:
  // The page size in byte used by this store.
  constexpr static std::size_t kPageSize = page_size;

  // The page type used by this store.
  using page_type = ArrayPage<V, page_size>;

  // Creates a new, empty FileStore using the given branching factor for its
  // hash computation.
  FileStore(std::size_t hash_branching_factor = 32);

  // Creates a new FileStore based on the given file.
  FileStore(std::size_t hash_branching_factor,
            std::unique_ptr<F<page_type>> file);

  // Updates the value associated to the given key.
  void Set(const K& key, V value);

  // Retrieves the value associated to the given key. If no values has
  // been previously set using the Set(..) function above, a zero-initialized
  // value is returned. The returned reference is only valid until the next
  // operation on the store.
  const V& Get(const K& key) const;

  // Computes a hash over the full content of this store.
  Hash GetHash() const;

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
      if (is_dirty) {
        hashes_.UpdateHash(id, page.AsRawData());
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
      return pool_.Get(id).AsRawData();
    }

   private:
    PagePool& pool_;
  };

  // The number of elements per page, used for page and offset computaiton.
  constexpr static std::size_t kNumElementsPerPage =
      PagePool::Page::kNumElementsPerPage;

  // The page pool handling the in-memory buffer of pages fetched from disk. The
  // pool is placed in a unique pointer to ensure pointer stability when the
  // store is moved.
  mutable std::unique_ptr<PagePool> pool_;

  // The data structure hanaging the hashing of states. The hashes are placed in
  // a unique pointer to ensure pointer stability when the store is moved.
  mutable std::unique_ptr<HashTree> hashes_;
};

template <typename K, Trivial V, template <typename> class F,
          std::size_t page_size>
requires File<F<ArrayPage<V, page_size>>>
FileStore<K, V, F, page_size>::FileStore(std::size_t hash_branching_factor)
    : FileStore(hash_branching_factor, std::make_unique<F<page_type>>()) {}

template <typename K, Trivial V, template <typename> class F,
          std::size_t page_size>
requires File<F<ArrayPage<V, page_size>>>
FileStore<K, V, F, page_size>::FileStore(std::size_t hash_branching_factor,
                                         std::unique_ptr<F<page_type>> file)
    : pool_(std::make_unique<PagePool>(std::move(file))),
      hashes_(std::make_unique<HashTree>(std::make_unique<PageProvider>(*pool_),
                                         hash_branching_factor)) {
  pool_->AddListener(std::make_unique<PoolListener>(*hashes_));
}

template <typename K, Trivial V, template <typename> class F,
          std::size_t page_size>
requires File<F<ArrayPage<V, page_size>>>
void FileStore<K, V, F, page_size>::Set(const K& key, V value) {
  auto& trg = pool_->Get(key / kNumElementsPerPage)[key % kNumElementsPerPage];
  if (trg != value) {
    trg = value;
    pool_->MarkAsDirty(key / kNumElementsPerPage);
    hashes_->MarkDirty(key / kNumElementsPerPage);
  }
}

template <typename K, Trivial V, template <typename> class F,
          std::size_t page_size>
requires File<F<ArrayPage<V, page_size>>>
const V& FileStore<K, V, F, page_size>::Get(const K& key) const {
  return pool_->Get(key / kNumElementsPerPage)[key % kNumElementsPerPage];
}

template <typename K, Trivial V, template <typename> class F,
          std::size_t page_size>
requires File<F<ArrayPage<V, page_size>>> Hash
FileStore<K, V, F, page_size>::GetHash()
const { return hashes_->GetHash(); }

}  // namespace carmen::backend::store
