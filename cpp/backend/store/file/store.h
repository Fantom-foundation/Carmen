#pragma once

#include "backend/store/file/file.h"
#include "backend/store/file/page_pool.h"
#include "common/hash.h"
#include "common/type.h"

namespace carmen::backend::store {

// The FileStore is a file-backed implementation of a mutable key/value store.
// It provides mutation, lookup, and global state hashing support.
template <typename K, Trivial V, template <std::size_t> class F,
          std::size_t page_size = 32>
requires File<F<page_size>, page_size>
class FileStore {
 public:
  // Creates a new FileStore using the provided value as the
  // default value for all its storage cells. Any get for an uninitialized
  // key will return the provided default value.
  FileStore() {}

  // Updates the value associated to the given key.
  void Set(const K& key, V value);

  // Retrieves the value associated to the given key. If no values has
  // been previously set using a the Set(..) function above, a zero-initialized
  // value is returned. The returned reference is only valued until the next
  // operation on the store.
  const V& Get(const K& key) const;

  // Computes a hash over the full content of this store.
  Hash GetHash() const;

 private:
  using PagePool = PagePool<V, F, page_size>;

  // The number of elements per page, used for page and offset computaiton.
  constexpr static std::size_t kNumElementsPerPage =
      PagePool::Page::kNumElementsPerPage;

  // The page pool handling the in-memory buffer of pages fetched from disk.
  mutable PagePool pool_;
};

template <typename K, Trivial V, template <std::size_t> class F,
          std::size_t page_size>
requires File<F<page_size>, page_size>
void FileStore<K, V, F, page_size>::Set(const K& key, V value) {
  auto& trg = pool_.Get(key / kNumElementsPerPage)[key % kNumElementsPerPage];
  if (trg != value) {
    trg = value;
    pool_.MarkAsDirty(key / kNumElementsPerPage);
  }
}

template <typename K, Trivial V, template <std::size_t> class F,
          std::size_t page_size>
requires File<F<page_size>, page_size>
const V& FileStore<K, V, F, page_size>::Get(const K& key) const {
  return pool_.Get(key / kNumElementsPerPage)[key % kNumElementsPerPage];
}

}  // namespace carmen::backend::store
