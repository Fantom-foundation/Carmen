#pragma once

#include "backend/store/leveldb/store.h"
#include "backend/store/store.h"
#include "common/hash.h"
#include "common/memory_usage.h"
#include "common/type.h"

namespace carmen::backend::store {
// LevelDBStoreTestAdapter is a wrapper around LevelDBStore. Providing
// interface for benchmarking and testing. This is subject to be removed
// once we have store interface updated.
template <std::integral K, Trivial V, std::size_t page_size>
class LevelDBStoreTestAdapter {
 public:
  LevelDBStoreTestAdapter(LevelDBStore<K, V, page_size> store)
      : store_(std::move(store)) {}

  void Set(const K& key, V value) { store_.Set(key, value).IgnoreError(); }

  V Get(const K& key) const {
    static auto empty = V{};
    auto res = store_.Get(key);
    if (!res.ok()) return empty;
    return *res;
  }

  Hash GetHash() const {
    static auto empty = Hash{};
    auto res = store_.GetHash();
    if (!res.ok()) return empty;
    return *res;
  }

  void Flush() { store_.Flush().IgnoreError(); }

  void Close() { store_.Close().IgnoreError(); }

  MemoryFootprint GetMemoryFootprint() const {
    return store_.GetMemoryFootprint();
  }

 private:
  LevelDBStore<K, V, page_size> store_;
};
}  // namespace carmen::backend::store
