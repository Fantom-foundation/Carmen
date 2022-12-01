#pragma once

#include "absl/status/statusor.h"
#include "backend/store/leveldb/store.h"
#include "backend/store/store.h"
#include "common/hash.h"
#include "common/memory_usage.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen::backend::store {
// LevelDbStoreTestAdapter is a wrapper around LevelDbStore. Providing
// interface for benchmarking and testing. This is subject to be removed
// once we have store interface updated.
template <std::integral K, Trivial V, std::size_t page_size>
class LevelDbStoreTestAdapter {
 public:
  // The value type used to index elements in this store.
  using key_type = K;

  // The type of value stored in this store.
  using value_type = V;

  static absl::StatusOr<LevelDbStoreTestAdapter> Open(
      Context& context, const std::filesystem::path& path) {
    ASSIGN_OR_RETURN(auto store,
                     (LevelDbStore<K, V, page_size>::Open(context, path)));
    return LevelDbStoreTestAdapter(std::move(store));
  }

  LevelDbStoreTestAdapter(LevelDbStore<K, V, page_size> store)
      : store_(std::move(store)) {}

  void Set(const K& key, V value) { store_.Set(key, value).IgnoreError(); }

  const V& Get(const K& key) const {
    static auto empty = V{};
    auto res = store_.Get(key);
    if (!res.ok()) return empty;
    temp_value_ = *res;
    return temp_value_;
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
  LevelDbStore<K, V, page_size> store_;
  mutable V temp_value_;
};
}  // namespace carmen::backend::store
