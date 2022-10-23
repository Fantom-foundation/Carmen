#pragma once

#include "backend/index/index.h"
#include "backend/index/leveldb/multi_file/index.h"
#include "common/type.h"

namespace carmen::backend::index {
// LevelDBIndexTestAdapter is a wrapper around LevelDBIndex. It exposes
// LevelDBIndex methods to be compatible with tests.
template <Trivial K, std::integral I>
class LevelDBIndexTestAdapter {
 public:
  using key_type [[maybe_unused]] = K;
  using value_type [[maybe_unused]] = I;

  explicit LevelDBIndexTestAdapter(LevelDBIndex<K, I> index)
      : index_(std::move(index)) {}

  std::pair<I, bool> GetOrAdd(const K& key) {
    auto result = index_.GetOrAdd(key);
    if (result.ok()) return *result;
    // no way to handle error
    return {0, false};
  }

  std::optional<I> Get(const K& key) const {
    auto result = index_.Get(key);
    if (result.ok()) return *result;
    return std::nullopt;
  }

  Hash GetHash() {
    auto result = index_.GetHash();
    if (result.ok()) return *result;
    return Hash{};
  }

 private:
  LevelDBIndex<K, I> index_;
};
}  // namespace carmen::backend::index
