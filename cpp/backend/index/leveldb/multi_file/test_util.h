#pragma once

#include "backend/index/index.h"
#include "backend/index/leveldb/multi_file/index.h"
#include "common/type.h"

namespace carmen::backend::index {
// MultiLevelDBIndexTestAdapter is a wrapper around MultiLevelDBIndex. Providing
// interface for benchmarking and testing. This is subject to be removed
// once we have index interface updated.
template <Trivial K, std::integral I>
class MultiLevelDBIndexTestAdapter {
 public:
  using key_type [[maybe_unused]] = K;
  using value_type [[maybe_unused]] = I;

  explicit MultiLevelDBIndexTestAdapter(MultiLevelDBIndex<K, I> index)
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
  MultiLevelDBIndex<K, I> index_;
};
}  // namespace carmen::backend::index
