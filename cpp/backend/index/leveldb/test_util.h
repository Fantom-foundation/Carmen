#pragma once

#include "backend/index/index.h"
#include "backend/index/leveldb/index.h"
#include "common/file_util.h"
#include "common/hash.h"
#include "common/type.h"

namespace carmen::backend::index {
// LevelDBKeySpaceAdapter is a wrapper around LevelDBKeySpace. It exposes
// LevelDBKeySpace methods to be compatible with tests.
template <Trivial K, std::integral I>
class LevelDBKeySpaceTestAdapter {
 public:
  using key_type [[maybe_unused]] = K;
  using value_type [[maybe_unused]] = I;

  explicit LevelDBKeySpaceTestAdapter(LevelDBKeySpace<K, I> key_space)
      : key_space_(std::move(key_space)) {}

  std::pair<I, bool> GetOrAdd(const K& key) {
    auto result = key_space_.GetOrAdd(key);
    if (result.ok()) return {(*result).first, (*result).second};
    // no way to handle error
    return {0, false};
  }

  std::optional<I> Get(const K& key) const {
    auto result = key_space_.Get(key);
    if (result.ok()) return *result;
    return std::nullopt;
  }

  Hash GetHash() {
    auto result = key_space_.GetHash();
    if (result.ok()) return *result;
    return Hash{};
  }

 private:
  LevelDBKeySpace<K, I> key_space_;
};
}  // namespace carmen::backend::index
