#pragma once

#include <filesystem>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/index/index.h"
#include "backend/index/leveldb/single_db/index.h"
#include "backend/structure.h"
#include "common/file_util.h"
#include "common/hash.h"
#include "common/memory_usage.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen::backend::index {
// SingleLevelDbIndexTestAdapter is a wrapper around LevelDbKeySpace. Providing
// interface for benchmarking and testing. This is subject to be removed
// once we have index interface updated.
template <Trivial K, std::integral I>
class SingleLevelDbIndexTestAdapter {
 public:
  using key_type [[maybe_unused]] = K;
  using value_type [[maybe_unused]] = I;

  static absl::StatusOr<SingleLevelDbIndexTestAdapter> Open(
      Context& context, const std::filesystem::path& path) {
    ASSIGN_OR_RETURN(auto space, (LevelDbKeySpace<K, I>::Open(context, path)));
    return SingleLevelDbIndexTestAdapter(std::move(space));
  }

  explicit SingleLevelDbIndexTestAdapter(LevelDbKeySpace<K, I> key_space)
      : key_space_(std::move(key_space)) {}

  std::pair<I, bool> GetOrAdd(const K& key) {
    auto result = key_space_.GetOrAdd(key);
    if (result.ok()) return *result;
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

  absl::Status Flush() { return key_space_.Flush(); }

  absl::Status Close() { return key_space_.Close(); }

  MemoryFootprint GetMemoryFootprint() const {
    return key_space_.GetMemoryFootprint();
  }

 private:
  LevelDbKeySpace<K, I> key_space_;
};
}  // namespace carmen::backend::index
