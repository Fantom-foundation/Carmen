#pragma once

#include <filesystem>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/index/index.h"
#include "backend/index/leveldb/multi_db/index.h"
#include "common/memory_usage.h"
#include "common/type.h"

namespace carmen::backend::index {
// MultiLevelDbIndexTestAdapter is a wrapper around MultiLevelDbIndex. Providing
// interface for benchmarking and testing. This is subject to be removed
// once we have index interface updated.
template <Trivial K, std::integral I>
class MultiLevelDbIndexTestAdapter {
 public:
  using key_type [[maybe_unused]] = K;
  using value_type [[maybe_unused]] = I;

  static absl::StatusOr<MultiLevelDbIndexTestAdapter> Open(
      Context& context, const std::filesystem::path& path) {
    ASSIGN_OR_RETURN(auto space,
                     (MultiLevelDbIndex<K, I>::Open(context, path)));
    return MultiLevelDbIndexTestAdapter(std::move(space));
  }

  explicit MultiLevelDbIndexTestAdapter(MultiLevelDbIndex<K, I> index)
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

  absl::StatusOr<Hash> GetHash() {
    auto result = index_.GetHash();
    if (result.ok()) return *result;
    return Hash{};
  }

  absl::Status Flush() { return index_.Flush(); }

  absl::Status Close() { return index_.Close(); }

  MemoryFootprint GetMemoryFootprint() const {
    return index_.GetMemoryFootprint();
  }

 private:
  MultiLevelDbIndex<K, I> index_;
};
}  // namespace carmen::backend::index
