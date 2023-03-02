#pragma once

#include <filesystem>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/index/snapshot.h"
#include "backend/structure.h"
#include "common/memory_usage.h"
#include "common/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
// A movable wrapper of a mock index. This may be required when an index needs
// to be moved into position.
template <typename K, typename V>
class MockIndex {
 public:
  using key_type = K;
  using value_type = V;
  using Snapshot = IndexSnapshot<K>;

  static absl::StatusOr<MockIndex> Open(Context&,
                                        const std::filesystem::path&) {
    return MockIndex();
  }
  auto GetOrAdd(const auto& key) { return index_->GetOrAdd(key); }
  auto Get(const auto& key) const { return index_->Get(key); }
  auto GetHash() { return index_->GetHash(); }
  auto GetProof() const { return index_->GetProof(); }
  auto CreateSnapshot() const { return index_->CreateSnapshot(); }
  auto SyncTo(const Snapshot& snapshot) { return index_->SyncTo(snapshot); }
  auto Flush() { return index_->Flush(); }
  auto Close() { return index_->Close(); }
  MemoryFootprint GetMemoryFootprint() const { index_->GetMemoryFootprint(); }
  // Returns a reference to the wrapped MockIndex. This pointer is stable.
  auto& GetMockIndex() { return *index_; }

 private:
  class Mock {
   public:
    MOCK_METHOD((absl::StatusOr<std::pair<V, bool>>), GetOrAdd, (const K& key));
    MOCK_METHOD((absl::StatusOr<V>), Get, (const K& key), (const));
    MOCK_METHOD(absl::StatusOr<Hash>, GetHash, ());
    MOCK_METHOD(absl::StatusOr<IndexProof>, GetProof, (), (const));
    MOCK_METHOD(absl::StatusOr<Snapshot>, CreateSnapshot, (), (const));
    MOCK_METHOD(absl::Status, SyncTo, (const Snapshot&));
    MOCK_METHOD(absl::Status, Flush, ());
    MOCK_METHOD(absl::Status, Close, ());
    MOCK_METHOD(MemoryFootprint, GetMemoryFootprint, (), (const));
  };
  std::unique_ptr<Mock> index_{std::make_unique<Mock>()};
};

}  // namespace carmen::backend::index
