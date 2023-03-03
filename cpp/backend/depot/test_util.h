#pragma once

#include <filesystem>

#include "backend/depot/snapshot.h"
#include "backend/structure.h"
#include "common/memory_usage.h"
#include "common/type.h"
#include "gmock/gmock.h"

namespace carmen::backend::depot {

// A movable wrapper of a mock depot. This may be required when a depot needs to
// be moved into position.
template <std::integral K>
class MockDepot {
 public:
  using key_type = K;
  using Snapshot = DepotSnapshot;

  static absl::StatusOr<MockDepot> Open(Context&,
                                        const std::filesystem::path&) {
    return MockDepot();
  }
  auto Set(const auto& key, auto data) { return depot_->Set(key, data); }
  auto Get(const auto& key) const { return depot_->Get(key); }
  auto GetSize(const auto& key) const { return depot_->GetSize(key); }
  auto GetHash() const { return depot_->GetHash(); }
  auto GetProof() const { return depot_->GetProof(); }
  auto CreateSnapshot() const { return depot_->CreateSnapshot(); }
  auto SyncTo(const Snapshot& snapshot) { return depot_->SyncTo(snapshot); }
  auto Flush() { return depot_->Flush(); }
  auto Close() { return depot_->Close(); }
  MemoryFootprint GetMemoryFootprint() const { depot_->GetMemoryFootprint(); }
  auto& GetMockDepot() { return *depot_; }

 private:
  class Mock {
   public:
    MOCK_METHOD(absl::StatusOr<std::span<const std::byte>>, Get, (const K& key),
                (const));
    MOCK_METHOD(absl::StatusOr<std::uint32_t>, GetSize, (const K& key),
                (const));
    MOCK_METHOD(absl::Status, Set,
                (const K& key, std::span<const std::byte> data));
    MOCK_METHOD(absl::StatusOr<Hash>, GetHash, (), (const));
    MOCK_METHOD(absl::StatusOr<DepotProof>, GetProof, (), (const));
    MOCK_METHOD(absl::StatusOr<Snapshot>, CreateSnapshot, (), (const));
    MOCK_METHOD(absl::Status, SyncTo, (const Snapshot&));
    MOCK_METHOD(absl::Status, Flush, ());
    MOCK_METHOD(absl::Status, Close, ());
    MOCK_METHOD(MemoryFootprint, GetMemoryFootprint, (), (const));
  };
  std::unique_ptr<Mock> depot_{std::make_unique<Mock>()};
};

}  // namespace carmen::backend::depot
