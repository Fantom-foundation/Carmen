#pragma once

#include <filesystem>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/structure.h"
#include "common/memory_usage.h"
#include "common/status_util.h"
#include "common/type.h"
#include "gmock/gmock.h"

namespace carmen::backend::store {

// A generic mock implementation for mocking out store implementations.
template <typename K, Trivial V, std::size_t page_size>
class MockStore {
 public:
  using key_type = K;
  using value_type = V;
  constexpr static std::size_t kPageSize = page_size;
  static absl::StatusOr<MockStore> Open(Context&,
                                        const std::filesystem::path&){};

  MOCK_METHOD(absl::Status, Set, (const K& key, V value));
  MOCK_METHOD(StatusOrRef<const V>, Get, (const K& key), (const));
  MOCK_METHOD(absl::StatusOr<Hash>, GetHash, (), (const));
  MOCK_METHOD(absl::Status, Flush, ());
  MOCK_METHOD(absl::Status, Close, ());
  MOCK_METHOD(MemoryFootprint, GetMemoryFootprint, (), (const));
};

// A movable wrapper of a mock store. This may be required when a store needs to
// be moved into position.
template <typename K, Trivial V, std::size_t page_size = 32>
class MockStoreWrapper {
 public:
  using key_type = K;
  using value_type = V;
  constexpr static std::size_t kPageSize = page_size;

  static absl::StatusOr<MockStoreWrapper> Open(Context&,
                                               const std::filesystem::path&) {
    return MockStoreWrapper();
  }
  MockStoreWrapper() : store_(std::make_unique<MockStore<K, V, page_size>>()) {}
  auto Set(const auto& key, auto data) { return store_->Set(key, data); }
  auto Get(const auto& key) const { return store_->Get(key); }
  auto GetSize(const auto& key) const { return store_->GetSize(key); }
  auto GetHash() const { return store_->GetHash(); }
  auto Flush() { return store_->Flush(); }
  auto Close() { return store_->Close(); }
  MemoryFootprint GetMemoryFootprint() const { store_->GetMemoryFootprint(); }
  MockStore<K, V, page_size>& GetMockStore() { return *store_; }

 private:
  std::unique_ptr<MockStore<K, V, page_size>> store_;
};

}  // namespace carmen::backend::store
