#pragma once

#include "common/memory_usage.h"
#include "common/type.h"
#include "gmock/gmock.h"

namespace carmen::backend::depot {

// A generic mock implementation for mocking out depot implementations.
template <std::integral K>
class MockDepot {
 public:
  using key_type = K;
  MOCK_METHOD(absl::StatusOr<std::span<const std::byte>>, Get, (const K& key),
              (const));
  MOCK_METHOD(absl::StatusOr<std::uint32_t>, GetSize, (const K& key), (const));
  MOCK_METHOD(absl::Status, Set,
              (const K& key, std::span<const std::byte> data));
  MOCK_METHOD(absl::StatusOr<Hash>, GetHash, (), (const));
  MOCK_METHOD(absl::Status, Flush, ());
  MOCK_METHOD(absl::Status, Close, ());
  MOCK_METHOD(MemoryFootprint, GetMemoryFootprint, (), (const));
};

// A movable wrapper of a mock depot. This may be required when a depot needs to
// be moved into position.
template <std::integral K>
class MockDepotWrapper {
 public:
  using key_type = K;

  MockDepotWrapper() : depot_(std::make_unique<MockDepot<K>>()) {}

  absl::Status Set(const K& key, std::span<const std::byte> data) {
    return depot_->Set(key, data);
  }

  absl::StatusOr<std::span<const std::byte>> Get(const K& key) const {
    return depot_->Get(key);
  }

  absl::StatusOr<std::uint32_t> GetSize(const K& key) const {
    return depot_->GetCode(key);
  }

  absl::StatusOr<Hash> GetHash() const { return depot_->GetHash(); }

  absl::Status Flush() { return depot_->Flush(); }

  absl::Status Close() { return depot_->Close(); }

  MemoryFootprint GetMemoryFootprint() const { depot_->GetMemoryFootprint(); }

  MockDepot<K>& GetMockDepot() { return *depot_; }

 private:
  std::unique_ptr<MockDepot<K>> depot_;
};

}  // namespace carmen::backend::depot
