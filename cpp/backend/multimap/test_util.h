#pragma once

#include <filesystem>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/structure.h"
#include "common/memory_usage.h"
#include "common/status_util.h"
#include "common/type.h"
#include "gmock/gmock.h"

namespace carmen::backend::multimap {
// A movable wrapper of a mock multimap. This may be required when a multimap
// needs to be moved into position.
template <std::integral K, Trivial V>
class MockMultiMap {
 public:
  using key_type = K;
  using value_type = V;

  static absl::StatusOr<MockMultiMap> Open(Context&,
                                           const std::filesystem::path&) {
    return MockMultiMap();
  }
  auto Insert(const auto& key, const auto& value) {
    return map_->Insert(key, value);
  }
  auto Contains(const auto& key, const auto& value) const {
    return map_->Contains(key, value);
  }
  auto Erase(const auto& key) { return map_->Erase(key); }
  auto Erase(const auto& key, const auto& value) {
    return map_->Erase(key, value);
  }
  template <typename Op>
  absl::Status ForEach(const auto& key, const Op& op) {
    return map_->ForEach(key, std::function<void(std::uint32_t)>(op));
  }
  auto Flush() { return map_->Flush(); }
  auto Close() { return map_->Close(); }
  auto GetMemoryFootprint() const { map_->GetMemoryFootprint(); }
  auto& GetMockMultiMap() { return *map_; }

 private:
  class Mock {
   public:
    static absl::StatusOr<Mock> Open(Context&, const std::filesystem::path&){};

    MOCK_METHOD(absl::StatusOr<bool>, Insert, (const K& key, const V& value));
    MOCK_METHOD(absl::StatusOr<bool>, Contains, (const K& key, const V& value),
                (const));
    MOCK_METHOD(absl::Status, Erase, (const K& key));
    MOCK_METHOD(absl::StatusOr<bool>, Erase, (const K& key, const V& value));
    MOCK_METHOD(absl::Status, ForEach,
                (const K& key, std::function<void(std::uint32_t)>));
    MOCK_METHOD(absl::Status, Flush, ());
    MOCK_METHOD(absl::Status, Close, ());
    MOCK_METHOD(MemoryFootprint, GetMemoryFootprint, (), (const));
  };

  std::unique_ptr<Mock> map_{std::make_unique<Mock>()};
};

}  // namespace carmen::backend::multimap
