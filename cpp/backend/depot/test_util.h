/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use of
 * this software will be governed by the GNU Lesser General Public License v3.
 */

#pragma once

#include <filesystem>

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

  static absl::StatusOr<MockDepot> Open(Context&,
                                        const std::filesystem::path&) {
    return MockDepot();
  }
  auto Set(const auto& key, auto data) { return depot_->Set(key, data); }
  auto Get(const auto& key) const { return depot_->Get(key); }
  auto GetSize(const auto& key) const { return depot_->GetSize(key); }
  auto GetHash() const { return depot_->GetHash(); }
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
    MOCK_METHOD(absl::Status, Flush, ());
    MOCK_METHOD(absl::Status, Close, ());
    MOCK_METHOD(MemoryFootprint, GetMemoryFootprint, (), (const));
  };
  std::unique_ptr<Mock> depot_{std::make_unique<Mock>()};
};

}  // namespace carmen::backend::depot
