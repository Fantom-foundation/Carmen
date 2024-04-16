/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3.
 */

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
// A movable wrapper of a mock store. This may be required when a store needs to
// be moved into position.
template <typename K, Trivial V, std::size_t page_size = 32>
class MockStore {
 public:
  using key_type = K;
  using value_type = V;
  constexpr static std::size_t kPageSize = page_size;

  static absl::StatusOr<MockStore> Open(Context&,
                                        const std::filesystem::path&) {
    return MockStore();
  }
  auto Set(const auto& key, auto data) { return store_->Set(key, data); }
  auto Get(const auto& key) const { return store_->Get(key); }
  auto GetSize(const auto& key) const { return store_->GetSize(key); }
  auto GetHash() const { return store_->GetHash(); }
  auto Flush() { return store_->Flush(); }
  auto Close() { return store_->Close(); }
  MemoryFootprint GetMemoryFootprint() const { store_->GetMemoryFootprint(); }
  auto& GetMockStore() { return *store_; }

 private:
  class Mock {
   public:
    MOCK_METHOD(absl::Status, Set, (const K& key, V value));
    MOCK_METHOD(StatusOrRef<const V>, Get, (const K& key), (const));
    MOCK_METHOD(absl::StatusOr<Hash>, GetHash, (), (const));
    MOCK_METHOD(absl::Status, Flush, ());
    MOCK_METHOD(absl::Status, Close, ());
    MOCK_METHOD(MemoryFootprint, GetMemoryFootprint, (), (const));
  };
  std::unique_ptr<Mock> store_{std::make_unique<Mock>()};
};

}  // namespace carmen::backend::store
