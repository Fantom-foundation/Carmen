// Copyright (c) 2024 Fantom Foundation
// 
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
// 
// Change Date: 2028-4-16
// 
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

#pragma once

#include <cstdint>
#include <filesystem>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "archive/archive.h"
#include "common/memory_usage.h"
#include "common/type.h"
#include "gmock/gmock.h"
#include "state/update.h"

namespace carmen {
// A movable wrapper of a mock archive. This may be required when an archive
// needs to be moved into position.
class MockArchive {
 public:
  static absl::StatusOr<MockArchive> Open(std::filesystem::path) {
    return MockArchive();
  }
  auto Add(BlockId block, const Update& update) {
    return archive_->Add(block, update);
  }
  auto Exists(BlockId block, const Address& account) {
    return archive_->Exists(block, account);
  }
  auto GetBalance(BlockId block, const Address& account) {
    return archive_->GetBalance(block, account);
  }
  auto GetCode(BlockId block, const Address& account) {
    return archive_->GetCode(block, account);
  }
  auto GetNonce(BlockId block, const Address& account) {
    return archive_->GetNonce(block, account);
  }
  auto GetStorage(BlockId block, const Address& account, const Key& key) {
    return archive_->GetStorage(block, account, key);
  }
  auto GetHash(BlockId block) { return archive_->GetHash(block); }
  auto GetAccountList(BlockId block) { return archive_->GetAccountList(block); }
  auto GetLatestBlock() { return archive_->GetLatestBlock(); }
  auto GetAccountHash(BlockId block, const Address& account) {
    return archive_->GetAccountHash(block, account);
  }

  auto Verify(BlockId block, const Hash& hash,
              absl::FunctionRef<void(std::string_view)> observer) {
    return archive_->Verify(block, hash, observer);
  }

  auto Flush() { return archive_->Flush(); }
  auto Close() { return archive_->Close(); }
  MemoryFootprint GetMemoryFootprint() const {
    return archive_->GetMemoryFootprint();
  }
  auto& GetMockArchive() { return *archive_; }

 private:
  class Mock {
   public:
    MOCK_METHOD(absl::Status, Add, (BlockId block, const Update& update));
    MOCK_METHOD(absl::StatusOr<bool>, Exists,
                (BlockId block, const Address& account));
    MOCK_METHOD(absl::StatusOr<Balance>, GetBalance,
                (BlockId block, const Address& account));
    MOCK_METHOD(absl::StatusOr<Code>, GetCode,
                (BlockId block, const Address& account));
    MOCK_METHOD(absl::StatusOr<Nonce>, GetNonce,
                (BlockId block, const Address& account));
    MOCK_METHOD(absl::StatusOr<Value>, GetStorage,
                (BlockId block, const Address& account, const Key& key));
    MOCK_METHOD(absl::StatusOr<Hash>, GetHash, (BlockId block));
    MOCK_METHOD(absl::StatusOr<std::vector<Address>>, GetAccountList,
                (BlockId block));
    MOCK_METHOD(absl::StatusOr<BlockId>, GetLatestBlock, ());
    MOCK_METHOD(absl::StatusOr<Hash>, GetAccountHash,
                (BlockId block, const Address& account));
    MOCK_METHOD(absl::Status, Verify,
                (BlockId block, const Hash& hash,
                 absl::FunctionRef<void(std::string_view)>));
    MOCK_METHOD(absl::Status, Flush, ());
    MOCK_METHOD(absl::Status, Close, ());
    MOCK_METHOD(MemoryFootprint, GetMemoryFootprint, (), (const));
  };
  std::unique_ptr<Mock> archive_{std::make_unique<Mock>()};
};

static_assert(Archive<MockArchive>, "MockArchive is not a valid archive.");

}  // namespace carmen
