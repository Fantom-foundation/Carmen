#pragma once

#include <cstdint>
#include <filesystem>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/memory_usage.h"
#include "common/type.h"
#include "gmock/gmock.h"
#include "state/archive.h"
#include "state/update.h"

namespace carmen {
// A movable wrapper of a mock archive. This may be required when an archive
// needs to be moved into position.
class MockArchive {
 public:
  static absl::StatusOr<MockArchive> Open(std::filesystem::path) {
    return MockArchive();
  }
  auto Add(auto block, auto& update) { return archive_->Add(block, update); }
  auto Exists(auto block, const auto& account) {
    return archive_->Exists(block, account);
  }
  auto GetBalance(auto block, const auto& account) {
    return archive_->GetBalance(block, account);
  }
  auto GetCode(auto block, const auto& account) {
    return archive_->GetCode(block, account);
  }
  auto GetNonce(auto block, const auto& account) {
    return archive_->GetNonce(block, account);
  }
  auto GetStorage(auto block, const auto& account, const auto& key) {
    return archive_->GetStorage(block, account, key);
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
    MOCK_METHOD(absl::Status, Flush, ());
    MOCK_METHOD(absl::Status, Close, ());
    MOCK_METHOD(MemoryFootprint, GetMemoryFootprint, (), (const));
  };
  std::unique_ptr<Mock> archive_{std::make_unique<Mock>()};
};

}  // namespace carmen
