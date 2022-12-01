#include "state/c_state.h"

#include "common/account_state.h"
#include "common/file_util.h"
#include "common/hash.h"
#include "common/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen {
namespace {

using ::testing::ElementsAre;

enum class Config {
  kInMemory,
  kFileBased,
};

std::string ToString(Config c) {
  switch (c) {
    case Config::kInMemory:
      return "InMemory";
    case Config::kFileBased:
      return "FileBased";
  }
  return "Unknown";
}

class CStateTest : public testing::TestWithParam<Config> {
 public:
  void SetUp() override {
    switch (GetParam()) {
      case Config::kInMemory:
        state_ = Carmen_CreateInMemoryState();
        ASSERT_NE(state_, nullptr);
        return;
      case Config::kFileBased:
        dir_ = std::make_unique<TempDir>();
        auto path = dir_->GetPath().string();
        state_ = Carmen_CreateFileBasedState(path.c_str(), path.size());
        ASSERT_NE(state_, nullptr);
        return;
    }
    FAIL() << "Unknown configuration: " << ToString(GetParam());
  }

  void TearDown() override {
    Carmen_ReleaseState(state_);
    state_ = nullptr;
  }

  C_State GetState() { return state_; }

 private:
  std::unique_ptr<TempDir> dir_;
  C_State state_;
};

TEST_P(CStateTest, StateCanBeCreatedAndReleased) {
  auto state = GetState();
  EXPECT_NE(state, nullptr);
}

TEST_P(CStateTest, AccountsInitiallyDoNotExist) {
  auto state = GetState();
  Address addr{0x01};
  AccountState as = AccountState::kExists;
  Carmen_GetAccountState(state, &addr, &as);
  EXPECT_EQ(as, AccountState::kUnknown);
}

TEST_P(CStateTest, AccountsCanBeCreated) {
  auto state = GetState();
  Address addr{0x01};
  AccountState as = AccountState::kExists;
  Carmen_GetAccountState(state, &addr, &as);
  EXPECT_EQ(as, AccountState::kUnknown);
  Carmen_CreateAccount(state, &addr);
  Carmen_GetAccountState(state, &addr, &as);
  EXPECT_EQ(as, AccountState::kExists);
}

TEST_P(CStateTest, AccountsCanBeDeleted) {
  auto state = GetState();
  Address addr{0x01};
  AccountState as = AccountState::kExists;
  Carmen_GetAccountState(state, &addr, &as);
  EXPECT_EQ(as, AccountState::kUnknown);
  Carmen_CreateAccount(state, &addr);
  Carmen_DeleteAccount(state, &addr);
  Carmen_GetAccountState(state, &addr, &as);
  EXPECT_EQ(as, AccountState::kDeleted);
}

TEST_P(CStateTest, BalancesAreInitiallyZero) {
  auto state = GetState();
  ASSERT_NE(state, nullptr);

  Address addr{0x01};
  Balance balance{0x02};
  Carmen_GetBalance(state, &addr, &balance);
  EXPECT_EQ(Balance{}, balance);
}

TEST_P(CStateTest, BalancesCanBeUpdated) {
  auto state = GetState();
  ASSERT_NE(state, nullptr);

  Address addr{0x01};
  Balance balance{0x02};
  Carmen_GetBalance(state, &addr, &balance);
  EXPECT_EQ(Balance{}, balance);

  balance = Balance{0x03};
  Carmen_SetBalance(state, &addr, &balance);
  balance = Balance{};
  Carmen_GetBalance(state, &addr, &balance);
  EXPECT_EQ(Balance{0x03}, balance);
}

TEST_P(CStateTest, NoncesAreInitiallyZero) {
  auto state = GetState();
  ASSERT_NE(state, nullptr);

  Address addr{0x01};
  Nonce nonce{0x02};
  Carmen_GetNonce(state, &addr, &nonce);
  EXPECT_EQ(Nonce{}, nonce);
}

TEST_P(CStateTest, NoncesCanBeUpdated) {
  auto state = GetState();
  ASSERT_NE(state, nullptr);

  Address addr{0x01};
  Nonce nonce{0x02};
  Carmen_GetNonce(state, &addr, &nonce);
  EXPECT_EQ(Nonce{}, nonce);

  nonce = Nonce{0x03};
  Carmen_SetNonce(state, &addr, &nonce);
  nonce = Nonce{};
  Carmen_GetNonce(state, &addr, &nonce);
  EXPECT_EQ(Nonce{0x03}, nonce);
}

TEST_P(CStateTest, StorageLocationsAreInitiallyZero) {
  auto state = GetState();
  ASSERT_NE(state, nullptr);

  Address addr{0x01};
  Key key{0x02};
  Value value{0x03};
  Carmen_GetStorageValue(state, &addr, &key, &value);
  EXPECT_EQ(Value{}, value);
}

TEST_P(CStateTest, StorageLocationsCanBeUpdated) {
  auto state = GetState();
  ASSERT_NE(state, nullptr);

  Address addr{0x01};
  Key key{0x02};
  Value value{0x03};
  Carmen_GetStorageValue(state, &addr, &key, &value);
  EXPECT_EQ(Value{}, value);

  value = Value{0x04};
  Carmen_SetStorageValue(state, &addr, &key, &value);
  value = Value{};
  Carmen_GetStorageValue(state, &addr, &key, &value);
  EXPECT_EQ(Value{0x04}, value);
}

TEST_P(CStateTest, StateHashesCanBeObtained) {
  auto state = GetState();
  ASSERT_NE(state, nullptr);

  Hash hash;
  Carmen_GetHash(state, &hash);
  EXPECT_NE(Hash{}, hash);
}

TEST_P(CStateTest, HashesChangeOnUpdates) {
  auto state = GetState();
  ASSERT_NE(state, nullptr);

  Hash initial_hash;
  Carmen_GetHash(state, &initial_hash);

  Address addr{0x01};
  Key key{0x02};
  Value value{0x03};
  Carmen_SetStorageValue(state, &addr, &key, &value);

  Hash new_hash;
  Carmen_GetHash(state, &new_hash);

  EXPECT_NE(initial_hash, new_hash);
}

TEST_P(CStateTest, CodesAreInitallyEmpty) {
  auto state = GetState();
  ASSERT_NE(state, nullptr);

  Address addr{0x01};
  std::vector<std::byte> code(10);
  uint32_t size = code.size();
  Carmen_GetCode(state, &addr, code.data(), &size);
  EXPECT_EQ(size, 0);
}

TEST_P(CStateTest, CodesCanBeSetAndRetrieved) {
  auto state = GetState();
  ASSERT_NE(state, nullptr);

  Address addr{0x01};
  std::vector<std::byte> code({std::byte{12}, std::byte{14}});
  Carmen_SetCode(state, &addr, code.data(), code.size());

  std::vector<std::byte> restored(10);
  uint32_t size = restored.size();
  Carmen_GetCode(state, &addr, restored.data(), &size);
  ASSERT_EQ(size, 2);
  restored.resize(size);
  EXPECT_EQ(code, restored);
}

TEST_P(CStateTest, GetCodeFailsIfBufferIsTooSmall) {
  auto state = GetState();
  ASSERT_NE(state, nullptr);

  Address addr{0x01};
  std::vector<std::byte> code({std::byte{12}, std::byte{14}});
  Carmen_SetCode(state, &addr, code.data(), code.size());

  std::vector<std::byte> restored({std::byte{10}});
  uint32_t size = restored.size();
  Carmen_GetCode(state, &addr, restored.data(), &size);
  EXPECT_EQ(size, 2);
  EXPECT_THAT(restored, ElementsAre(std::byte{10}));
}

TEST_P(CStateTest, CodesAffectHashes) {
  auto state = GetState();
  ASSERT_NE(state, nullptr);

  Hash initial;
  Carmen_GetHash(state, &initial);

  Address addr{0x01};
  std::vector<std::byte> code({std::byte{12}, std::byte{14}});
  Carmen_SetCode(state, &addr, code.data(), code.size());

  Hash first_update;
  Carmen_GetHash(state, &first_update);

  code.push_back(std::byte{16});
  Carmen_SetCode(state, &addr, code.data(), code.size());

  Hash second_update;
  Carmen_GetHash(state, &second_update);

  EXPECT_NE(initial, first_update);
  EXPECT_NE(initial, second_update);
  EXPECT_NE(first_update, second_update);
}

TEST_P(CStateTest, CodeHashesMatchCodes) {
  auto state = GetState();
  ASSERT_NE(state, nullptr);

  Address addr{0x01};
  Hash hash;
  Carmen_GetCodeHash(state, &addr, &hash);
  EXPECT_EQ(hash, Hash{});

  std::vector<std::byte> code({std::byte{12}, std::byte{14}});
  Carmen_SetCode(state, &addr, code.data(), code.size());
  Carmen_GetCodeHash(state, &addr, &hash);
  EXPECT_EQ(hash, GetKeccak256Hash(std::span(code)));

  code.clear();
  Carmen_SetCode(state, &addr, code.data(), code.size());
  Carmen_GetCodeHash(state, &addr, &hash);
  EXPECT_EQ(hash, Hash{});
}

TEST_P(CStateTest, CodeSizesMatchCodes) {
  auto state = GetState();
  ASSERT_NE(state, nullptr);

  Address addr{0x01};
  std::vector<std::byte> code({std::byte{12}, std::byte{14}});
  Carmen_SetCode(state, &addr, code.data(), code.size());

  std::uint32_t size;
  Carmen_GetCodeSize(state, &addr, &size);
  EXPECT_EQ(size, 2);

  code.clear();
  Carmen_SetCode(state, &addr, code.data(), code.size());
  Carmen_GetCodeSize(state, &addr, &size);
  EXPECT_EQ(size, 0);
}

TEST_P(CStateTest, StateCanBeFlushed) {
  auto state = GetState();
  ASSERT_NE(state, nullptr);
  Address addr{0x01};
  Key key{0x02};
  Value value{0x03};
  Carmen_SetStorageValue(state, &addr, &key, &value);

  Carmen_Flush(state);
}

TEST_P(CStateTest, StateCanBeFlushedMoreThanOnce) {
  auto state = GetState();
  ASSERT_NE(state, nullptr);
  Address addr{0x01};
  Key key{0x02};
  Value value{0x03};
  Carmen_SetStorageValue(state, &addr, &key, &value);

  Carmen_Flush(state);

  value = Value{0x04};
  Carmen_SetStorageValue(state, &addr, &key, &value);

  Carmen_Flush(state);
  Carmen_Flush(state);
}

TEST_P(CStateTest, StateCanBeClosed) {
  auto state = GetState();
  ASSERT_NE(state, nullptr);
  Carmen_Close(state);
}

TEST_P(CStateTest, MemoryFootprintCanBeObtained) {
  auto state = GetState();
  ASSERT_NE(state, nullptr);
  char* data = nullptr;
  uint64_t length;
  Carmen_GetMemoryFootprint(state, &data, &length);
  EXPECT_NE(data, nullptr);
  EXPECT_GT(length, 0);
  free(data);
}

INSTANTIATE_TEST_SUITE_P(
    All, CStateTest, testing::Values(Config::kInMemory, Config::kFileBased),
    [](const testing::TestParamInfo<CStateTest::ParamType>& info) {
      return ToString(info.param);
    });

TEST(FileBasedStateTest, CanBeStoredAndReloaded) {
  TempDir dir;
  auto path = dir.GetPath().string();
  Hash hash;
  {
    auto state = Carmen_CreateFileBasedState(path.c_str(), path.length());
    ASSERT_NE(state, nullptr);

    Address addr{0x01};
    Key key{0x02};
    Value value{0x03};
    Carmen_SetStorageValue(state, &addr, &key, &value);
    Carmen_GetHash(state, &hash);
    Carmen_ReleaseState(state);
  }
  {
    auto state = Carmen_CreateFileBasedState(path.c_str(), path.length());
    ASSERT_NE(state, nullptr);

    Address addr{0x01};
    Key key{0x02};
    Value value{};
    Carmen_GetStorageValue(state, &addr, &key, &value);
    EXPECT_EQ(value, Value{0x03});
    Hash recovered;
    Carmen_GetHash(state, &recovered);
    EXPECT_EQ(hash, recovered);
    Carmen_ReleaseState(state);
  }
}

}  // namespace
}  // namespace carmen
