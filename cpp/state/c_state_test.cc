#include "state/c_state.h"

#include "common/account_state.h"
#include "common/file_util.h"
#include "common/type.h"
#include "gtest/gtest.h"

namespace carmen {
namespace {

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
        return;
      case Config::kFileBased:
        dir_ = std::make_unique<TempDir>();
        auto path = dir_->GetPath().string();
        state_ = Carmen_CreateFileBasedState(path.c_str(), path.size());
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

INSTANTIATE_TEST_SUITE_P(
    All, CStateTest, testing::Values(Config::kInMemory, Config::kFileBased),
    [](const testing::TestParamInfo<CStateTest::ParamType>& info) {
      return ToString(info.param);
    });

}  // namespace
}  // namespace carmen
