#include "state/c_state.h"

#include "common/account_state.h"
#include "common/file_util.h"
#include "common/hash.h"
#include "common/status_test_util.h"
#include "common/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "state/update.h"

namespace carmen {
namespace {

using ::testing::ElementsAre;

enum class Variant {
  kInMemory,
  kFileBased,
  kLevelDbBased,
};

std::string ToString(Variant c) {
  switch (c) {
    case Variant::kInMemory:
      return "InMemory";
    case Variant::kFileBased:
      return "FileBased";
    case Variant::kLevelDbBased:
      return "LevelDbBased";
  }
  return "Unknown";
}

// A configuration struct for the parameterized test below.
struct Config {
  Variant variant;
  bool with_archive;
};

// Wrapper functions for updateing individual elements.

void Carmen_CreateAccount(C_State state, C_Address addr) {
  Update update;
  update.Create(*reinterpret_cast<const Address*>(addr));
  auto data = update.ToBytes();
  Carmen_Apply(state, 0, data->data(), data->size());
}

void Carmen_DeleteAccount(C_State state, C_Address addr) {
  Update update;
  update.Delete(*reinterpret_cast<const Address*>(addr));
  auto data = update.ToBytes();
  Carmen_Apply(state, 0, data->data(), data->size());
}

void Carmen_SetBalance(C_State state, C_Address addr, C_Balance balance) {
  Update update;
  update.Set(*reinterpret_cast<const Address*>(addr),
             *reinterpret_cast<const Balance*>(balance));
  auto data = update.ToBytes();
  Carmen_Apply(state, 0, data->data(), data->size());
}

void Carmen_SetCode(C_State state, C_Address addr, C_Code code,
                    uint32_t length) {
  Update update;
  update.Set(*reinterpret_cast<const Address*>(addr),
             Code(std::span(reinterpret_cast<const std::byte*>(code), length)));
  auto data = update.ToBytes();
  Carmen_Apply(state, 0, data->data(), data->size());
}

void Carmen_SetNonce(C_State state, C_Address addr, C_Nonce nonce) {
  Update update;
  update.Set(*reinterpret_cast<const Address*>(addr),
             *reinterpret_cast<const Nonce*>(nonce));
  auto data = update.ToBytes();
  Carmen_Apply(state, 0, data->data(), data->size());
}

void Carmen_SetStorageValue(C_State state, C_Address addr, C_Key key,
                            C_Value value) {
  Update update;
  update.Set(*reinterpret_cast<const Address*>(addr),
             *reinterpret_cast<const Key*>(key),
             *reinterpret_cast<const Value*>(value));
  auto data = update.ToBytes();
  Carmen_Apply(state, 0, data->data(), data->size());
}

class CStateTest : public testing::TestWithParam<Config> {
 public:
  void SetUp() override {
    const Config& config = GetParam();
    switch (config.variant) {
      case Variant::kInMemory: {
        state_ = Carmen_CreateInMemoryState(config.with_archive);
        ASSERT_NE(state_, nullptr);
        return;
      }
      case Variant::kFileBased: {
        dir_ = std::make_unique<TempDir>();
        auto path = dir_->GetPath().string();
        state_ = Carmen_CreateFileBasedState(path.c_str(), path.size(),
                                             config.with_archive);
        ASSERT_NE(state_, nullptr);
        return;
      }
      case Variant::kLevelDbBased: {
        dir_ = std::make_unique<TempDir>();
        auto path = dir_->GetPath().string();
        state_ = Carmen_CreateLevelDbBasedState(path.c_str(), path.size(),
                                                config.with_archive);
        ASSERT_NE(state_, nullptr);
        return;
      }
    }
    FAIL() << "Unknown variant: " << ToString(config.variant);
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
  EXPECT_EQ(as, AccountState::kUnknown);
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
  const Hash hash_of_empty_code = GetKeccak256Hash({});
  auto state = GetState();
  ASSERT_NE(state, nullptr);

  Address addr{0x01};
  Hash hash;
  Carmen_GetCodeHash(state, &addr, &hash);
  EXPECT_EQ(hash, hash_of_empty_code);

  std::vector<std::byte> code({std::byte{12}, std::byte{14}});
  Carmen_SetCode(state, &addr, code.data(), code.size());
  Carmen_GetCodeHash(state, &addr, &hash);
  EXPECT_EQ(hash, GetKeccak256Hash(std::span(code)));

  code.clear();
  Carmen_SetCode(state, &addr, code.data(), code.size());
  Carmen_GetCodeHash(state, &addr, &hash);
  EXPECT_EQ(hash, hash_of_empty_code);
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

TEST_P(CStateTest, ArchiveCanBeAccessedIfEnabled) {
  auto state = GetState();
  auto archive = Carmen_GetArchiveState(state, 0);
  EXPECT_EQ(archive != nullptr, GetParam().with_archive);
  Carmen_ReleaseState(archive);
}

TEST_P(CStateTest, ArchiveCanBeQueried) {
  if (!GetParam().with_archive) {
    return;  // This test is only relevant when archives are enabled
  }
  auto state = GetState();

  Address addr{0x12};
  Balance balance{0x45};
  Nonce nonce{0x67};
  Code code{0x89};
  Key key{0xAB};
  Value value{0xCD};

  Update update;
  update.Create(addr);
  update.Set(addr, balance);
  update.Set(addr, nonce);
  update.Set(addr, code);
  update.Set(addr, key, value);

  ASSERT_OK_AND_ASSIGN(auto data, update.ToBytes());
  Carmen_Apply(state, 1, data.data(), data.size());

  Balance balance_restored{0x99};
  Nonce nonce_restored{0x99};
  Value value_restored{0x99};
  Hash hash{0x99};

  // Check archive state at block 0.
  auto archive0 = Carmen_GetArchiveState(state, 0);
  ASSERT_TRUE(archive0);

  AccountState account_state;
  Carmen_GetAccountState(archive0, &addr, &account_state);
  EXPECT_EQ(account_state, AccountState::kUnknown);
  Carmen_GetBalance(archive0, &addr, &balance_restored);
  EXPECT_EQ(balance_restored, Balance{});
  Carmen_GetNonce(archive0, &addr, &nonce_restored);
  EXPECT_EQ(nonce_restored, Nonce{});
  Carmen_GetStorageValue(archive0, &addr, &key, &value_restored);
  EXPECT_EQ(value_restored, Value{});
  Carmen_GetHash(archive0, &hash);
  EXPECT_EQ(hash, Hash{});

  std::vector<std::byte> restored_code;
  restored_code.resize(100);
  uint32_t size = restored_code.size();
  Carmen_GetCode(archive0, &addr, restored_code.data(), &size);
  restored_code.resize(size);
  EXPECT_EQ(Code{restored_code}, Code{});

  // Check archive state at block 1.
  auto archive1 = Carmen_GetArchiveState(archive0, 1);
  ASSERT_TRUE(archive1);
  Carmen_GetAccountState(archive1, &addr, &account_state);
  EXPECT_EQ(account_state, AccountState::kExists);
  Carmen_GetBalance(archive1, &addr, &balance_restored);
  EXPECT_EQ(balance_restored, balance);
  Carmen_GetNonce(archive1, &addr, &nonce_restored);
  EXPECT_EQ(nonce_restored, nonce);
  Carmen_GetStorageValue(archive1, &addr, &key, &value_restored);
  EXPECT_EQ(value_restored, value);
  Carmen_GetHash(archive1, &hash);
  EXPECT_EQ(
      testing::PrintToString(hash),
      "0xcfff0617b252767e16f8798deafa1deb3fdbf892aa08a3e53baef043547e9448");

  restored_code.clear();
  restored_code.resize(100);
  size = restored_code.size();
  Carmen_GetCode(archive1, &addr, restored_code.data(), &size);
  restored_code.resize(size);
  EXPECT_EQ(Code{restored_code}, code);

  Carmen_ReleaseState(archive0);
  Carmen_ReleaseState(archive1);
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
    All, CStateTest,
    testing::Values(Config{Variant::kInMemory, false},
                    Config{Variant::kFileBased, false},
                    Config{Variant::kLevelDbBased, false},
                    Config{Variant::kInMemory, true},
                    Config{Variant::kFileBased, true},
                    Config{Variant::kLevelDbBased, true}),
    [](const testing::TestParamInfo<CStateTest::ParamType>& info) {
      const char* archive = info.param.with_archive ? "with" : "without";
      return ToString(info.param.variant) + "_" + archive + "_archive";
    });

void StoreAndReloadFileBasedStore(bool with_archive) {
  TempDir dir;
  auto path = dir.GetPath().string();
  Hash hash;
  {
    auto state =
        Carmen_CreateFileBasedState(path.c_str(), path.length(), with_archive);
    ASSERT_NE(state, nullptr);

    Address addr{0x01};
    Key key{0x02};
    Value value{0x03};
    Carmen_SetStorageValue(state, &addr, &key, &value);
    Carmen_GetHash(state, &hash);
    Carmen_ReleaseState(state);
  }
  {
    auto state =
        Carmen_CreateFileBasedState(path.c_str(), path.length(), with_archive);
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

TEST(FileBasedStateTest, CanBeStoredAndReloadedWithoutArchive) {
  StoreAndReloadFileBasedStore(/*with_archive=*/false);
}

TEST(FileBasedStateTest, CanBeStoredAndReloadedWithArchive) {
  StoreAndReloadFileBasedStore(/*with_archive=*/true);
}

void StoreAndReloadLevelDbBasedStore(bool with_archive) {
  TempDir dir;
  auto path = dir.GetPath().string();
  Hash hash;
  {
    auto state = Carmen_CreateLevelDbBasedState(path.c_str(), path.length(),
                                                with_archive);
    ASSERT_NE(state, nullptr);

    Address addr{0x01};
    Key key{0x02};
    Value value{0x03};
    Carmen_SetStorageValue(state, &addr, &key, &value);
    Carmen_GetHash(state, &hash);
    Carmen_ReleaseState(state);
  }
  {
    auto state = Carmen_CreateLevelDbBasedState(path.c_str(), path.length(),
                                                with_archive);
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

TEST(LevelDbBasedStateTest, CanBeStoredAndReloadedWithoutArchive) {
  StoreAndReloadLevelDbBasedStore(/*with_archive=*/false);
}

TEST(LevelDbBasedStateTest, CanBeStoredAndReloadedWithArchive) {
  StoreAndReloadLevelDbBasedStore(/*with_archive=*/true);
}

}  // namespace
}  // namespace carmen
