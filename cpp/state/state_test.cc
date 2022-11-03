#include "state/state.h"

#include "backend/depot/memory/depot.h"
#include "backend/index/memory/index.h"
#include "backend/store/memory/store.h"
#include "common/account_state.h"
#include "common/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen {

using ::testing::ElementsAre;
using ::testing::ElementsAreArray;

template <typename K, typename V>
using InMemoryIndex = backend::index::InMemoryIndex<K, V>;

template <typename K, typename V>
using InMemoryStore = backend::store::InMemoryStore<K, V>;

template <typename K>
using InMemoryDepot = backend::depot::InMemoryDepot<K>;

using InMemoryState = State<InMemoryIndex, InMemoryStore, InMemoryDepot>;

TEST(StateTest, DefaultAccountStateIsUnknown) {
  Address a{0x01};
  Address b{0x02};

  InMemoryState state;
  EXPECT_EQ(AccountState::kUnknown, state.GetAccountState(a));
  EXPECT_EQ(AccountState::kUnknown, state.GetAccountState(b));
}

TEST(StateTest, AccountsCanBeCreatedAndAreDifferentiated) {
  Address a{0x01};
  Address b{0x02};

  InMemoryState state;
  EXPECT_EQ(AccountState::kUnknown, state.GetAccountState(a));
  EXPECT_EQ(AccountState::kUnknown, state.GetAccountState(b));

  state.CreateAccount(a);
  EXPECT_EQ(AccountState::kExists, state.GetAccountState(a));
  EXPECT_EQ(AccountState::kUnknown, state.GetAccountState(b));

  state.CreateAccount(b);
  EXPECT_EQ(AccountState::kExists, state.GetAccountState(a));
  EXPECT_EQ(AccountState::kExists, state.GetAccountState(b));
}

TEST(StateTest, AccountsCanBeDeleted) {
  Address a{0x01};

  InMemoryState state;
  EXPECT_EQ(AccountState::kUnknown, state.GetAccountState(a));

  state.CreateAccount(a);
  EXPECT_EQ(AccountState::kExists, state.GetAccountState(a));

  state.DeleteAccount(a);
  EXPECT_EQ(AccountState::kDeleted, state.GetAccountState(a));
}

TEST(StateTest, DeletingAnUnknownAccountDoesNotCreateIt) {
  Address a{0x01};

  InMemoryState state;
  EXPECT_EQ(AccountState::kUnknown, state.GetAccountState(a));

  state.DeleteAccount(a);
  EXPECT_EQ(AccountState::kUnknown, state.GetAccountState(a));
}

TEST(StateTest, DeletedAccountsCanBeRecreated) {
  Address a{0x01};

  InMemoryState state;
  EXPECT_EQ(AccountState::kUnknown, state.GetAccountState(a));
  state.CreateAccount(a);
  state.DeleteAccount(a);
  EXPECT_EQ(AccountState::kDeleted, state.GetAccountState(a));
  state.CreateAccount(a);
  EXPECT_EQ(AccountState::kExists, state.GetAccountState(a));
}

TEST(StateTest, DefaultBalanceIsZero) {
  Address a{0x01};
  Address b{0x02};

  InMemoryState state;
  EXPECT_EQ(Balance{}, state.GetBalance(a));
  EXPECT_EQ(Balance{}, state.GetBalance(b));
}

TEST(StateTest, BalancesCanBeUpdated) {
  Address a{0x01};
  Address b{0x02};
  Balance zero{};

  InMemoryState state;
  EXPECT_EQ(zero, state.GetBalance(a));
  EXPECT_EQ(zero, state.GetBalance(b));

  state.SetBalance(a, Balance{0x12});
  EXPECT_EQ(Balance{0x12}, state.GetBalance(a));
  EXPECT_EQ(zero, state.GetBalance(b));

  state.SetBalance(b, Balance{0x14});
  EXPECT_EQ(Balance{0x12}, state.GetBalance(a));
  EXPECT_EQ(Balance{0x14}, state.GetBalance(b));
}

TEST(StateTest, BalancesAreCoveredByGlobalStateHash) {
  InMemoryState state;
  auto base_hash = state.GetHash();
  state.SetBalance({}, Balance{0x12});
  auto value_12_hash = state.GetHash();
  EXPECT_NE(base_hash, value_12_hash);
  state.SetBalance({}, Balance{0x14});
  auto value_14_hash = state.GetHash();
  EXPECT_NE(base_hash, value_14_hash);

  // Resetting value gets us original hash.
  state.SetBalance({}, Balance{0x12});
  auto value_12_hash_again = state.GetHash();
  EXPECT_EQ(value_12_hash, value_12_hash_again);
}

TEST(StateTest, DefaultNonceIsZero) {
  Address a{0x01};
  Address b{0x02};
  Nonce zero;

  InMemoryState state;
  EXPECT_EQ(zero, state.GetNonce(a));
  EXPECT_EQ(zero, state.GetNonce(b));
}

TEST(StateTest, NoncesCanBeUpdated) {
  Address a{0x01};
  Address b{0x02};
  Nonce zero;

  InMemoryState state;
  EXPECT_EQ(zero, state.GetNonce(a));
  EXPECT_EQ(zero, state.GetNonce(b));

  state.SetNonce(a, Nonce{0x12});
  EXPECT_EQ(Nonce{0x12}, state.GetNonce(a));
  EXPECT_EQ(zero, state.GetNonce(b));

  state.SetNonce(b, Nonce{0x14});
  EXPECT_EQ(Nonce{0x12}, state.GetNonce(a));
  EXPECT_EQ(Nonce{0x14}, state.GetNonce(b));
}

TEST(StateTest, NoncesAreCoveredByGlobalStateHash) {
  InMemoryState state;
  auto base_hash = state.GetHash();
  state.SetNonce({}, Nonce{0x12});
  auto value_12_hash = state.GetHash();
  EXPECT_NE(base_hash, value_12_hash);
  state.SetNonce({}, Nonce{0x14});
  auto value_14_hash = state.GetHash();
  EXPECT_NE(base_hash, value_14_hash);

  // Resetting value gets us original hash.
  state.SetNonce({}, Nonce{0x12});
  auto value_12_hash_again = state.GetHash();
  EXPECT_EQ(value_12_hash, value_12_hash_again);
}

TEST(StateTest, DefaultCodeIsEmpty) {
  Address a{0x01};
  Address b{0x02};

  InMemoryState state;
  EXPECT_THAT(state.GetCode(a), ElementsAre());
  EXPECT_THAT(state.GetCode(b), ElementsAre());
}

TEST(StateTest, CodesCanBeUpdated) {
  Address a{0x01};
  Address b{0x02};
  std::vector<std::byte> code1{std::byte{1}, std::byte{2}};
  std::vector<std::byte> code2{std::byte{1}, std::byte{2}};

  InMemoryState state;
  EXPECT_THAT(state.GetCode(a), ElementsAre());
  EXPECT_THAT(state.GetCode(b), ElementsAre());

  state.SetCode(a, code1);
  EXPECT_THAT(state.GetCode(a), ElementsAreArray(code1));
  EXPECT_THAT(state.GetCode(b), ElementsAre());

  state.SetCode(b, code2);
  EXPECT_THAT(state.GetCode(a), ElementsAreArray(code1));
  EXPECT_THAT(state.GetCode(b), ElementsAreArray(code2));
}

TEST(StateTest, UpdatingCodesUpdatesCodeHashes) {
  Address a{0x01};
  std::vector<std::byte> code{std::byte{1}, std::byte{2}};

  InMemoryState state;

  EXPECT_EQ(state.GetCodeHash(a), Hash{});

  state.SetCode(a, code);
  EXPECT_EQ(state.GetCodeHash(a), GetSha256Hash(std::span(code)));

  // Resetting code to zero reverts code to zero.
  state.SetCode(a, {});
  EXPECT_EQ(state.GetCodeHash(a), Hash{});
}

TEST(StateTest, CodesAreCoveredByGlobalStateHash) {
  InMemoryState state;
  auto base_hash = state.GetHash();
  state.SetCode({}, std::vector{std::byte{12}});
  auto value_12_hash = state.GetHash();
  EXPECT_NE(base_hash, value_12_hash);
  state.SetCode({}, std::vector{std::byte(14)});
  auto value_14_hash = state.GetHash();
  EXPECT_NE(base_hash, value_14_hash);

  // Resetting value gets us original hash.
  state.SetCode({}, std::vector{std::byte{12}});
  auto value_12_hash_again = state.GetHash();
  EXPECT_EQ(value_12_hash, value_12_hash_again);
}

TEST(StateTest, LookingUpMissingCodeDoesNotChangeGlobalHash) {
  Address a{0x01};
  InMemoryState state;
  auto base_hash = state.GetHash();
  state.GetCode(a);
  EXPECT_EQ(state.GetHash(), base_hash);
}

TEST(StateTest, ValuesAddedCanBeRetrieved) {
  Address a;
  Key k;
  Value v{0x01, 0x02};

  InMemoryState state;
  state.SetStorageValue(a, k, v);
  EXPECT_EQ(v, state.GetStorageValue(a, k));

  v = Value{0x03};
  state.SetStorageValue(a, k, v);
  EXPECT_EQ(v, state.GetStorageValue(a, k));
}

}  // namespace carmen
