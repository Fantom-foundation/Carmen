#include "state/state.h"

#include "backend/depot/memory/depot.h"
#include "backend/index/memory/index.h"
#include "backend/store/memory/store.h"
#include "common/account_state.h"
#include "common/memory_usage.h"
#include "common/status_test_util.h"
#include "common/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen {

using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::IsOkAndHolds;
using ::testing::IsOkAndMatches;

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
  EXPECT_THAT(state.GetAccountState(a), IsOkAndHolds(AccountState::kUnknown));
  EXPECT_THAT(state.GetAccountState(b), IsOkAndHolds(AccountState::kUnknown));
}

TEST(StateTest, AccountsCanBeCreatedAndAreDifferentiated) {
  Address a{0x01};
  Address b{0x02};

  InMemoryState state;
  EXPECT_THAT(state.GetAccountState(a), IsOkAndHolds(AccountState::kUnknown));
  EXPECT_THAT(state.GetAccountState(b), IsOkAndHolds(AccountState::kUnknown));

  ASSERT_OK(state.CreateAccount(a));
  EXPECT_THAT(state.GetAccountState(a), IsOkAndHolds(AccountState::kExists));
  EXPECT_THAT(state.GetAccountState(b), IsOkAndHolds(AccountState::kUnknown));

  ASSERT_OK(state.CreateAccount(b));
  EXPECT_THAT(state.GetAccountState(a), IsOkAndHolds(AccountState::kExists));
  EXPECT_THAT(state.GetAccountState(b), IsOkAndHolds(AccountState::kExists));
}

TEST(StateTest, AccountsCanBeDeleted) {
  Address a{0x01};

  InMemoryState state;
  EXPECT_THAT(state.GetAccountState(a), IsOkAndHolds(AccountState::kUnknown));

  ASSERT_OK(state.CreateAccount(a));
  EXPECT_THAT(state.GetAccountState(a), IsOkAndHolds(AccountState::kExists));

  ASSERT_OK(state.DeleteAccount(a));
  EXPECT_THAT(state.GetAccountState(a), IsOkAndHolds(AccountState::kDeleted));
}

TEST(StateTest, DeletingAnUnknownAccountDoesNotCreateIt) {
  Address a{0x01};

  InMemoryState state;
  EXPECT_THAT(state.GetAccountState(a), IsOkAndHolds(AccountState::kUnknown));

  ASSERT_OK(state.DeleteAccount(a));
  EXPECT_THAT(state.GetAccountState(a), IsOkAndHolds(AccountState::kUnknown));
}

TEST(StateTest, DeletedAccountsCanBeRecreated) {
  Address a{0x01};

  InMemoryState state;
  EXPECT_THAT(state.GetAccountState(a), IsOkAndHolds(AccountState::kUnknown));
  ASSERT_OK(state.CreateAccount(a));
  ASSERT_OK(state.DeleteAccount(a));
  EXPECT_THAT(state.GetAccountState(a), IsOkAndHolds(AccountState::kDeleted));
  ASSERT_OK(state.CreateAccount(a));
  EXPECT_THAT(state.GetAccountState(a), IsOkAndHolds(AccountState::kExists));
}

TEST(StateTest, DefaultBalanceIsZero) {
  Address a{0x01};
  Address b{0x02};

  InMemoryState state;
  EXPECT_THAT(state.GetBalance(a), IsOkAndHolds(Balance{}));
  EXPECT_THAT(state.GetBalance(b), IsOkAndHolds(Balance{}));
}

TEST(StateTest, BalancesCanBeUpdated) {
  Address a{0x01};
  Address b{0x02};
  Balance zero{};

  InMemoryState state;
  EXPECT_THAT(state.GetBalance(a), IsOkAndHolds(zero));
  EXPECT_THAT(state.GetBalance(b), IsOkAndHolds(zero));

  ASSERT_OK(state.SetBalance(a, Balance{0x12}));
  EXPECT_THAT(state.GetBalance(a), IsOkAndHolds(Balance{0x12}));
  EXPECT_THAT(state.GetBalance(b), IsOkAndHolds(zero));

  ASSERT_OK(state.SetBalance(b, Balance{0x14}));
  EXPECT_THAT(state.GetBalance(a), IsOkAndHolds(Balance{0x12}));
  EXPECT_THAT(state.GetBalance(b), IsOkAndHolds(Balance{0x14}));
}

TEST(StateTest, BalancesAreCoveredByGlobalStateHash) {
  InMemoryState state;
  ASSERT_OK_AND_ASSIGN(auto base_hash, state.GetHash());
  ASSERT_OK(state.SetBalance({}, Balance{0x12}));
  ASSERT_OK_AND_ASSIGN(auto value_12_hash, state.GetHash());
  EXPECT_NE(base_hash, value_12_hash);
  ASSERT_OK(state.SetBalance({}, Balance{0x14}));
  ASSERT_OK_AND_ASSIGN(auto value_14_hash, state.GetHash());
  EXPECT_NE(base_hash, value_14_hash);

  // Resetting value gets us original hash.
  ASSERT_OK(state.SetBalance({}, Balance{0x12}));
  ASSERT_OK_AND_ASSIGN(auto value_12_hash_again, state.GetHash());
  EXPECT_EQ(value_12_hash, value_12_hash_again);
}

TEST(StateTest, DefaultNonceIsZero) {
  Address a{0x01};
  Address b{0x02};
  Nonce zero;

  InMemoryState state;
  EXPECT_THAT(state.GetNonce(a), IsOkAndHolds(zero));
  EXPECT_THAT(state.GetNonce(b), IsOkAndHolds(zero));
}

TEST(StateTest, NoncesCanBeUpdated) {
  Address a{0x01};
  Address b{0x02};
  Nonce zero;

  InMemoryState state;
  EXPECT_THAT(state.GetNonce(a), IsOkAndHolds(zero));
  EXPECT_THAT(state.GetNonce(b), IsOkAndHolds(zero));

  ASSERT_OK(state.SetNonce(a, Nonce{0x12}));
  EXPECT_THAT(state.GetNonce(a), IsOkAndHolds(Nonce{0x12}));
  EXPECT_THAT(state.GetNonce(b), IsOkAndHolds(zero));

  ASSERT_OK(state.SetNonce(b, Nonce{0x14}));
  EXPECT_THAT(state.GetNonce(a), IsOkAndHolds(Nonce{0x12}));
  EXPECT_THAT(state.GetNonce(b), IsOkAndHolds(Nonce{0x14}));
}

TEST(StateTest, NoncesAreCoveredByGlobalStateHash) {
  InMemoryState state;
  ASSERT_OK_AND_ASSIGN(auto base_hash, state.GetHash());
  ASSERT_OK(state.SetNonce({}, Nonce{0x12}));
  ASSERT_OK_AND_ASSIGN(auto value_12_hash, state.GetHash());
  EXPECT_NE(base_hash, value_12_hash);
  ASSERT_OK(state.SetNonce({}, Nonce{0x14}));
  ASSERT_OK_AND_ASSIGN(auto value_14_hash, state.GetHash());
  EXPECT_NE(base_hash, value_14_hash);

  // Resetting value gets us original hash.
  ASSERT_OK(state.SetNonce({}, Nonce{0x12}));
  ASSERT_OK_AND_ASSIGN(auto value_12_hash_again, state.GetHash());
  EXPECT_EQ(value_12_hash, value_12_hash_again);
}

TEST(StateTest, DefaultCodeIsEmpty) {
  Address a{0x01};
  Address b{0x02};

  InMemoryState state;
  EXPECT_THAT(state.GetCode(a), IsOkAndMatches(ElementsAre()));
  EXPECT_THAT(state.GetCode(b), IsOkAndMatches(ElementsAre()));
}

TEST(StateTest, CodesCanBeUpdated) {
  Address a{0x01};
  Address b{0x02};
  std::vector<std::byte> code1{std::byte{1}, std::byte{2}};
  std::vector<std::byte> code2{std::byte{1}, std::byte{2}};

  InMemoryState state;
  EXPECT_THAT(state.GetCode(a), IsOkAndMatches(ElementsAre()));
  EXPECT_THAT(state.GetCode(b), IsOkAndMatches(ElementsAre()));

  ASSERT_OK(state.SetCode(a, code1));
  EXPECT_THAT(state.GetCode(a), IsOkAndMatches(ElementsAreArray(code1)));
  EXPECT_THAT(state.GetCode(b), IsOkAndMatches(ElementsAre()));

  ASSERT_OK(state.SetCode(b, code2));
  EXPECT_THAT(state.GetCode(a), IsOkAndMatches(ElementsAreArray(code1)));
  EXPECT_THAT(state.GetCode(b), IsOkAndMatches(ElementsAreArray(code2)));
}

TEST(StateTest, UpdatingCodesUpdatesCodeHashes) {
  const Hash hash_of_empty_code = GetKeccak256Hash({});

  Address a{0x01};
  std::vector<std::byte> code{std::byte{1}, std::byte{2}};

  InMemoryState state;
  EXPECT_THAT(state.GetCodeHash(a), IsOkAndHolds(hash_of_empty_code));

  ASSERT_OK(state.SetCode(a, code));
  EXPECT_THAT(state.GetCodeHash(a),
              IsOkAndHolds(GetKeccak256Hash(std::span(code))));

  // Resetting code to zero updates the hash accordingly.
  ASSERT_OK(state.SetCode(a, {}));
  EXPECT_THAT(state.GetCodeHash(a), IsOkAndHolds(hash_of_empty_code));
}

TEST(StateTest, CodesAreCoveredByGlobalStateHash) {
  InMemoryState state;
  ASSERT_OK_AND_ASSIGN(auto base_hash, state.GetHash());
  ASSERT_OK(state.SetCode({}, std::vector{std::byte{12}}));
  ASSERT_OK_AND_ASSIGN(auto value_12_hash, state.GetHash());
  EXPECT_NE(base_hash, value_12_hash);
  ASSERT_OK(state.SetCode({}, std::vector{std::byte{14}}));
  ASSERT_OK_AND_ASSIGN(auto value_14_hash, state.GetHash());
  EXPECT_NE(base_hash, value_14_hash);

  // Resetting value gets us original hash.
  ASSERT_OK(state.SetCode({}, std::vector{std::byte{12}}));
  ASSERT_OK_AND_ASSIGN(auto value_12_hash_again, state.GetHash());
  EXPECT_EQ(value_12_hash, value_12_hash_again);
}

TEST(StateTest, LookingUpMissingCodeDoesNotChangeGlobalHash) {
  Address a{0x01};
  InMemoryState state;
  ASSERT_OK_AND_ASSIGN(auto base_hash, state.GetHash());
  ASSERT_OK(state.GetCode(a));
  ASSERT_THAT(state.GetHash(), IsOkAndHolds(base_hash));
}

TEST(StateTest, ValuesAddedCanBeRetrieved) {
  Address a;
  Key k;
  Value v{0x01, 0x02};

  InMemoryState state;
  ASSERT_OK(state.SetStorageValue(a, k, v));
  ASSERT_THAT(state.GetStorageValue(a, k), IsOkAndHolds(v));

  v = Value{0x03};
  ASSERT_OK(state.SetStorageValue(a, k, v));
  ASSERT_THAT(state.GetStorageValue(a, k), IsOkAndHolds(v));
}

TEST(StateTest, CanProduceAMemoryFootprint) {
  InMemoryState state;
  auto usage = state.GetMemoryFootprint();
  EXPECT_GT(usage.GetTotal(), Memory());
}

}  // namespace carmen
