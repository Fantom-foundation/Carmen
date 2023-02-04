#include "state/state.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/depot/test_util.h"
#include "backend/index/test_util.h"
#include "backend/multimap/test_util.h"
#include "backend/store/test_util.h"
#include "common/account_state.h"
#include "common/file_util.h"
#include "common/status_test_util.h"
#include "common/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "state/configurations.h"

namespace carmen {

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::IsEmpty;
using ::testing::IsOkAndHolds;
using ::testing::Return;
using ::testing::StatusIs;

template <typename T>
class StateTest : public testing::Test {};

TYPED_TEST_SUITE_P(StateTest);

TYPED_TEST_P(StateTest, DefaultAccountStateIsUnknown) {
  Address a{0x01};
  Address b{0x02};

  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto state, TypeParam::Open(dir));
  EXPECT_THAT(state.GetAccountState(a), IsOkAndHolds(AccountState::kUnknown));
  EXPECT_THAT(state.GetAccountState(b), IsOkAndHolds(AccountState::kUnknown));
}

TYPED_TEST_P(StateTest, AccountsCanBeCreatedAndAreDifferentiated) {
  Address a{0x01};
  Address b{0x02};

  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto state, TypeParam::Open(dir));
  EXPECT_THAT(state.GetAccountState(a), IsOkAndHolds(AccountState::kUnknown));
  EXPECT_THAT(state.GetAccountState(b), IsOkAndHolds(AccountState::kUnknown));

  EXPECT_OK(state.CreateAccount(a));
  EXPECT_THAT(state.GetAccountState(a), IsOkAndHolds(AccountState::kExists));
  EXPECT_THAT(state.GetAccountState(b), IsOkAndHolds(AccountState::kUnknown));

  EXPECT_OK(state.CreateAccount(b));
  EXPECT_THAT(state.GetAccountState(a), IsOkAndHolds(AccountState::kExists));
  EXPECT_THAT(state.GetAccountState(b), IsOkAndHolds(AccountState::kExists));
}

TYPED_TEST_P(StateTest, AccountsCanBeDeleted) {
  Address a{0x01};

  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto state, TypeParam::Open(dir));
  EXPECT_THAT(state.GetAccountState(a), IsOkAndHolds(AccountState::kUnknown));

  EXPECT_OK(state.CreateAccount(a));
  EXPECT_THAT(state.GetAccountState(a), IsOkAndHolds(AccountState::kExists));

  EXPECT_OK(state.DeleteAccount(a));
  EXPECT_THAT(state.GetAccountState(a), IsOkAndHolds(AccountState::kDeleted));
}

TYPED_TEST_P(StateTest, DeletingAnUnknownAccountDoesNotCreateIt) {
  Address a{0x01};

  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto state, TypeParam::Open(dir));
  EXPECT_THAT(state.GetAccountState(a), IsOkAndHolds(AccountState::kUnknown));

  EXPECT_OK(state.DeleteAccount(a));
  EXPECT_THAT(state.GetAccountState(a), IsOkAndHolds(AccountState::kUnknown));
}

TYPED_TEST_P(StateTest, DeletedAccountsCanBeRecreated) {
  Address a{0x01};

  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto state, TypeParam::Open(dir));
  EXPECT_THAT(state.GetAccountState(a), IsOkAndHolds(AccountState::kUnknown));
  EXPECT_OK(state.CreateAccount(a));
  EXPECT_OK(state.DeleteAccount(a));
  EXPECT_THAT(state.GetAccountState(a), IsOkAndHolds(AccountState::kDeleted));
  EXPECT_OK(state.CreateAccount(a));
  EXPECT_THAT(state.GetAccountState(a), IsOkAndHolds(AccountState::kExists));
}

TYPED_TEST_P(StateTest, DeletingAnAccountDeletesItsStorage) {
  Address a{0x01};
  Key k{0x01, 0x02};
  Value v{0x02, 0x03, 0x04};

  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto state, TypeParam::Open(dir));

  EXPECT_OK(state.CreateAccount(a));
  EXPECT_THAT(state.GetStorageValue(a, k), IsOkAndHolds(Value{}));
  EXPECT_OK(state.SetStorageValue(a, k, v));
  EXPECT_THAT(state.GetStorageValue(a, k), IsOkAndHolds(v));

  EXPECT_OK(state.DeleteAccount(a));
  EXPECT_THAT(state.GetStorageValue(a, k), IsOkAndHolds(Value{}));
}

TYPED_TEST_P(StateTest, DefaultBalanceIsZero) {
  Address a{0x01};
  Address b{0x02};

  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto state, TypeParam::Open(dir));
  EXPECT_THAT(state.GetBalance(a), IsOkAndHolds(Balance{}));
  EXPECT_THAT(state.GetBalance(b), IsOkAndHolds(Balance{}));
}

TYPED_TEST_P(StateTest, BalancesCanBeUpdated) {
  Address a{0x01};
  Address b{0x02};
  Balance zero{};

  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto state, TypeParam::Open(dir));
  EXPECT_THAT(state.GetBalance(a), IsOkAndHolds(zero));
  EXPECT_THAT(state.GetBalance(b), IsOkAndHolds(zero));

  EXPECT_OK(state.SetBalance(a, Balance{0x12}));
  EXPECT_THAT(state.GetBalance(a), IsOkAndHolds(Balance{0x12}));
  EXPECT_THAT(state.GetBalance(b), IsOkAndHolds(zero));

  EXPECT_OK(state.SetBalance(b, Balance{0x14}));
  EXPECT_THAT(state.GetBalance(a), IsOkAndHolds(Balance{0x12}));
  EXPECT_THAT(state.GetBalance(b), IsOkAndHolds(Balance{0x14}));
}

TYPED_TEST_P(StateTest, BalancesAreCoveredByGlobalStateHash) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto state, TypeParam::Open(dir));
  ASSERT_OK_AND_ASSIGN(auto base_hash, state.GetHash());
  EXPECT_OK(state.SetBalance({}, Balance{0x12}));
  ASSERT_OK_AND_ASSIGN(auto value_12_hash, state.GetHash());
  EXPECT_NE(base_hash, value_12_hash);
  EXPECT_OK(state.SetBalance({}, Balance{0x14}));
  ASSERT_OK_AND_ASSIGN(auto value_14_hash, state.GetHash());
  EXPECT_NE(base_hash, value_14_hash);

  // Resetting value gets us original hash.
  EXPECT_OK(state.SetBalance({}, Balance{0x12}));
  EXPECT_THAT(state.GetHash(), IsOkAndHolds(value_12_hash));
}

TYPED_TEST_P(StateTest, DefaultNonceIsZero) {
  Address a{0x01};
  Address b{0x02};
  Nonce zero;

  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto state, TypeParam::Open(dir));
  EXPECT_THAT(state.GetNonce(a), IsOkAndHolds(zero));
  EXPECT_THAT(state.GetNonce(b), IsOkAndHolds(zero));
}

TYPED_TEST_P(StateTest, NoncesCanBeUpdated) {
  Address a{0x01};
  Address b{0x02};
  Nonce zero;

  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto state, TypeParam::Open(dir));
  EXPECT_THAT(state.GetNonce(a), IsOkAndHolds(zero));
  EXPECT_THAT(state.GetNonce(b), IsOkAndHolds(zero));

  EXPECT_OK(state.SetNonce(a, Nonce{0x12}));
  EXPECT_THAT(state.GetNonce(a), IsOkAndHolds(Nonce{0x12}));
  EXPECT_THAT(state.GetNonce(b), IsOkAndHolds(zero));

  EXPECT_OK(state.SetNonce(b, Nonce{0x14}));
  EXPECT_THAT(state.GetNonce(a), IsOkAndHolds(Nonce{0x12}));
  EXPECT_THAT(state.GetNonce(b), IsOkAndHolds(Nonce{0x14}));
}

TYPED_TEST_P(StateTest, NoncesAreCoveredByGlobalStateHash) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto state, TypeParam::Open(dir));
  ASSERT_OK_AND_ASSIGN(auto base_hash, state.GetHash());
  EXPECT_OK(state.SetNonce({}, Nonce{0x12}));
  ASSERT_OK_AND_ASSIGN(auto value_12_hash, state.GetHash());
  EXPECT_NE(base_hash, value_12_hash);
  EXPECT_OK(state.SetNonce({}, Nonce{0x14}));
  ASSERT_OK_AND_ASSIGN(auto value_14_hash, state.GetHash());
  EXPECT_NE(base_hash, value_14_hash);

  // Resetting value gets us original hash.
  EXPECT_OK(state.SetNonce({}, Nonce{0x12}));
  EXPECT_THAT(state.GetHash(), IsOkAndHolds(value_12_hash));
}

TYPED_TEST_P(StateTest, DefaultCodeIsEmpty) {
  Address a{0x01};
  Address b{0x02};

  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto state, TypeParam::Open(dir));
  EXPECT_THAT(state.GetCode(a), IsOkAndHolds(ElementsAre()));
  EXPECT_THAT(state.GetCode(b), IsOkAndHolds(ElementsAre()));
}

TYPED_TEST_P(StateTest, CodesCanBeUpdated) {
  Address a{0x01};
  Address b{0x02};
  std::vector<std::byte> code1{std::byte{1}, std::byte{2}};
  std::vector<std::byte> code2{std::byte{1}, std::byte{2}};

  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto state, TypeParam::Open(dir));
  EXPECT_THAT(state.GetCode(a), IsOkAndHolds(ElementsAre()));
  EXPECT_THAT(state.GetCode(b), IsOkAndHolds(ElementsAre()));

  EXPECT_OK(state.SetCode(a, code1));
  EXPECT_THAT(state.GetCode(a), IsOkAndHolds(ElementsAreArray(code1)));
  EXPECT_THAT(state.GetCode(b), IsOkAndHolds(ElementsAre()));

  EXPECT_OK(state.SetCode(b, code2));
  EXPECT_THAT(state.GetCode(a), IsOkAndHolds(ElementsAreArray(code1)));
  EXPECT_THAT(state.GetCode(b), IsOkAndHolds(ElementsAreArray(code2)));
}

TYPED_TEST_P(StateTest, UpdatingCodesUpdatesCodeHashes) {
  const Hash hash_of_empty_code = GetKeccak256Hash({});

  Address a{0x01};
  std::vector<std::byte> code{std::byte{1}, std::byte{2}};

  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto state, TypeParam::Open(dir));
  EXPECT_THAT(state.GetCodeHash(a), IsOkAndHolds(hash_of_empty_code));

  EXPECT_OK(state.SetCode(a, code));
  EXPECT_THAT(state.GetCodeHash(a),
              IsOkAndHolds(GetKeccak256Hash(std::span(code))));

  // Resetting code to zero updates the hash accordingly.
  EXPECT_OK(state.SetCode(a, {}));
  EXPECT_THAT(state.GetCodeHash(a), IsOkAndHolds(hash_of_empty_code));
}

TYPED_TEST_P(StateTest, CodesAreCoveredByGlobalStateHash) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto state, TypeParam::Open(dir));
  ASSERT_OK_AND_ASSIGN(auto base_hash, state.GetHash());
  EXPECT_OK(state.SetCode({}, std::vector{std::byte{12}}));
  ASSERT_OK_AND_ASSIGN(auto value_12_hash, state.GetHash());
  EXPECT_NE(base_hash, value_12_hash);
  EXPECT_OK(state.SetCode({}, std::vector{std::byte{14}}));
  ASSERT_OK_AND_ASSIGN(auto value_14_hash, state.GetHash());
  EXPECT_NE(base_hash, value_14_hash);

  // Resetting value gets us original hash.
  EXPECT_OK(state.SetCode({}, std::vector{std::byte{12}}));
  EXPECT_THAT(state.GetHash(), IsOkAndHolds(value_12_hash));
}

TYPED_TEST_P(StateTest, LookingUpMissingCodeDoesNotChangeGlobalHash) {
  Address a{0x01};
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto state, TypeParam::Open(dir));
  ASSERT_OK_AND_ASSIGN(auto base_hash, state.GetHash());
  EXPECT_OK(state.GetCode(a));
  EXPECT_THAT(state.GetHash(), IsOkAndHolds(base_hash));
}

TYPED_TEST_P(StateTest, ValuesAddedCanBeRetrieved) {
  Address a;
  Key k;
  Value v{0x01, 0x02};

  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto state, TypeParam::Open(dir));
  EXPECT_OK(state.SetStorageValue(a, k, v));
  EXPECT_THAT(state.GetStorageValue(a, k), IsOkAndHolds(v));

  v = Value{0x03};
  EXPECT_OK(state.SetStorageValue(a, k, v));
  EXPECT_THAT(state.GetStorageValue(a, k), IsOkAndHolds(v));
}

TYPED_TEST_P(StateTest, UpdatesCanBeApplied) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto state, TypeParam::Open(dir));
  ASSERT_OK(state.CreateAccount(Address{0x02}));

  Update update;
  update.Create(Address{0x01});
  update.Delete(Address{0x02});
  update.Set(Address{0x03}, Balance{0xB1});
  update.Set(Address{0x04}, Nonce{0xA1});
  update.Set(Address{0x05}, Key{0x06}, Value{0x07});
  update.Set(Address{0x06}, Code{0x01, 0x02});

  EXPECT_OK(state.Apply(12, update));

  EXPECT_THAT(state.GetAccountState(Address{0x00}), AccountState::kUnknown);
  EXPECT_THAT(state.GetAccountState(Address{0x01}), AccountState::kExists);
  EXPECT_THAT(state.GetAccountState(Address{0x02}), AccountState::kDeleted);

  EXPECT_THAT(state.GetBalance(Address{0x03}), IsOkAndHolds(Balance{0xB1}));
  EXPECT_THAT(state.GetNonce(Address{0x04}), IsOkAndHolds(Nonce{0xA1}));
  EXPECT_THAT(state.GetStorageValue(Address{0x05}, Key{0x06}),
              IsOkAndHolds(Value{0x07}));
  EXPECT_THAT(state.GetCode(Address{0x06}),
              IsOkAndHolds(ElementsAre(std::byte{0x01}, std::byte{0x02})));
}

TYPED_TEST_P(StateTest, CanProduceAMemoryFootprint) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto state, TypeParam::Open(dir));
  auto usage = state.GetMemoryFootprint();
  EXPECT_GT(usage.GetTotal(), Memory());
}

REGISTER_TYPED_TEST_SUITE_P(
    StateTest, AccountsCanBeDeleted, AccountsCanBeCreatedAndAreDifferentiated,
    BalancesAreCoveredByGlobalStateHash, BalancesCanBeUpdated,
    CodesAreCoveredByGlobalStateHash, CodesCanBeUpdated,
    DefaultAccountStateIsUnknown, DefaultBalanceIsZero, DefaultCodeIsEmpty,
    DefaultNonceIsZero, DeletedAccountsCanBeRecreated,
    DeletingAnAccountDeletesItsStorage, DeletingAnUnknownAccountDoesNotCreateIt,
    LookingUpMissingCodeDoesNotChangeGlobalHash,
    NoncesAreCoveredByGlobalStateHash, NoncesCanBeUpdated,
    UpdatingCodesUpdatesCodeHashes, ValuesAddedCanBeRetrieved,
    UpdatesCanBeApplied, CanProduceAMemoryFootprint);

using StateConfigurations =
    ::testing::Types<InMemoryState, FileBasedState, LevelDbBasedState>;

INSTANTIATE_TYPED_TEST_SUITE_P(Config, StateTest, StateConfigurations);

template <typename K, typename V>
using MockIndex = backend::index::MockIndex<K, V>;

template <typename K, typename V>
using MockStore = backend::store::MockStoreWrapper<K, V, kPageSize>;

template <typename K>
using MockDepot = backend::depot::MockDepotWrapper<K>;

template <typename K, typename V>
using MockMultiMap = backend::multimap::MockMultiMap<K, V>;

using MockState = State<MockIndex, MockStore, MockDepot, MockMultiMap>;

// A test fixture for the State class. It provides a State instance with
// mocked dependencies. The dependencies are exposed through getters.
class MockStateTest : public ::testing::Test {
 public:
  auto& GetState() { return state_; }

 private:
  class Mock : public MockState {
   public:
    Mock()
        : MockState(MockIndex<Address, AddressId>(), MockIndex<Key, KeyId>(),
                    MockIndex<Slot, SlotId>(), MockStore<AddressId, Balance>(),
                    MockStore<AddressId, Nonce>(), MockStore<SlotId, Value>(),
                    MockStore<AddressId, AccountState>(),
                    MockDepot<AddressId>(), MockStore<AddressId, Hash>(),
                    MockMultiMap<AddressId, SlotId>()) {}
    auto& GetAddressIndex() { return this->address_index_.GetMockIndex(); }
    auto& GetKeyIndex() { return this->key_index_.GetMockIndex(); }
    auto& GetSlotIndex() { return this->slot_index_.GetMockIndex(); }
    auto& GetBalancesStore() { return this->balances_.GetMockStore(); }
    auto& GetNoncesStore() { return this->nonces_.GetMockStore(); }
    auto& GetValueStore() { return this->value_store_.GetMockStore(); }
    auto& GetAccountStatesStore() {
      return this->account_states_.GetMockStore();
    }
    auto& GetCodesDepot() { return this->codes_.GetMockDepot(); }
    auto& GetCodeHashesStore() { return this->code_hashes_.GetMockStore(); }
    auto& GetAddressToSlotsMap() {
      return this->address_to_slots_.GetMockMultiMap();
    }
    auto GetEmptyCodeHash() const { return this->kEmptyCodeHash; }
  };
  Mock state_{};
};

TEST_F(MockStateTest, CreateAccountErrorIsForwarded) {
  auto& state = GetState();

  EXPECT_CALL(state.GetAddressIndex(), GetOrAdd(_))
      .WillOnce(Return(absl::InternalError("Address index error")))
      .WillRepeatedly(Return(
          absl::StatusOr<std::pair<MockState::AddressId, bool>>({1, true})));
  EXPECT_THAT(state.CreateAccount(Address{}),
              StatusIs(absl::StatusCode::kInternal, "Address index error"));

  EXPECT_CALL(state.GetAccountStatesStore(), Set(_, _))
      .WillOnce(Return(absl::InternalError("Account state store error")));
  EXPECT_THAT(
      state.CreateAccount(Address{}),
      StatusIs(absl::StatusCode::kInternal, "Account state store error"));
}

TEST_F(MockStateTest, GetAccountStateNotFoundErrorIsHandled) {
  auto& state = GetState();

  EXPECT_CALL(state.GetAddressIndex(), Get(_))
      .WillOnce(Return(absl::NotFoundError("Address not found")));
  EXPECT_THAT(state.GetAccountState(Address{}), AccountState::kUnknown);
}

TEST_F(MockStateTest, GetAccountStateErrorIsForwarded) {
  auto& state = GetState();

  EXPECT_CALL(state.GetAddressIndex(), Get(_))
      .WillOnce(Return(absl::InternalError("Address index error")))
      .WillRepeatedly(Return(absl::StatusOr<MockState::AddressId>(1)));
  EXPECT_THAT(state.GetAccountState(Address{}),
              StatusIs(absl::StatusCode::kInternal, "Address index error"));

  EXPECT_CALL(state.GetAccountStatesStore(), Get(_))
      .WillOnce(Return(absl::InternalError("Account state store error")));
  EXPECT_THAT(
      state.GetAccountState(Address{}),
      StatusIs(absl::StatusCode::kInternal, "Account state store error"));
}

TEST_F(MockStateTest, DeleteAccountNotFoundErrorIsHandled) {
  auto& state = GetState();

  EXPECT_CALL(state.GetAddressIndex(), Get(_))
      .WillOnce(Return(absl::NotFoundError("Address not found")));
  EXPECT_OK(state.DeleteAccount(Address{}));
}

TEST_F(MockStateTest, DeleteAccountErrorIsForwarded) {
  auto& state = GetState();

  EXPECT_CALL(state.GetAddressIndex(), Get(_))
      .WillOnce(Return(absl::InternalError("Address index error")))
      .WillRepeatedly(Return(absl::StatusOr<MockState::AddressId>(1)));
  EXPECT_THAT(state.DeleteAccount(Address{}),
              StatusIs(absl::StatusCode::kInternal, "Address index error"));

  EXPECT_CALL(state.GetAccountStatesStore(), Set(_, _))
      .WillOnce(Return(absl::InternalError("Account state store error")))
      .WillRepeatedly(Return(absl::OkStatus()));
  EXPECT_THAT(
      state.DeleteAccount(Address{}),
      StatusIs(absl::StatusCode::kInternal, "Account state store error"));

  EXPECT_CALL(state.GetAddressToSlotsMap(), ForEach(_, _))
      .WillOnce(Return(absl::InternalError("Address to slot multimap error")))
      .WillOnce([](const MockState::AddressId& id,
                   const std::function<void(std::uint32_t)>& op) {
        op(id);
        return absl::OkStatus();
      })
      .WillRepeatedly(Return(absl::OkStatus()));
  EXPECT_THAT(
      state.DeleteAccount(Address{}),
      StatusIs(absl::StatusCode::kInternal, "Address to slot multimap error"));

  // return value store error inside ForEach callback
  EXPECT_CALL(state.GetValueStore(), Set(_, _))
      .WillOnce(Return(absl::InternalError("Value store error")));
  EXPECT_THAT(state.DeleteAccount(Address{}),
              StatusIs(absl::StatusCode::kInternal, "Value store error"));

  EXPECT_CALL(state.GetAddressToSlotsMap(), Erase(_))
      .WillOnce(Return(absl::InternalError("Address to slot multimap error")));
  EXPECT_THAT(
      state.DeleteAccount(Address{}),
      StatusIs(absl::StatusCode::kInternal, "Address to slot multimap error"));
}

TEST_F(MockStateTest, GetBalanceNotFoundErrorIsHandled) {
  auto& state = GetState();

  EXPECT_CALL(state.GetAddressIndex(), Get(_))
      .WillOnce(Return(absl::NotFoundError("Address not found")));
  EXPECT_THAT(state.GetBalance(Address{}), IsOkAndHolds(Balance{}));
}

TEST_F(MockStateTest, GetBalanceErrorIsForwarded) {
  auto& state = GetState();

  EXPECT_CALL(state.GetAddressIndex(), Get(_))
      .WillOnce(Return(absl::InternalError("Address index error")))
      .WillRepeatedly(Return(absl::StatusOr<MockState::AddressId>(1)));
  EXPECT_THAT(state.GetBalance(Address{}),
              StatusIs(absl::StatusCode::kInternal, "Address index error"));

  EXPECT_CALL(state.GetBalancesStore(), Get(_))
      .WillOnce(Return(absl::InternalError("Balance store error")));
  EXPECT_THAT(state.GetBalance(Address{}),
              StatusIs(absl::StatusCode::kInternal, "Balance store error"));
}

TEST_F(MockStateTest, SetBalanceErrorIsForwarded) {
  auto& state = GetState();

  EXPECT_CALL(state.GetAddressIndex(), GetOrAdd(_))
      .WillOnce(Return(absl::InternalError("Address index error")))
      .WillRepeatedly(Return(
          absl::StatusOr<std::pair<MockState::AddressId, bool>>({1, true})));
  EXPECT_THAT(state.SetBalance(Address{}, Balance{}),
              StatusIs(absl::StatusCode::kInternal, "Address index error"));

  EXPECT_CALL(state.GetBalancesStore(), Set(_, _))
      .WillOnce(Return(absl::InternalError("Balance store error")));
  EXPECT_THAT(state.SetBalance(Address{}, Balance{}),
              StatusIs(absl::StatusCode::kInternal, "Balance store error"));
}

TEST_F(MockStateTest, GetNonceNotFoundErrorIsHandled) {
  auto& state = GetState();

  EXPECT_CALL(state.GetAddressIndex(), Get(_))
      .WillOnce(Return(absl::NotFoundError("Address not found")));
  EXPECT_THAT(state.GetNonce(Address{}), IsOkAndHolds(Nonce{}));
}

TEST_F(MockStateTest, GetNonceErrorIsForwarded) {
  auto& state = GetState();

  EXPECT_CALL(state.GetAddressIndex(), Get(_))
      .WillOnce(Return(absl::InternalError("Address index error")))
      .WillRepeatedly(Return(absl::StatusOr<MockState::AddressId>(1)));
  EXPECT_THAT(state.GetNonce(Address{}),
              StatusIs(absl::StatusCode::kInternal, "Address index error"));

  EXPECT_CALL(state.GetNoncesStore(), Get(_))
      .WillOnce(Return(absl::InternalError("Nonces store error")));
  EXPECT_THAT(state.GetNonce(Address{}),
              StatusIs(absl::StatusCode::kInternal, "Nonces store error"));
}

TEST_F(MockStateTest, SetNonceErrorIsForwarded) {
  auto& state = GetState();

  EXPECT_CALL(state.GetAddressIndex(), GetOrAdd(_))
      .WillOnce(Return(absl::InternalError("Address index error")))
      .WillRepeatedly(Return(
          absl::StatusOr<std::pair<MockState::AddressId, bool>>({1, true})));
  EXPECT_THAT(state.SetNonce(Address{}, Nonce{}),
              StatusIs(absl::StatusCode::kInternal, "Address index error"));

  EXPECT_CALL(state.GetNoncesStore(), Set(_, _))
      .WillOnce(Return(absl::InternalError("Nonces store error")));
  EXPECT_THAT(state.SetNonce(Address{}, Nonce{}),
              StatusIs(absl::StatusCode::kInternal, "Nonces store error"));
}

TEST_F(MockStateTest, GeStorageValueNotFoundErrorIsHandled) {
  auto& state = GetState();

  EXPECT_CALL(state.GetAddressIndex(), Get(_))
      .WillOnce(Return(absl::NotFoundError("Address not found")))
      .WillRepeatedly(Return(absl::StatusOr<MockState::AddressId>(1)));
  EXPECT_THAT(state.GetStorageValue(Address{}, Key{}), IsOkAndHolds(Value{}));

  EXPECT_CALL(state.GetKeyIndex(), Get(_))
      .WillOnce(Return(absl::NotFoundError("Key not found")))
      .WillRepeatedly(Return(absl::StatusOr<MockState::KeyId>(1)));
  EXPECT_THAT(state.GetStorageValue(Address{}, Key{}), IsOkAndHolds(Value{}));

  EXPECT_CALL(state.GetSlotIndex(), Get(_))
      .WillOnce(Return(absl::NotFoundError("Slot not found")))
      .WillRepeatedly(Return(absl::StatusOr<MockState::SlotId>(1)));
  EXPECT_THAT(state.GetStorageValue(Address{}, Key{}), IsOkAndHolds(Value{}));
}

TEST_F(MockStateTest, GetStorageValueErrorIsForwarded) {
  auto& state = GetState();

  EXPECT_CALL(state.GetAddressIndex(), Get(_))
      .WillOnce(Return(absl::InternalError("Address index error")))
      .WillRepeatedly(Return(absl::StatusOr<MockState::AddressId>(1)));
  EXPECT_THAT(state.GetStorageValue(Address{}, Key{}),
              StatusIs(absl::StatusCode::kInternal, "Address index error"));

  EXPECT_CALL(state.GetKeyIndex(), Get(_))
      .WillOnce(Return(absl::InternalError("Key index error")))
      .WillRepeatedly(Return(absl::StatusOr<MockState::KeyId>(1)));
  EXPECT_THAT(state.GetStorageValue(Address{}, Key{}),
              StatusIs(absl::StatusCode::kInternal, "Key index error"));

  EXPECT_CALL(state.GetSlotIndex(), Get(_))
      .WillOnce(Return(absl::InternalError("Slot index error")))
      .WillRepeatedly(Return(absl::StatusOr<MockState::SlotId>(1)));
  EXPECT_THAT(state.GetStorageValue(Address{}, Key{}),
              StatusIs(absl::StatusCode::kInternal, "Slot index error"));

  EXPECT_CALL(state.GetValueStore(), Get(_))
      .WillOnce(Return(absl::InternalError("Values store error")));
  EXPECT_THAT(state.GetStorageValue(Address{}, Key{}),
              StatusIs(absl::StatusCode::kInternal, "Values store error"));
}

TEST_F(MockStateTest, SetStorageValueErrorIsForwarded) {
  auto& state = GetState();

  EXPECT_CALL(state.GetAddressIndex(), GetOrAdd(_))
      .WillOnce(Return(absl::InternalError("Address index error")))
      .WillRepeatedly(Return(
          absl::StatusOr<std::pair<MockState::AddressId, bool>>({1, true})));
  EXPECT_THAT(state.SetStorageValue(Address{}, Key{}, Value{}),
              StatusIs(absl::StatusCode::kInternal, "Address index error"));

  EXPECT_CALL(state.GetKeyIndex(), GetOrAdd(_))
      .WillOnce(Return(absl::InternalError("Key index error")))
      .WillRepeatedly(
          Return(absl::StatusOr<std::pair<MockState::KeyId, bool>>({1, true})));
  EXPECT_THAT(state.SetStorageValue(Address{}, Key{}, Value{}),
              StatusIs(absl::StatusCode::kInternal, "Key index error"));

  EXPECT_CALL(state.GetSlotIndex(), GetOrAdd(_))
      .WillOnce(Return(absl::InternalError("Slot index error")))
      .WillRepeatedly(Return(
          absl::StatusOr<std::pair<MockState::SlotId, bool>>({1, true})));
  EXPECT_THAT(state.SetStorageValue(Address{}, Key{}, Value{}),
              StatusIs(absl::StatusCode::kInternal, "Slot index error"));

  EXPECT_CALL(state.GetValueStore(), Set(_, _))
      .WillOnce(Return(absl::InternalError("Values store error")))
      .WillRepeatedly(Return(absl::OkStatus()));
  EXPECT_THAT(state.SetStorageValue(Address{}, Key{}, Value{}),
              StatusIs(absl::StatusCode::kInternal, "Values store error"));

  // for empty value Erase(...) is called on address to slots map
  EXPECT_CALL(state.GetAddressToSlotsMap(), Erase(_, _))
      .WillOnce(Return(absl::InternalError("Address to slots map error")));
  EXPECT_THAT(
      state.SetStorageValue(Address{}, Key{}, Value{}),
      StatusIs(absl::StatusCode::kInternal, "Address to slots map error"));

  // for non-empty value Insert(...) is called on address to slots map
  EXPECT_CALL(state.GetAddressToSlotsMap(), Insert(_, _))
      .WillOnce(Return(absl::InternalError("Address to slots map error")));
  EXPECT_THAT(
      state.SetStorageValue(Address{}, Key{}, Value{1}),
      StatusIs(absl::StatusCode::kInternal, "Address to slots map error"));
}

TEST_F(MockStateTest, GetCodeNotFoundErrorIsHandled) {
  auto& state = GetState();

  EXPECT_CALL(state.GetAddressIndex(), Get(_))
      .WillOnce(Return(absl::NotFoundError("Address not found")))
      .WillRepeatedly(Return(absl::StatusOr<MockState::AddressId>(1)));
  EXPECT_THAT(state.GetCode(Address{}), IsOkAndHolds(IsEmpty()));

  EXPECT_CALL(state.GetCodesDepot(), Get(_))
      .WillOnce(Return(absl::NotFoundError("Code not found")));
  EXPECT_THAT(state.GetCode(Address{}), IsOkAndHolds(IsEmpty()));
}

TEST_F(MockStateTest, GetCodeErrorIsForwarded) {
  auto& state = GetState();

  EXPECT_CALL(state.GetAddressIndex(), Get(_))
      .WillOnce(Return(absl::InternalError("Address index error")))
      .WillRepeatedly(Return(absl::StatusOr<MockState::AddressId>(1)));
  EXPECT_THAT(state.GetCode(Address{}),
              StatusIs(absl::StatusCode::kInternal, "Address index error"));

  EXPECT_CALL(state.GetCodesDepot(), Get(_))
      .WillOnce(Return(absl::InternalError("Codes depot error")));
  EXPECT_THAT(state.GetCode(Address{}),
              StatusIs(absl::StatusCode::kInternal, "Codes depot error"));
}

TEST_F(MockStateTest, SetCodeErrorIsForwarded) {
  auto& state = GetState();

  EXPECT_CALL(state.GetAddressIndex(), GetOrAdd(_))
      .WillOnce(Return(absl::InternalError("Address index error")))
      .WillRepeatedly(Return(
          absl::StatusOr<std::pair<MockState::AddressId, bool>>({1, true})));
  EXPECT_THAT(state.SetCode(Address{}, Code{}),
              StatusIs(absl::StatusCode::kInternal, "Address index error"));

  EXPECT_CALL(state.GetCodesDepot(), Set(_, _))
      .WillOnce(Return(absl::InternalError("Codes depot error")));
  EXPECT_THAT(state.SetCode(Address{}, Code{}),
              StatusIs(absl::StatusCode::kInternal, "Codes depot error"));
}

TEST_F(MockStateTest, GetCodeSizeNotFoundErrorIsHandled) {
  auto& state = GetState();

  EXPECT_CALL(state.GetAddressIndex(), Get(_))
      .WillOnce(Return(absl::NotFoundError("Address not found")))
      .WillRepeatedly(Return(absl::StatusOr<MockState::AddressId>(1)));
  EXPECT_THAT(state.GetCodeSize(Address{}), IsOkAndHolds(0));

  EXPECT_CALL(state.GetCodesDepot(), GetSize(_))
      .WillOnce(Return(absl::NotFoundError("Code not found")));
  EXPECT_THAT(state.GetCodeSize(Address{}), IsOkAndHolds(0));
}

TEST_F(MockStateTest, GetCodeSizeErrorIsForwarded) {
  auto& state = GetState();

  EXPECT_CALL(state.GetAddressIndex(), Get(_))
      .WillOnce(Return(absl::InternalError("Address index error")))
      .WillRepeatedly(Return(absl::StatusOr<MockState::AddressId>(1)));
  EXPECT_THAT(state.GetCodeSize(Address{}),
              StatusIs(absl::StatusCode::kInternal, "Address index error"));

  EXPECT_CALL(state.GetCodesDepot(), GetSize(_))
      .WillOnce(Return(absl::InternalError("Codes depot error")));
  EXPECT_THAT(state.GetCodeSize(Address{}),
              StatusIs(absl::StatusCode::kInternal, "Codes depot error"));
}

TEST_F(MockStateTest, GetCodeHashNotFoundErrorIsHandled) {
  auto& state = GetState();

  EXPECT_CALL(state.GetAddressIndex(), Get(_))
      .WillOnce(Return(absl::NotFoundError("Address not found")));
  EXPECT_THAT(state.GetCodeHash(Address{}), state.GetEmptyCodeHash());
}

TEST_F(MockStateTest, GetCodeHashEmptyCodeIsHandled) {
  auto& state = GetState();

  EXPECT_CALL(state.GetAddressIndex(), Get(_))
      .Times(2)
      .WillRepeatedly(Return(absl::StatusOr<MockState::AddressId>(1)));
  EXPECT_CALL(state.GetCodeHashesStore(), Get(_))
      .WillOnce(Return(absl::StatusOr<Hash>(Hash{})));
  EXPECT_CALL(state.GetCodesDepot(), GetSize(_))
      .WillOnce(Return(absl::StatusOr<std::uint32_t>(0)));
  EXPECT_THAT(state.GetCodeHash(Address{}), state.GetEmptyCodeHash());
}

TEST_F(MockStateTest, GetCodeHashErrorIsForwarded) {
  auto& state = GetState();

  EXPECT_CALL(state.GetAddressIndex(), Get(_))
      .WillOnce(Return(absl::InternalError("Address index error")))
      .WillOnce(Return(absl::StatusOr<MockState::AddressId>(1)));
  EXPECT_THAT(state.GetCodeHash(Address{}),
              StatusIs(absl::StatusCode::kInternal, "Address index error"));

  EXPECT_CALL(state.GetCodeHashesStore(), Get(_))
      .WillOnce(Return(absl::InternalError("Code hashes store error")));
  EXPECT_THAT(state.GetCodeHash(Address{}),
              StatusIs(absl::StatusCode::kInternal, "Code hashes store error"));
}

TEST_F(MockStateTest, GetHashErrorIsForwarded) {
  auto& state = GetState();

  EXPECT_CALL(state.GetAddressIndex(), GetHash())
      .WillOnce(Return(absl::InternalError("Address index error")))
      .WillRepeatedly(Return(absl::StatusOr<Hash>(Hash{})));
  EXPECT_THAT(state.GetHash(),
              StatusIs(absl::StatusCode::kInternal, "Address index error"));

  EXPECT_CALL(state.GetKeyIndex(), GetHash())
      .WillOnce(Return(absl::InternalError("Key index error")))
      .WillRepeatedly(Return(absl::StatusOr<Hash>(Hash{})));
  EXPECT_THAT(state.GetHash(),
              StatusIs(absl::StatusCode::kInternal, "Key index error"));

  EXPECT_CALL(state.GetSlotIndex(), GetHash())
      .WillOnce(Return(absl::InternalError("Slot index error")))
      .WillRepeatedly(Return(absl::StatusOr<Hash>(Hash{})));
  EXPECT_THAT(state.GetHash(),
              StatusIs(absl::StatusCode::kInternal, "Slot index error"));

  EXPECT_CALL(state.GetBalancesStore(), GetHash())
      .WillOnce(Return(absl::InternalError("Balances store error")))
      .WillRepeatedly(Return(absl::StatusOr<Hash>(Hash{})));
  EXPECT_THAT(state.GetHash(),
              StatusIs(absl::StatusCode::kInternal, "Balances store error"));

  EXPECT_CALL(state.GetNoncesStore(), GetHash())
      .WillOnce(Return(absl::InternalError("Nonces store error")))
      .WillRepeatedly(Return(absl::StatusOr<Hash>(Hash{})));
  EXPECT_THAT(state.GetHash(),
              StatusIs(absl::StatusCode::kInternal, "Nonces store error"));

  EXPECT_CALL(state.GetValueStore(), GetHash())
      .WillOnce(Return(absl::InternalError("Value store error")))
      .WillRepeatedly(Return(absl::StatusOr<Hash>(Hash{})));
  EXPECT_THAT(state.GetHash(),
              StatusIs(absl::StatusCode::kInternal, "Value store error"));

  EXPECT_CALL(state.GetAccountStatesStore(), GetHash())
      .WillOnce(Return(absl::InternalError("Account states store error")))
      .WillRepeatedly(Return(absl::StatusOr<Hash>(Hash{})));
  EXPECT_THAT(state.GetHash(), StatusIs(absl::StatusCode::kInternal,
                                        "Account states store error"));

  EXPECT_CALL(state.GetCodesDepot(), GetHash())
      .WillOnce(Return(absl::InternalError("Codes depot error")));
  EXPECT_THAT(state.GetHash(),
              StatusIs(absl::StatusCode::kInternal, "Codes depot error"));
}

TEST_F(MockStateTest, FlushErrorIsForwarded) {
  auto& state = GetState();

  EXPECT_CALL(state.GetAddressIndex(), Flush())
      .WillOnce(Return(absl::InternalError("Address index error")))
      .WillRepeatedly(Return(absl::OkStatus()));
  EXPECT_THAT(state.Flush(),
              StatusIs(absl::StatusCode::kInternal, "Address index error"));

  EXPECT_CALL(state.GetKeyIndex(), Flush())
      .WillOnce(Return(absl::InternalError("Key index error")))
      .WillRepeatedly(Return(absl::OkStatus()));
  EXPECT_THAT(state.Flush(),
              StatusIs(absl::StatusCode::kInternal, "Key index error"));

  EXPECT_CALL(state.GetSlotIndex(), Flush())
      .WillOnce(Return(absl::InternalError("Slot index error")))
      .WillRepeatedly(Return(absl::OkStatus()));
  EXPECT_THAT(state.Flush(),
              StatusIs(absl::StatusCode::kInternal, "Slot index error"));

  EXPECT_CALL(state.GetBalancesStore(), Flush())
      .WillOnce(Return(absl::InternalError("Balance store error")))
      .WillRepeatedly(Return(absl::OkStatus()));
  EXPECT_THAT(state.Flush(),
              StatusIs(absl::StatusCode::kInternal, "Balance store error"));

  EXPECT_CALL(state.GetNoncesStore(), Flush())
      .WillOnce(Return(absl::InternalError("Nonce store error")))
      .WillRepeatedly(Return(absl::OkStatus()));
  EXPECT_THAT(state.Flush(),
              StatusIs(absl::StatusCode::kInternal, "Nonce store error"));

  EXPECT_CALL(state.GetValueStore(), Flush())
      .WillOnce(Return(absl::InternalError("Value store error")))
      .WillRepeatedly(Return(absl::OkStatus()));
  EXPECT_THAT(state.Flush(),
              StatusIs(absl::StatusCode::kInternal, "Value store error"));

  EXPECT_CALL(state.GetAccountStatesStore(), Flush())
      .WillOnce(Return(absl::InternalError("Account state store error")))
      .WillRepeatedly(Return(absl::OkStatus()));
  EXPECT_THAT(state.Flush(), StatusIs(absl::StatusCode::kInternal,
                                      "Account state store error"));

  EXPECT_CALL(state.GetCodesDepot(), Flush())
      .WillOnce(Return(absl::InternalError("Code depot error")))
      .WillRepeatedly(Return(absl::OkStatus()));
  EXPECT_THAT(state.Flush(),
              StatusIs(absl::StatusCode::kInternal, "Code depot error"));

  EXPECT_CALL(state.GetCodeHashesStore(), Flush())
      .WillOnce(Return(absl::InternalError("Code hash store error")))
      .WillRepeatedly(Return(absl::OkStatus()));
  EXPECT_THAT(state.Flush(),
              StatusIs(absl::StatusCode::kInternal, "Code hash store error"));

  EXPECT_CALL(state.GetAddressToSlotsMap(), Flush())
      .WillOnce(Return(absl::InternalError("Address to slot multimap error")));
  EXPECT_THAT(state.Flush(), StatusIs(absl::StatusCode::kInternal,
                                      "Address to slot multimap error"));
}

TEST_F(MockStateTest, CloseErrorIsForwarded) {
  auto& state = GetState();

  EXPECT_CALL(state.GetAddressIndex(), Close())
      .WillOnce(Return(absl::InternalError("Address index error")))
      .WillRepeatedly(Return(absl::OkStatus()));
  EXPECT_THAT(state.Close(),
              StatusIs(absl::StatusCode::kInternal, "Address index error"));

  EXPECT_CALL(state.GetKeyIndex(), Close())
      .WillOnce(Return(absl::InternalError("Key index error")))
      .WillRepeatedly(Return(absl::OkStatus()));
  EXPECT_THAT(state.Close(),
              StatusIs(absl::StatusCode::kInternal, "Key index error"));

  EXPECT_CALL(state.GetSlotIndex(), Close())
      .WillOnce(Return(absl::InternalError("Slot index error")))
      .WillRepeatedly(Return(absl::OkStatus()));
  EXPECT_THAT(state.Close(),
              StatusIs(absl::StatusCode::kInternal, "Slot index error"));

  EXPECT_CALL(state.GetBalancesStore(), Close())
      .WillOnce(Return(absl::InternalError("Balance store error")))
      .WillRepeatedly(Return(absl::OkStatus()));
  EXPECT_THAT(state.Close(),
              StatusIs(absl::StatusCode::kInternal, "Balance store error"));

  EXPECT_CALL(state.GetNoncesStore(), Close())
      .WillOnce(Return(absl::InternalError("Nonce store error")))
      .WillRepeatedly(Return(absl::OkStatus()));
  EXPECT_THAT(state.Close(),
              StatusIs(absl::StatusCode::kInternal, "Nonce store error"));

  EXPECT_CALL(state.GetValueStore(), Close())
      .WillOnce(Return(absl::InternalError("Value store error")))
      .WillRepeatedly(Return(absl::OkStatus()));
  EXPECT_THAT(state.Close(),
              StatusIs(absl::StatusCode::kInternal, "Value store error"));

  EXPECT_CALL(state.GetAccountStatesStore(), Close())
      .WillOnce(Return(absl::InternalError("Account state store error")))
      .WillRepeatedly(Return(absl::OkStatus()));
  EXPECT_THAT(state.Close(), StatusIs(absl::StatusCode::kInternal,
                                      "Account state store error"));

  EXPECT_CALL(state.GetCodesDepot(), Close())
      .WillOnce(Return(absl::InternalError("Code depot error")))
      .WillRepeatedly(Return(absl::OkStatus()));
  EXPECT_THAT(state.Close(),
              StatusIs(absl::StatusCode::kInternal, "Code depot error"));

  EXPECT_CALL(state.GetCodeHashesStore(), Close())
      .WillOnce(Return(absl::InternalError("Code hash store error")))
      .WillRepeatedly(Return(absl::OkStatus()));
  EXPECT_THAT(state.Close(),
              StatusIs(absl::StatusCode::kInternal, "Code hash store error"));

  EXPECT_CALL(state.GetAddressToSlotsMap(), Close())
      .WillOnce(Return(absl::InternalError("Address to slot multimap error")));
  EXPECT_THAT(state.Close(), StatusIs(absl::StatusCode::kInternal,
                                      "Address to slot multimap error"));
}
}  // namespace carmen
