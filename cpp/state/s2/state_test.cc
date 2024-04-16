/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3 
 */

#include "state/s2/state.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "archive/leveldb/archive.h"
#include "archive/test_util.h"
#include "backend/depot/memory/depot.h"
#include "backend/depot/test_util.h"
#include "backend/index/memory/index.h"
#include "backend/index/test_util.h"
#include "backend/multimap/memory/multimap.h"
#include "backend/multimap/test_util.h"
#include "backend/store/memory/store.h"
#include "backend/store/test_util.h"
#include "common/account_state.h"
#include "common/status_test_util.h"
#include "common/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "state/configurations.h"
#include "state/state_test_suite.h"
#include "state/update.h"

namespace carmen::s2 {
namespace {

using ::testing::_;
using ::testing::IsOkAndHolds;
using ::testing::Return;
using ::testing::StatusIs;

// ------------------------- Functionality Tests ------------------------------

using TestArchive = archive::leveldb::LevelDbArchive;

using StateConfigurations =
    ::testing::Types<State<InMemoryConfig<TestArchive>>,
                     State<FileBasedConfig<TestArchive>>,
                     State<LevelDbBasedConfig<TestArchive>>>;

INSTANTIATE_TYPED_TEST_SUITE_P(Schema_2, StateTest, StateConfigurations);

// ------------------------ Error Handling Tests ------------------------------

template <typename K, typename V>
using MockIndex = backend::index::MockIndex<K, V>;

template <typename K, typename V>
using MockStore = backend::store::MockStore<K, V, kPageSize>;

template <typename K>
using MockDepot = backend::depot::MockDepot<K>;

template <typename K, typename V>
using MockMultiMap = backend::multimap::MockMultiMap<K, V>;

using MockConfig =
    Configuration<MockIndex, MockStore, MockDepot, MockMultiMap, MockArchive>;

using MockState = State<MockConfig>;

// A test fixture for the State class. It provides a State instance with
// mocked dependencies. The dependencies are exposed through getters.
class MockStateTest : public ::testing::Test {
 public:
  auto& GetState() { return state_; }

 private:
  class Mock : public MockState {
   public:
    Mock()
        : MockState(MockIndex<Address, AddressId>(), MockIndex<Slot, SlotId>(),
                    MockStore<AddressId, Balance>(),
                    MockStore<AddressId, Nonce>(), MockStore<SlotId, Value>(),
                    MockStore<AddressId, AccountState>(),
                    MockDepot<AddressId>(), MockStore<AddressId, Hash>(),
                    MockMultiMap<AddressId, SlotId>(),
                    std::make_unique<MockArchive>()) {}
    auto& GetAddressIndex() { return this->address_index_.GetMockIndex(); }
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
    // archive will always be available, because it is created in the
    // constructor.
    auto& GetArchive() { return this->archive_->GetMockArchive(); }
    auto GetEmptyCodeHash() const { return kEmptyCodeHash; }
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
  EXPECT_THAT(state.GetCode(Address{}), IsOkAndHolds(Code()));

  EXPECT_CALL(state.GetCodesDepot(), Get(_))
      .WillOnce(Return(absl::NotFoundError("Code not found")));
  EXPECT_THAT(state.GetCode(Address{}), IsOkAndHolds(Code()));
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
      .WillOnce(Return(absl::InternalError("Address to slot multimap error")))
      .WillRepeatedly(Return(absl::OkStatus()));
  EXPECT_THAT(state.Flush(), StatusIs(absl::StatusCode::kInternal,
                                      "Address to slot multimap error"));

  EXPECT_CALL(state.GetArchive(), Flush())
      .WillOnce(Return(absl::InternalError("Archive error")));
  EXPECT_THAT(state.Flush(),
              StatusIs(absl::StatusCode::kInternal, "Archive error"));
}

TEST_F(MockStateTest, CloseErrorIsForwarded) {
  auto& state = GetState();

  EXPECT_CALL(state.GetAddressIndex(), Close())
      .WillOnce(Return(absl::InternalError("Address index error")))
      .WillRepeatedly(Return(absl::OkStatus()));
  EXPECT_THAT(state.Close(),
              StatusIs(absl::StatusCode::kInternal, "Address index error"));

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
      .WillOnce(Return(absl::InternalError("Address to slot multimap error")))
      .WillRepeatedly(Return(absl::OkStatus()));
  EXPECT_THAT(state.Close(), StatusIs(absl::StatusCode::kInternal,
                                      "Address to slot multimap error"));

  EXPECT_CALL(state.GetArchive(), Close())
      .WillOnce(Return(absl::InternalError("Archive error")));
  EXPECT_THAT(state.Close(),
              StatusIs(absl::StatusCode::kInternal, "Archive error"));
}

TEST_F(MockStateTest, ApplyArchiveErrorIsForwarded) {
  auto& state = GetState();

  EXPECT_CALL(state.GetArchive(), Add(_, _))
      .WillOnce(Return(absl::InternalError("Archive error")));
  EXPECT_THAT(state.Apply(0, Update{}),
              StatusIs(absl::StatusCode::kInternal, "Archive error"));
}

}  // namespace
}  // namespace carmen::s2
