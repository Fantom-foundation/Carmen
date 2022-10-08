#include "state/c_state.h"

#include "common/type.h"
#include "gtest/gtest.h"

namespace carmen {
namespace {

TEST(CStateTest, StateCanBeCreatedAndReleased) {
  auto state = Carmen_CreateState();
  EXPECT_NE(state, nullptr);
  Carmen_ReleaseState(state);
}

TEST(CStateTest, BalancesAreInitiallyZero) {
  auto state = Carmen_CreateState();
  ASSERT_NE(state, nullptr);

  Address addr{0x01};
  Balance balance{0x02};
  Carmen_GetBalance(state, &addr, &balance);
  EXPECT_EQ(Balance{}, balance);

  Carmen_ReleaseState(state);
}

TEST(CStateTest, BalancesCanBeUpdated) {
  auto state = Carmen_CreateState();
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

  Carmen_ReleaseState(state);
}

TEST(CStateTest, NoncesAreInitiallyZero) {
  auto state = Carmen_CreateState();
  ASSERT_NE(state, nullptr);

  Address addr{0x01};
  Nonce nonce{0x02};
  Carmen_GetNonce(state, &addr, &nonce);
  EXPECT_EQ(Nonce{}, nonce);

  Carmen_ReleaseState(state);
}

TEST(CStateTest, NoncesCanBeUpdated) {
  auto state = Carmen_CreateState();
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

  Carmen_ReleaseState(state);
}

TEST(CStateTest, StorageLocationsAreInitiallyZero) {
  auto state = Carmen_CreateState();
  ASSERT_NE(state, nullptr);

  Address addr{0x01};
  Key key{0x02};
  Value value{0x03};
  Carmen_GetStorageValue(state, &addr, &key, &value);
  EXPECT_EQ(Value{}, value);

  Carmen_ReleaseState(state);
}

TEST(CStateTest, StorageLocationsCanBeUpdated) {
  auto state = Carmen_CreateState();
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

  Carmen_ReleaseState(state);
}

TEST(CStateTest, StateHashesCanBeObtained) {
  auto state = Carmen_CreateState();
  ASSERT_NE(state, nullptr);

  Hash hash;
  Carmen_GetHash(state, &hash);
  EXPECT_NE(Hash{}, hash);

  Carmen_ReleaseState(state);
}

TEST(CStateTest, HashesChangeOnUpdates) {
  auto state = Carmen_CreateState();
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

  Carmen_ReleaseState(state);
}

}  // namespace
}  // namespace carmen
