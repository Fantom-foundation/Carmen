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
