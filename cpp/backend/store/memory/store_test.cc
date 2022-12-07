#include "backend/store/memory/store.h"

#include <type_traits>

#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::store {
namespace {

using ::testing::IsOkAndHolds;

using Store = InMemoryStore<int, int>;

TEST(InMemoryStoreTest, TypeTraits) {
  EXPECT_TRUE(std::is_default_constructible_v<Store>);
  EXPECT_FALSE(std::is_copy_constructible_v<Store>);
  EXPECT_TRUE(std::is_move_constructible_v<Store>);
  EXPECT_FALSE(std::is_copy_assignable_v<Store>);
  EXPECT_FALSE(std::is_move_assignable_v<Store>);
}

TEST(InMemoryStoreTest, SnapshotShieldsMutations) {
  Store store;

  store.Set(10, 12);
  EXPECT_EQ(store.Get(10), 12);

  auto snapshot = store.CreateSnapshot();

  store.Set(10, 14);
  EXPECT_EQ(store.Get(10), 14);

  Store restored(*snapshot);
  EXPECT_EQ(store.Get(10), 14);
  EXPECT_EQ(restored.Get(10), 12);
}

TEST(InMemoryStoreTest, SnapshotRecoveryHasSameHash) {
  Store store;
  store.Set(10, 12);
  ASSERT_OK_AND_ASSIGN(auto hash, store.GetHash());
  auto snapshot = store.CreateSnapshot();

  Store restored(*snapshot);
  EXPECT_THAT(restored.GetHash(), IsOkAndHolds(hash));
}

TEST(InMemoryStoreTest, LargeSnapshotRecoveryWorks) {
  constexpr const int kNumElements = 100000;

  Store store;
  for (int i = 0; i < kNumElements; i++) {
    store.Set(i, i + 10);
  }
  ASSERT_OK_AND_ASSIGN(auto hash, store.GetHash());
  auto snapshot = store.CreateSnapshot();

  Store restored(*snapshot);
  for (int i = 0; i < kNumElements; i++) {
    EXPECT_EQ(store.Get(i), i + 10);
  }
  EXPECT_THAT(restored.GetHash(), IsOkAndHolds(hash));
}

}  // namespace
}  // namespace carmen::backend::store
