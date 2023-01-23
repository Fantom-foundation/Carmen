#include "backend/store/memory/store.h"

#include <type_traits>

#include "common/file_util.h"
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
  TempDir dir;
  Context ctx;
  ASSERT_OK_AND_ASSIGN(auto store, Store::Open(ctx, dir.GetPath()));

  ASSERT_OK(store.Set(10, 12));
  EXPECT_THAT(store.Get(10), IsOkAndHolds(12));

  auto snapshot = store.CreateSnapshot();

  ASSERT_OK(store.Set(10, 14));
  EXPECT_THAT(store.Get(10), IsOkAndHolds(14));

  Store restored(*snapshot);
  EXPECT_THAT(store.Get(10), IsOkAndHolds(14));
  EXPECT_THAT(restored.Get(10), IsOkAndHolds(12));
}

TEST(InMemoryStoreTest, SnapshotRecoveryHasSameHash) {
  TempDir dir;
  Context ctx;
  ASSERT_OK_AND_ASSIGN(auto store, Store::Open(ctx, dir.GetPath()));

  ASSERT_OK(store.Set(10, 12));
  ASSERT_OK_AND_ASSIGN(auto hash, store.GetHash());
  auto snapshot = store.CreateSnapshot();

  Store restored(*snapshot);
  EXPECT_THAT(restored.GetHash(), IsOkAndHolds(hash));
}

TEST(InMemoryStoreTest, LargeSnapshotRecoveryWorks) {
  constexpr const int kNumElements = 100000;
  TempDir dir;
  Context ctx;
  ASSERT_OK_AND_ASSIGN(auto store, Store::Open(ctx, dir.GetPath()));

  for (int i = 0; i < kNumElements; i++) {
    ASSERT_OK(store.Set(i, i + 10));
  }
  ASSERT_OK_AND_ASSIGN(auto hash, store.GetHash());
  auto snapshot = store.CreateSnapshot();

  Store restored(*snapshot);
  for (int i = 0; i < kNumElements; i++) {
    EXPECT_THAT(restored.Get(i), IsOkAndHolds(i + 10));
  }
  EXPECT_THAT(restored.GetHash(), IsOkAndHolds(hash));
}

}  // namespace
}  // namespace carmen::backend::store
