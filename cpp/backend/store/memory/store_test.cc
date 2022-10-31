#include "backend/store/memory/store.h"

#include <type_traits>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::store {
namespace {

using Store = InMemoryStore<int, int>;

TEST(InMemoryStoreTest, TypeTraits) {
  EXPECT_TRUE(std::is_default_constructible_v<Store>);
  EXPECT_FALSE(std::is_copy_constructible_v<Store>);
  EXPECT_FALSE(std::is_move_constructible_v<Store>);
  EXPECT_FALSE(std::is_copy_assignable_v<Store>);
  EXPECT_FALSE(std::is_move_assignable_v<Store>);
}

TEST(InMemoryStoreTest, DataCanBeAddedAndRetrieved) {
  Store store;
  EXPECT_EQ(0, store.Get(10));
  EXPECT_EQ(0, store.Get(12));

  store.Set(10, 12);
  EXPECT_EQ(12, store.Get(10));
  EXPECT_EQ(0, store.Get(12));

  store.Set(12, 14);
  EXPECT_EQ(12, store.Get(10));
  EXPECT_EQ(14, store.Get(12));
}

TEST(InMemoryStoreTest, EntriesCanBeUpdated) {
  Store store;
  EXPECT_EQ(0, store.Get(10));
  store.Set(10, 12);
  EXPECT_EQ(12, store.Get(10));
  store.Set(10, 14);
  EXPECT_EQ(14, store.Get(10));
}

TEST(InMemoryStoreTest, EmptyStoreHasZeroValueHash) {
  Store store;
  EXPECT_EQ(store.GetHash(), Hash());
}

TEST(InMemoryStoreTest, HashesChangeWithUpdates) {
  Store store;
  auto empty_hash = store.GetHash();
  store.Set(1, 2);
  auto hash_a = store.GetHash();
  EXPECT_NE(empty_hash, hash_a);
  store.Set(2, 4);
  auto hash_b = store.GetHash();
  EXPECT_NE(empty_hash, hash_b);
  EXPECT_NE(hash_a, hash_b);
}

TEST(InMemoryStoreTest, HashesCoverMultiplePages) {
  Store store;
  auto empty_hash = store.GetHash();
  for (int i = 0; i < 1000000; i++) {
    store.Set(i, i + 1);
  }
  auto hash_a = store.GetHash();
  EXPECT_NE(empty_hash, hash_a);
  store.Set(500000, 0);
  auto hash_b = store.GetHash();
  EXPECT_NE(empty_hash, hash_b);
  EXPECT_NE(hash_a, hash_b);
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
  auto hash = store.GetHash();
  auto snapshot = store.CreateSnapshot();

  Store restored(*snapshot);
  EXPECT_EQ(restored.GetHash(), hash);
}

TEST(InMemoryStoreTest, LargeSnapshotRecoveryWorks) {
  constexpr const int kNumElements = 100000;

  Store store;
  for (int i = 0; i < kNumElements; i++) {
    store.Set(i, i + 10);
  }
  auto hash = store.GetHash();
  auto snapshot = store.CreateSnapshot();

  Store restored(*snapshot);
  for (int i = 0; i < kNumElements; i++) {
    EXPECT_THAT(store.Get(i), i + 10);
  }
  EXPECT_EQ(restored.GetHash(), hash);
}

}  // namespace
}  // namespace carmen::backend::store
