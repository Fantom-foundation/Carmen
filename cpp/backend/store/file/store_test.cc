#include "backend/store/file/store.h"

#include <type_traits>

#include "backend/common/file.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::store {
namespace {

using Store = FileStore<int, int, InMemoryFile, 40>;

TEST(FileStoreTest, TypeProperties) {
  EXPECT_TRUE(std::is_move_constructible_v<Store>);
}

TEST(FileStoreTest, DataCanBeAddedAndRetrieved) {
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

TEST(FileStoreTest, EntriesCanBeUpdated) {
  Store store;
  EXPECT_EQ(0, store.Get(10));
  store.Set(10, 12);
  EXPECT_EQ(12, store.Get(10));
  store.Set(10, 14);
  EXPECT_EQ(14, store.Get(10));
}

TEST(FileStoreTest, EmptyStoreHasZeroValueHash) {
  Store store;
  EXPECT_EQ(store.GetHash(), Hash());
}

TEST(FileStoreTest, HashesChangeWithUpdates) {
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

TEST(FileStoreTest, HashesCoverMultiplePages) {
  Store store;
  auto empty_hash = store.GetHash();
  for (int i = 0; i < 10000; i++) {
    store.Set(i, i + 1);
  }
  auto hash_a = store.GetHash();
  EXPECT_NE(empty_hash, hash_a);
  store.Set(5000, 0);
  auto hash_b = store.GetHash();
  EXPECT_NE(empty_hash, hash_b);
  EXPECT_NE(hash_a, hash_b);
}

}  // namespace
}  // namespace carmen::backend::store
