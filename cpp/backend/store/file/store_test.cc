#include "backend/store/file/store.h"

#include <type_traits>

#include "backend/store/file/file.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::store {
namespace {

using Store = FileStore<int, int, InMemoryFile, 40>;

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

}  // namespace
}  // namespace carmen::backend::store
