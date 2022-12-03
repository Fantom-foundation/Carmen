#include "backend/store/leveldb/store.h"

#include "backend/structure.h"
#include "common/file_util.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::store {
namespace {

using TestStore = LevelDbStore<int, int>;

TEST(LevelDbStoreTest, StoreCanBeSavedAndRestored) {
  const auto kNumElements = static_cast<int>(TestStore::kPageSize * 10);
  TempDir dir;
  Context ctx;
  Hash hash;
  {
    auto store = TestStore::Open(ctx, dir.GetPath());
    ASSERT_OK(store);
    for (int i = 0; i < kNumElements; i++) {
      ASSERT_OK((*store).Set(i, i * i));
    }
    hash = *(*store).GetHash();
  }
  {
    auto restored = TestStore::Open(ctx, dir.GetPath());
    ASSERT_OK(restored);
    EXPECT_EQ(hash, *(*restored).GetHash());
    for (int i = 0; i < kNumElements; i++) {
      EXPECT_EQ(*(*restored).Get(i), i * i);
    }
  }
}
}  // namespace
}  // namespace carmen::backend::store
