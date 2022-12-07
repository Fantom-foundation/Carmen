#include "backend/store/leveldb/store.h"

#include "backend/structure.h"
#include "common/file_util.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::store {
namespace {

using ::testing::IsOkAndHolds;

using TestStore = LevelDbStore<int, int>;

TEST(LevelDbStoreTest, StoreCanBeSavedAndRestored) {
  const auto kNumElements = static_cast<int>(TestStore::kPageSize * 10);
  TempDir dir;
  Context ctx;
  Hash hash;
  {
    ASSERT_OK_AND_ASSIGN(auto store, TestStore::Open(ctx, dir.GetPath()));
    for (int i = 0; i < kNumElements; i++) {
      ASSERT_OK(store.Set(i, i * i));
    }
    ASSERT_OK_AND_ASSIGN(hash, store.GetHash());
  }
  {
    ASSERT_OK_AND_ASSIGN(auto restored, TestStore::Open(ctx, dir.GetPath()));
    EXPECT_THAT(restored.GetHash(), IsOkAndHolds(hash));
    for (int i = 0; i < kNumElements; i++) {
      EXPECT_THAT(restored.Get(i), IsOkAndHolds(i * i));
    }
  }
}
}  // namespace
}  // namespace carmen::backend::store
