#include "backend/store/leveldb/store.h"

#include "backend/store/store_test_suite.h"
#include "backend/structure.h"
#include "common/file_util.h"
#include "common/status_test_util.h"
#include "common/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::store {
namespace {

using ::testing::IsOkAndHolds;

using TestStore = LevelDbStore<int, int>;

using StoreTypes = ::testing::Types<
    // Page size 32, branching size 32.
    StoreTestConfig<LevelDbStore<int, Value, 32>, 32>,
    // Page size 64, branching size 3.
    StoreTestConfig<LevelDbStore<int, Value, 64>, 3>,
    // Page size 64, branching size 8.
    StoreTestConfig<LevelDbStore<int, Value, 64>, 8>,
    // Page size 128, branching size 4.
    StoreTestConfig<LevelDbStore<int, Value, 128>, 4>>;

// Instantiates common store tests for the LevelDb store type.
INSTANTIATE_TYPED_TEST_SUITE_P(LevelDb, StoreTest, StoreTypes);

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
