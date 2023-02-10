#include "backend/store/file/store.h"

#include "backend/common/file.h"
#include "backend/store/store_test_suite.h"
#include "backend/structure.h"
#include "common/file_util.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::store {
namespace {

using ::testing::IsOkAndHolds;

using StoreTypes = ::testing::Types<
    // Page size 32, branching size 32.
    StoreTestConfig<EagerFileStore<int, Value, InMemoryFile, 32>, 32>,
    StoreTestConfig<EagerFileStore<int, Value, SingleFile, 32>, 32>,
    StoreTestConfig<LazyFileStore<int, Value, SingleFile, 32>, 32>,
    // Page size 64, branching size 3.
    StoreTestConfig<EagerFileStore<int, Value, InMemoryFile, 64>, 3>,
    StoreTestConfig<EagerFileStore<int, Value, SingleFile, 64>, 3>,
    StoreTestConfig<LazyFileStore<int, Value, SingleFile, 64>, 3>,
    // Page size 64, branching size 8.
    StoreTestConfig<EagerFileStore<int, Value, InMemoryFile, 64>, 8>,
    StoreTestConfig<EagerFileStore<int, Value, SingleFile, 64>, 8>,
    StoreTestConfig<LazyFileStore<int, Value, SingleFile, 64>, 8>,
    // Page size 128, branching size 4.
    StoreTestConfig<EagerFileStore<int, Value, InMemoryFile, 128>, 4>,
    StoreTestConfig<EagerFileStore<int, Value, SingleFile, 128>, 4>,
    StoreTestConfig<LazyFileStore<int, Value, SingleFile, 128>, 4>>;

// Instantiates common store tests for the File store type.
INSTANTIATE_TYPED_TEST_SUITE_P(File, StoreTest, StoreTypes);

template <typename T>
class FileStoreTest : public testing::Test {};

TYPED_TEST_SUITE_P(FileStoreTest);

TYPED_TEST_P(FileStoreTest, StoreCanBeSavedAndRestored) {
  using Store = TypeParam;
  const auto kNumElements = static_cast<int>(Store::kPageSize * 10);
  TempDir dir;
  Context ctx;
  Hash hash;
  {
    ASSERT_OK_AND_ASSIGN(auto store, Store::Open(ctx, dir.GetPath()));
    for (int i = 0; i < kNumElements; i++) {
      ASSERT_OK(store.Set(i, i * i));
    }
    ASSERT_OK_AND_ASSIGN(hash, store.GetHash());
  }
  {
    ASSERT_OK_AND_ASSIGN(auto restored, Store::Open(ctx, dir.GetPath()));
    EXPECT_THAT(restored.GetHash(), IsOkAndHolds(hash));
    for (int i = 0; i < kNumElements; i++) {
      EXPECT_THAT(restored.Get(i), IsOkAndHolds(i * i));
    }
  }
}

REGISTER_TYPED_TEST_SUITE_P(FileStoreTest, StoreCanBeSavedAndRestored);

using FileStoreVariants = ::testing::Types<EagerFileStore<int, int, SingleFile>,
                                           LazyFileStore<int, int, SingleFile>>;

INSTANTIATE_TYPED_TEST_SUITE_P(FileStoreTests, FileStoreTest,
                               FileStoreVariants);

}  // namespace
}  // namespace carmen::backend::store
