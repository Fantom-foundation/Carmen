#include "backend/store/file/store.h"

#include "backend/common/file.h"
#include "backend/structure.h"
#include "common/file_util.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::store {
namespace {

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
      store.Set(i, i * i);
    }
    ASSERT_OK_AND_ASSIGN(hash, store.GetHash());
  }
  {
    ASSERT_OK_AND_ASSIGN(auto restored, Store::Open(ctx, dir.GetPath()));
    ASSERT_OK_AND_ASSIGN(auto restored_hash, restored.GetHash());
    EXPECT_EQ(hash, restored_hash);
    for (int i = 0; i < kNumElements; i++) {
      EXPECT_EQ(restored.Get(i), i * i);
    }
  }
}

REGISTER_TYPED_TEST_SUITE_P(FileStoreTest, StoreCanBeSavedAndRestored);

using FileStoreVariants =
    ::testing::Types<EagerFileStore<int, int, SingleFile>,
                     LazyFileStore<int, int, SingleFile> >;

INSTANTIATE_TYPED_TEST_SUITE_P(FileStoreTests, FileStoreTest,
                               FileStoreVariants);

}  // namespace
}  // namespace carmen::backend::store
