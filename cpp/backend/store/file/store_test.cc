#include "backend/store/file/store.h"

#include "backend/common/file.h"
#include "common/file_util.h"
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
  Hash hash;
  {
    Store store(dir.GetPath());
    for (int i = 0; i < kNumElements; i++) {
      store.Set(i, i * i);
    }
    hash = *store.GetHash();
  }
  {
    Store restored(dir.GetPath());
    EXPECT_EQ(hash, *restored.GetHash());
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
