#include "backend/store/file/store.h"

#include "backend/common/file.h"
#include "common/file_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::store {
namespace {

TEST(FileStoreTest, StoreCanBeSavedAndRestored) {
  using Store = FileStore<int, int, SingleFile>;
  const auto kNumElements = static_cast<int>(Store::kPageSize * 10);
  TempDir dir;
  Hash hash;
  {
    Store store(dir.GetPath());
    for (int i = 0; i < kNumElements; i++) {
      store.Set(i, i * i);
    }
    hash = store.GetHash();
  }
  {
    Store restored(dir.GetPath());
    EXPECT_EQ(hash, restored.GetHash());
    for (int i = 0; i < kNumElements; i++) {
      EXPECT_EQ(restored.Get(i), i * i);
    }
  }
}

}  // namespace
}  // namespace carmen::backend::store
