#include "backend/index/leveldb/multi_file/index.h"

#include "common/file_util.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

using TestIndex = LevelDBIndex<int, int>;

using ::testing::IsOk;
using ::testing::Not;
using ::testing::StrEq;

TEST(LevelDBMultiFileIndex, TestOpen) {
  auto dir = TempDir();
  ASSERT_THAT(TestIndex::Open(dir.GetPath()), IsOk());

  TestIndex::Open(dir.GetPath()).value();

}

TEST(LevelDBMultiFileIndex, IndexIsPersistent) {
  auto dir = TempDir();
  absl::StatusOr<std::pair<int, bool>> result;

  // Insert value in a separate block to ensure that the index is closed.
  {
    auto index = *TestIndex::Open(dir.GetPath());
    EXPECT_THAT(index.Get(1).status().code(), absl::StatusCode::kNotFound);
    //result = index.GetOrAdd(1);
    //ASSERT_OK(result);
    //EXPECT_EQ((*result).second, true);
    //EXPECT_THAT(*index.Get(1), (*result).first);
  }

  // Reopen index and check that the value is still present.
  {
    //auto index = *TestIndex::Open(dir.GetPath());
    //EXPECT_THAT(*index.Get(1), (*result).first);
  }
}

}  // namespace
}  // namespace carmen::backend::index
