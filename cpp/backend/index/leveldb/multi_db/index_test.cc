#include "backend/index/leveldb/multi_db/index.h"

#include "backend/index/test_util.h"
#include "common/file_util.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

using ::testing::IsOk;
using ::testing::IsOkAndHolds;
using ::testing::Not;
using ::testing::StatusIs;
using ::testing::StrEq;

using TestIndex = MultiLevelDbIndex<int, int>;

// Instantiates common index tests for the multi leveldb index type.
INSTANTIATE_TYPED_TEST_SUITE_P(LevelDb, IndexTest, TestIndex);

TEST(LevelDbMultiFileIndex, TestOpen) {
  auto dir = TempDir();
  ASSERT_THAT(TestIndex::Open(dir.GetPath()), IsOk());
}

TEST(LevelDbMultiFileIndex, IndexIsPersistent) {
  auto dir = TempDir();
  std::pair<int, bool> result;

  // Insert value in a separate block to ensure that the index is closed.
  {
    ASSERT_OK_AND_ASSIGN(auto index, TestIndex::Open(dir.GetPath()));
    EXPECT_THAT(index.Get(1), StatusIs(absl::StatusCode::kNotFound, _));
    ASSERT_OK_AND_ASSIGN(result, index.GetOrAdd(1));
    EXPECT_TRUE(result.second);
    EXPECT_THAT(index.Get(1), IsOkAndHolds(result.first));
  }

  // Reopen index and check that the value is still present.
  {
    ASSERT_OK_AND_ASSIGN(auto index, TestIndex::Open(dir.GetPath()));
    EXPECT_THAT(index.Get(1), IsOkAndHolds(result.first));
  }
}
}  // namespace
}  // namespace carmen::backend::index
