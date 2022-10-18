#include "backend/index/leveldb/index.h"

#include "absl/status/statusor.h"
#include "backend/index/leveldb/test_util.h"
#include "backend/index/test_util.h"
#include "common/file_util.h"
#include "common/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

using ::testing::StrEq;

using TestIndex = LevelDBKeySpaceTestAdapter<int, int>;

// Instantiates common index tests for the Cached index type.
INSTANTIATE_TYPED_TEST_SUITE_P(LevelDB, IndexTest, TestIndex);

LevelDBKeySpace<int, int> GetTestIndex(const TempDir& dir) {
  return LevelDBIndex(dir.GetPath().string()).KeySpace<int, int>('t');
}

TEST(LevelDBIndexTest, ConvertToLevelDBKey) {
  int key = 21;
  auto res = internal::ToDBKey('A', key);
  std::stringstream ss;
  ss << 'A';
  ss.write(reinterpret_cast<const char*>(&key), sizeof(key));
  EXPECT_THAT(res, StrEq(ss.str()));
}

TEST(LevelDBIndexTest, ConvertAndParseLevelDBValue) {
  std::uint8_t input = 69;
  auto value = internal::ToDBValue(input);
  EXPECT_EQ(input, *internal::ParseDBResult<std::uint8_t>(value));
}

TEST(LevelDBIndexTest, IndexIsPersistent) {
  TempDir dir = TempDir();
  absl::StatusOr<std::pair<int, bool>> result;

  // Insert value in a separate block to ensure that the index is closed.
  {
    auto index = GetTestIndex(dir);
    EXPECT_THAT(index.Get(1).status().code(), absl::StatusCode::kNotFound);
    result = index.GetOrAdd(1);
    EXPECT_EQ((*result).second, true);
    EXPECT_THAT(*index.Get(1), (*result).first);
  }

  // Reopen index and check that the value is still present.
  {
    auto index = GetTestIndex(dir);
    EXPECT_THAT(*index.Get(1), (*result).first);
  }
}

}  // namespace
}  // namespace carmen::backend::index
