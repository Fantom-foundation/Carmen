// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

#include "backend/index/leveldb/single_db/index.h"

#include "absl/status/status.h"
#include "backend/index/index_test_suite.h"
#include "common/file_util.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

using ::testing::_;
using ::testing::IsOkAndHolds;
using ::testing::StatusIs;
using ::testing::StrEq;

using TestIndex = LevelDbKeySpace<int, int>;

// Instantiates common index tests for the single leveldb index type.
INSTANTIATE_TYPED_TEST_SUITE_P(LevelDb, IndexTest, TestIndex);

absl::StatusOr<LevelDbKeySpace<int, int>> GetTestIndex(const TempDir& dir) {
  ASSIGN_OR_RETURN(auto index, SingleLevelDbIndex::Open(dir.GetPath()));
  return index.KeySpace<int, int>('t');
}

TEST(LevelDbIndexTest, ConvertToLevelDbKey) {
  int key = 21;
  auto res = internal::ToDBKey('A', key);
  std::stringstream ss;
  ss << 'A';
  ss.write(reinterpret_cast<const char*>(&key), sizeof(key));
  EXPECT_THAT(std::string(res.data(), res.size()), StrEq(ss.str()));
}

TEST(LevelDbIndexTest, ConvertAndParseLevelDbValue) {
  std::uint8_t input = 69;
  auto value = internal::ToDBValue(input);
  EXPECT_EQ(input, *internal::ParseDBResult<std::uint8_t>(value));
}

TEST(LevelDbIndexTest, IndexIsPersistent) {
  TempDir dir = TempDir();
  std::pair<int, bool> result;

  // Insert value in a separate block to ensure that the index is closed.
  {
    ASSERT_OK_AND_ASSIGN(auto index, GetTestIndex(dir));
    EXPECT_THAT(index.Get(1), StatusIs(absl::StatusCode::kNotFound, _));
    ASSERT_OK_AND_ASSIGN(result, index.GetOrAdd(1));
    EXPECT_TRUE(result.second);
    EXPECT_THAT(index.Get(1), IsOkAndHolds(result.first));
  }

  // Reopen index and check that the value is still present.
  {
    ASSERT_OK_AND_ASSIGN(auto index, GetTestIndex(dir));
    ASSERT_OK_AND_ASSIGN(result, index.GetOrAdd(1));
    EXPECT_THAT(index.Get(1), IsOkAndHolds(result.first));
  }
}

}  // namespace
}  // namespace carmen::backend::index
