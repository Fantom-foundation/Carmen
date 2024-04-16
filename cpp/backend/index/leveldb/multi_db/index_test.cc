/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3.
 */

#include "backend/index/leveldb/multi_db/index.h"

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

using TestIndex = MultiLevelDbIndex<int, int>;

// Instantiates common index tests for the multi leveldb index type.
INSTANTIATE_TYPED_TEST_SUITE_P(LevelDb, IndexTest, TestIndex);

TEST(LevelDbMultiFileIndex, TestOpen) {
  auto dir = TempDir();
  EXPECT_OK(TestIndex::Open(dir.GetPath()));
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
