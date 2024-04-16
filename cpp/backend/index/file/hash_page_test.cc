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

#include "backend/index/file/hash_page.h"

#include "backend/common/page.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

using ::testing::FieldsAre;
using ::testing::IsNull;
using ::testing::Pointee;

TEST(HashPageTest, IsPage) {
  EXPECT_TRUE((Page<HashPage<int, int, int, 64>>));
  EXPECT_TRUE((Page<HashPage<int, int, long, 128>>));
}

TEST(HashPageTest, SizeOfFitsPageConstraints) {
  EXPECT_EQ(kFileSystemPageSize, sizeof(HashPage<int, int, int, 64>));
  EXPECT_EQ(kFileSystemPageSize, sizeof(HashPage<int, int, int, 128>));
  EXPECT_EQ(kFileSystemPageSize, sizeof(HashPage<int, int, long, 128>));
  EXPECT_EQ(1 << 14, sizeof(HashPage<int, int, long, 1 << 14>));
}

using TestPage = HashPage<std::size_t, int, int, 64>;

TEST(HashPageTest, ClearedPageIsEmpty) {
  TestPage page;
  page.Clear();
  EXPECT_EQ(0, page.Size());
}

TEST(HashPageTest, ClearedPageHasNoSuccessor) {
  TestPage page;
  page.Clear();
  EXPECT_EQ(0, page.GetNext());
}

TEST(HashPageTest, InsertedElementsCanBeFound) {
  TestPage page;
  page.Clear();
  EXPECT_THAT(page.Find(0, 1), IsNull());
  EXPECT_THAT(page.Find(2, 3), IsNull());
  EXPECT_THAT(page.Find(4, 5), IsNull());

  EXPECT_THAT(page.Insert(0, 1, 6), Pointee(FieldsAre(0, 1, 6)));
  EXPECT_THAT(page.Find(0, 1), Pointee(FieldsAre(0, 1, 6)));
  EXPECT_THAT(page.Find(2, 3), IsNull());
  EXPECT_THAT(page.Find(4, 5), IsNull());

  EXPECT_THAT(page.Insert(2, 3, 7), Pointee(FieldsAre(2, 3, 7)));
  EXPECT_THAT(page.Find(0, 1), Pointee(FieldsAre(0, 1, 6)));
  EXPECT_THAT(page.Find(2, 3), Pointee(FieldsAre(2, 3, 7)));
  EXPECT_THAT(page.Find(4, 5), IsNull());

  EXPECT_THAT(page.Insert(4, 5, 8), Pointee(FieldsAre(4, 5, 8)));
  EXPECT_THAT(page.Find(0, 1), Pointee(FieldsAre(0, 1, 6)));
  EXPECT_THAT(page.Find(2, 3), Pointee(FieldsAre(2, 3, 7)));
  EXPECT_THAT(page.Find(4, 5), Pointee(FieldsAre(4, 5, 8)));
}

TEST(HashPageTest, InsertFailsIfSizeLimitIsReached) {
  TestPage page;
  page.Clear();
  const auto limit = TestPage::kNumEntries;
  for (std::size_t i = 0; i < limit; i++) {
    EXPECT_THAT(page.Insert(i, i, i), Pointee(FieldsAre(i, i, i)));
  }
  EXPECT_THAT(page.Insert(limit, 0, 0), IsNull());
  EXPECT_THAT(page.Insert(limit + 1, 0, 0), IsNull());
}

}  // namespace
}  // namespace carmen::backend::index
