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

TEST(HashPageTest, SizeOfIsAsAdvertised) {
  EXPECT_EQ(64, sizeof(HashPage<int, int, int, 64>));
  EXPECT_EQ(128, sizeof(HashPage<int, int, int, 128>));
  EXPECT_EQ(128, sizeof(HashPage<int, int, long, 128>));
  EXPECT_EQ(1 << 14, sizeof(HashPage<int, int, long, 1 << 14>));
}

using TestPage = HashPage<std::size_t, int, int, 64>;

TEST(HashPageTest, NewPageIsEmpty) {
  TestPage page;
  EXPECT_EQ(0, page.Size());
}

TEST(HashPageTest, NewPageHasNoSuccessor) {
  TestPage page;
  EXPECT_EQ(0, page.GetNext());
}

TEST(HashPageTest, InsertedElementsCanBeFound) {
  TestPage page;
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
  const auto limit = TestPage::kNumEntries;
  for (std::size_t i = 0; i < limit; i++) {
    EXPECT_THAT(page.Insert(i, i, i), Pointee(FieldsAre(i, i, i)));
  }
  EXPECT_THAT(page.Insert(limit, 0, 0), IsNull());
  EXPECT_THAT(page.Insert(limit + 1, 0, 0), IsNull());
}

}  // namespace
}  // namespace carmen::backend::index
