#include "backend/store/file/page.h"

#include "common/type.h"
#include "gtest/gtest.h"

namespace carmen::backend::store {
namespace {

TEST(PageTest, PageSize) {
  EXPECT_EQ(10, sizeof(Page<int, 10>));
  EXPECT_EQ(50, sizeof(Page<int, 50>));
  EXPECT_EQ(4092, sizeof(Page<int, 4092>));

  EXPECT_EQ(10, sizeof(Page<Value, 10>));
  EXPECT_EQ(50, sizeof(Page<Value, 50>));
  EXPECT_EQ(4092, sizeof(Page<Value, 4092>));
}

TEST(PageTest, NumberOfElements) {
  EXPECT_EQ(0, (kNumElementsPerPage<int, 0>));
  EXPECT_EQ(2, (kNumElementsPerPage<int, 2 * sizeof(int)>));
  EXPECT_EQ(10, (kNumElementsPerPage<int, 10 * sizeof(int)>));

  EXPECT_EQ(2, (kNumElementsPerPage<std::uint8_t, 2 * sizeof(std::uint8_t)>));
  EXPECT_EQ(2, (kNumElementsPerPage<std::uint16_t, 2 * sizeof(std::uint16_t)>));
  EXPECT_EQ(2, (kNumElementsPerPage<std::uint32_t, 2 * sizeof(std::uint32_t)>));

  EXPECT_EQ(47 / sizeof(int), (Page<int, 47>::kNumElementsPerPage));
}

using TestPage = Page<int, 100>;

TEST(PageTest, ElementsCanBeAccessedAndAreDifferentiated) {
  constexpr auto kSize = TestPage::kNumElementsPerPage;

  TestPage page;
  for (std::size_t i = 0; i < kSize; i++) {
    page[i] = i;
  }

  for (std::size_t i = 0; i < kSize; i++) {
    EXPECT_EQ(i, page[i]);
  }
}

TEST(PageTest, PagesCanBeCopiedThroughTheirRawData) {
  Page<int, 64> page_a;
  for (std::size_t i = 0; i < page_a.kNumElementsPerPage; i++) {
    page_a[i] = i + 1;
  }

  Page<int, 64> page_b;
  auto src = page_a.AsRawData();
  auto dst = page_b.AsRawData();
  std::copy(src.begin(), src.end(), dst.begin());
  for (std::size_t i = 0; i < page_b.kNumElementsPerPage; i++) {
    EXPECT_EQ(i + 1, page_a[i]);
    EXPECT_EQ(i + 1, page_b[i]);
  }
}

}  // namespace
}  // namespace carmen::backend::store
