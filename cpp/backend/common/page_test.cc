#include "backend/common/page.h"

#include "common/type.h"
#include "gtest/gtest.h"

namespace carmen::backend {
namespace {

TEST(ArrayPageTest, ArePages) {
  EXPECT_TRUE((Page<ArrayPage<int, 12>>));
  EXPECT_TRUE((Page<ArrayPage<float, 73>>));
}

TEST(ArrayPageTest, PageSize) {
  EXPECT_EQ(10, sizeof(ArrayPage<int, 10>));
  EXPECT_EQ(50, sizeof(ArrayPage<int, 50>));
  EXPECT_EQ(4092, sizeof(ArrayPage<int, 4092>));

  EXPECT_EQ(10, sizeof(ArrayPage<Value, 10>));
  EXPECT_EQ(50, sizeof(ArrayPage<Value, 50>));
  EXPECT_EQ(4092, sizeof(ArrayPage<Value, 4092>));
}

TEST(ArrayPageTest, NumberOfElements) {
  EXPECT_EQ(0, (ArrayPage<int, 0>::kNumElementsPerPage));
  EXPECT_EQ(2, (ArrayPage<int, 2 * sizeof(int)>::kNumElementsPerPage));
  EXPECT_EQ(10, (ArrayPage<int, 10 * sizeof(int)>::kNumElementsPerPage));

  EXPECT_EQ(
      2,
      (ArrayPage<std::uint8_t, 2 * sizeof(std::uint8_t)>::kNumElementsPerPage));
  EXPECT_EQ(2, (ArrayPage<std::uint16_t,
                          2 * sizeof(std::uint16_t)>::kNumElementsPerPage));
  EXPECT_EQ(2, (ArrayPage<std::uint32_t,
                          2 * sizeof(std::uint32_t)>::kNumElementsPerPage));

  EXPECT_EQ(47 / sizeof(int), (ArrayPage<int, 47>::kNumElementsPerPage));
}

using TestPage = ArrayPage<int, 100>;

TEST(ArrayPageTest, ElementsCanBeAccessedAndAreDifferentiated) {
  constexpr auto kSize = TestPage::kNumElementsPerPage;

  TestPage page;
  for (std::size_t i = 0; i < kSize; i++) {
    page[i] = i;
  }

  for (std::size_t i = 0; i < kSize; i++) {
    EXPECT_EQ(i, page[i]);
  }
}

TEST(ArrayPageTest, PagesCanBeCopiedThroughTheirRawData) {
  ArrayPage<int, 64> page_a;
  for (std::size_t i = 0; i < page_a.kNumElementsPerPage; i++) {
    page_a[i] = i + 1;
  }

  ArrayPage<int, 64> page_b;
  auto src = page_a.AsRawData();
  auto dst = page_b.AsRawData();
  std::copy(src.begin(), src.end(), dst.begin());
  for (std::size_t i = 0; i < page_b.kNumElementsPerPage; i++) {
    EXPECT_EQ(i + 1, page_a[i]);
    EXPECT_EQ(i + 1, page_b[i]);
  }
}

}  // namespace
}  // namespace carmen::backend
