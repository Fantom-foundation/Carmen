#include "backend/common/page.h"

#include "common/type.h"
#include "gtest/gtest.h"

namespace carmen::backend {
namespace {

constexpr auto FsPageSize = kFileSystemPageSize;

TEST(GetRequiredPageSizeTest, RoundsUpUsage) {
  EXPECT_EQ(FsPageSize, GetRequiredPageSize(0));
  EXPECT_EQ(FsPageSize, GetRequiredPageSize(1));
  EXPECT_EQ(FsPageSize, GetRequiredPageSize(FsPageSize - 1));
  EXPECT_EQ(FsPageSize, GetRequiredPageSize(FsPageSize));
  EXPECT_EQ(2 * FsPageSize, GetRequiredPageSize(FsPageSize + 1));
  EXPECT_EQ(2 * FsPageSize, GetRequiredPageSize(2 * FsPageSize - 1));
  EXPECT_EQ(2 * FsPageSize, GetRequiredPageSize(2 * FsPageSize));
  EXPECT_EQ(3 * FsPageSize, GetRequiredPageSize(2 * FsPageSize + 1));
}

TEST(RawPageTest, IsPage) {
  EXPECT_TRUE(Page<RawPage<>>);
  EXPECT_TRUE(Page<RawPage<4096>>);
  EXPECT_TRUE(Page<RawPage<4 * 4096>>);

  EXPECT_FALSE(Page<RawPage<256>>);
  EXPECT_FALSE(Page<RawPage<4095>>);
  EXPECT_FALSE(Page<RawPage<4097>>);
}

TEST(ArrayPageTest, ArePages) {
  EXPECT_TRUE((Page<ArrayPage<int>>));
  EXPECT_TRUE((Page<ArrayPage<float>>));
  EXPECT_TRUE((Page<ArrayPage<int, 12>>));
  EXPECT_TRUE((Page<ArrayPage<float, 73>>));
  EXPECT_TRUE((Page<ArrayPage<int, FsPageSize * 4>>));
}

TEST(ArrayPageTest, PageSize) {
  EXPECT_EQ(FsPageSize, sizeof(ArrayPage<int, 10>));
  EXPECT_EQ(FsPageSize, sizeof(ArrayPage<int, 50>));
  EXPECT_EQ(FsPageSize, sizeof(ArrayPage<int, FsPageSize / sizeof(int)>));
  EXPECT_EQ(FsPageSize * 2,
            sizeof(ArrayPage<int, FsPageSize / sizeof(int) + 1>));
  EXPECT_EQ(FsPageSize * 2,
            sizeof(ArrayPage<int, FsPageSize / sizeof(int) * 2>));

  EXPECT_EQ(FsPageSize, sizeof(ArrayPage<Value, 10>));
  EXPECT_EQ(FsPageSize, sizeof(ArrayPage<Value, 50>));
  EXPECT_EQ(FsPageSize, sizeof(ArrayPage<Value, 4092 / sizeof(Value)>));
  EXPECT_EQ(FsPageSize * 2,
            sizeof(ArrayPage<Value, FsPageSize / sizeof(Value) + 1>));
}

TEST(ArrayPageTest, NumberOfElements) {
  EXPECT_EQ(0, (ArrayPage<int, 0>::kNumElementsPerPage));
  EXPECT_EQ(2, (ArrayPage<int, 2>::kNumElementsPerPage));
  EXPECT_EQ(10, (ArrayPage<int, 10>::kNumElementsPerPage));

  EXPECT_EQ(2, (ArrayPage<std::uint8_t, 2>::kNumElementsPerPage));
  EXPECT_EQ(2, (ArrayPage<std::uint16_t, 2>::kNumElementsPerPage));
  EXPECT_EQ(2, (ArrayPage<std::uint32_t, 2>::kNumElementsPerPage));

  EXPECT_EQ(47, (ArrayPage<int, 47>::kNumElementsPerPage));
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

  for (std::size_t i = 0; i < kSize; i++) {
    EXPECT_EQ(i, page.AsArray()[i]);
  }
}

TEST(ArrayPageTest, PagesCanBeCopiedThroughTheirRawData) {
  ArrayPage<int, 64> page_a;
  for (std::size_t i = 0; i < page_a.kNumElementsPerPage; i++) {
    page_a[i] = i + 1;
  }

  ArrayPage<int, 64> page_b;
  auto src = page_a.operator std::span<const std::byte, 4096>();
  auto dst = page_b.operator std::span<std::byte, 4096>();
  std::copy(src.begin(), src.end(), dst.begin());
  for (std::size_t i = 0; i < page_b.kNumElementsPerPage; i++) {
    EXPECT_EQ(i + 1, page_a[i]);
    EXPECT_EQ(i + 1, page_b[i]);
  }
}

}  // namespace
}  // namespace carmen::backend
