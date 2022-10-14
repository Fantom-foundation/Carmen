#include "backend/common/file.h"

#include <filesystem>
#include <sstream>

#include "backend/common/page.h"
#include "common/file_util.h"
#include "gtest/gtest.h"

namespace carmen::backend::store {
namespace {

// A page format used for the tests.
template <std::size_t page_size>
class Page : public std::array<std::byte, page_size> {
 public:
  std::span<const std::byte, page_size> AsRawData() const { return *this; };
  std::span<std::byte, page_size> AsRawData() { return *this; };
};

TEST(TestPageTest, IsPage) { EXPECT_TRUE(carmen::backend::Page<Page<12>>); }

TEST(InMemoryFileTest, IsFile) { EXPECT_TRUE((File<InMemoryFile<Page<32>>>)); }

TEST(InMemoryFileTest, InitialFileIsEmpty) {
  InMemoryFile<Page<32>> file;
  EXPECT_EQ(0, file.GetNumPages());
}

TEST(InMemoryFileTest, PagesCanBeWrittenAndRead) {
  using Page = Page<8>;
  InMemoryFile<Page> file;

  Page page_a{std::byte{0x01}, std::byte{0x02}};
  file.StorePage(0, page_a);
  EXPECT_EQ(1, file.GetNumPages());

  Page restored;
  file.LoadPage(0, restored);
  EXPECT_EQ(page_a, restored);
}

TEST(InMemoryFileTest, PagesAreDifferentiated) {
  using Page = Page<4>;
  InMemoryFile<Page> file;

  Page page_a{std::byte{0x01}, std::byte{0x02}};
  Page page_b{std::byte{0x03}, std::byte{0x04}};

  file.StorePage(0, page_a);
  file.StorePage(1, page_b);
  EXPECT_EQ(2, file.GetNumPages());

  Page restored;
  file.LoadPage(0, restored);
  EXPECT_EQ(page_a, restored);
  file.LoadPage(1, restored);
  EXPECT_EQ(page_b, restored);
}

TEST(InMemoryFileTest, WritingPagesCreatesImplicitEmptyPages) {
  using Page = Page<8>;
  InMemoryFile<Page> file;

  // Storing a page at position 2 implicitly creates pages 0 and 1.
  Page page_a{std::byte{0x01}, std::byte{0x02}};
  file.StorePage(2, page_a);
  EXPECT_EQ(3, file.GetNumPages());

  Page zero{};
  Page restored;
  file.LoadPage(0, restored);
  EXPECT_EQ(zero, restored);
  file.LoadPage(1, restored);
  EXPECT_EQ(zero, restored);
  file.LoadPage(2, restored);
  EXPECT_EQ(page_a, restored);
}

TEST(InMemoryFileTest, LoadingUninitializedPagesLeadsToZeros) {
  using Page = Page<4>;
  InMemoryFile<Page> file;
  Page zero{};
  Page loaded;
  loaded.fill(std::byte{1});
  file.LoadPage(0, loaded);
  EXPECT_EQ(zero, loaded);
}

TEST(SingleFileTest, IsFile) {
  EXPECT_TRUE(File<SingleFile<Page<8>>>);
  EXPECT_TRUE(File<SingleFile<Page<32>>>);
}

TEST(SingleFileTest, InitialFileIsEmpty) {
  TempFile temp_file;
  SingleFile<Page<32>> file(temp_file.GetPath());
  EXPECT_EQ(0, file.GetNumPages());
}

TEST(SingleFileTest, PagesCanBeWrittenAndRead) {
  using Page = Page<8>;
  TempFile temp_file;
  SingleFile<Page> file(temp_file.GetPath());

  Page page_a{std::byte{0x01}, std::byte{0x02}};
  file.StorePage(0, page_a);
  EXPECT_EQ(1, file.GetNumPages());

  Page restored;
  file.LoadPage(0, restored);
  EXPECT_EQ(page_a, restored);
}

TEST(SingleFileTest, PagesAreDifferentiated) {
  TempFile temp_file;
  using Page = Page<4>;
  SingleFile<Page> file(temp_file.GetPath());

  Page page_a{std::byte{0x01}, std::byte{0x02}};
  Page page_b{std::byte{0x03}, std::byte{0x04}};

  file.StorePage(0, page_a);
  file.StorePage(1, page_b);
  EXPECT_EQ(2, file.GetNumPages());

  Page restored;
  file.LoadPage(0, restored);
  EXPECT_EQ(page_a, restored);
  file.LoadPage(1, restored);
  EXPECT_EQ(page_b, restored);
}

TEST(SingleFileTest, WritingPagesCreatesImplicitEmptyPages) {
  TempFile temp_file;
  using Page = Page<8>;
  SingleFile<Page> file(temp_file.GetPath());

  // Storing a page at position 2 implicitly creates pages 0 and 1.
  Page page_a{std::byte{0x01}, std::byte{0x02}};
  file.StorePage(2, page_a);
  EXPECT_EQ(3, file.GetNumPages());

  Page zero{};
  Page restored;
  file.LoadPage(0, restored);
  EXPECT_EQ(zero, restored);
  file.LoadPage(1, restored);
  EXPECT_EQ(zero, restored);
  file.LoadPage(2, restored);
  EXPECT_EQ(page_a, restored);
}

TEST(SingleFileTest, LoadingUninitializedPagesLeadsToZeros) {
  TempFile temp_file;
  using Page = Page<4>;
  SingleFile<Page> file(temp_file.GetPath());
  Page zero{};
  Page loaded;
  loaded.fill(std::byte{1});
  file.LoadPage(0, loaded);
  EXPECT_EQ(zero, loaded);
}

}  // namespace
}  // namespace carmen::backend::store
