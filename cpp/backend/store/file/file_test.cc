#include "backend/store/file/file.h"

#include <filesystem>
#include <sstream>

#include "common/file_util.h"
#include "gtest/gtest.h"

namespace carmen::backend::store {
namespace {

TEST(InMemoryFileTest, IsFile) { EXPECT_TRUE((File<InMemoryFile<32>, 32>)); }

TEST(InMemoryFileTest, InitialFileIsEmpty) {
  InMemoryFile<32> file;
  EXPECT_EQ(0, file.GetNumPages());
}

TEST(InMemoryFileTest, PagesCanBeWrittenAndRead) {
  using Page = std::array<std::byte, 8>;
  InMemoryFile<8> file;

  Page page_a{std::byte{0x01}, std::byte{0x02}};
  file.StorePage(0, page_a);
  EXPECT_EQ(1, file.GetNumPages());

  Page restored;
  file.LoadPage(0, restored);
  EXPECT_EQ(page_a, restored);
}

TEST(InMemoryFileTest, PagesAreDifferentiated) {
  using Page = std::array<std::byte, 4>;
  InMemoryFile<4> file;

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
  using Page = std::array<std::byte, 8>;
  InMemoryFile<8> file;

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
  using Page = std::array<std::byte, 4>;
  InMemoryFile<4> file;
  Page zero{};
  Page loaded;
  loaded.fill(std::byte{1});
  file.LoadPage(0, loaded);
  EXPECT_EQ(zero, loaded);
}

TEST(SingleFileTest, IsFile) {
  EXPECT_TRUE((File<SingleFile<8>, 8>));
  EXPECT_TRUE((File<SingleFile<32>, 32>));
  EXPECT_FALSE((File<SingleFile<8>, 32>));
}

TEST(SingleFileTest, InitialFileIsEmpty) {
  TempFile temp_file;
  SingleFile<32> file(temp_file.GetPath());
  EXPECT_EQ(0, file.GetNumPages());
}

TEST(SingleFileTest, PagesCanBeWrittenAndRead) {
  using Page = std::array<std::byte, 8>;
  TempFile temp_file;
  SingleFile<8> file(temp_file.GetPath());

  Page page_a{std::byte{0x01}, std::byte{0x02}};
  file.StorePage(0, page_a);
  EXPECT_EQ(1, file.GetNumPages());

  Page restored;
  file.LoadPage(0, restored);
  EXPECT_EQ(page_a, restored);
}

TEST(SingleFileTest, PagesAreDifferentiated) {
  TempFile temp_file;
  using Page = std::array<std::byte, 4>;
  SingleFile<4> file(temp_file.GetPath());

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
  using Page = std::array<std::byte, 8>;
  SingleFile<8> file(temp_file.GetPath());

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
  using Page = std::array<std::byte, 4>;
  SingleFile<4> file(temp_file.GetPath());
  Page zero{};
  Page loaded;
  loaded.fill(std::byte{1});
  file.LoadPage(0, loaded);
  EXPECT_EQ(zero, loaded);
}

}  // namespace
}  // namespace carmen::backend::store
