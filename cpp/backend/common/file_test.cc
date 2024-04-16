/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3 
 */

#include "backend/common/file.h"

#include <filesystem>
#include <sstream>

#include "backend/common/page.h"
#include "common/file_util.h"
#include "common/status_test_util.h"
#include "gtest/gtest.h"

namespace carmen::backend::store {
namespace {

using ::testing::_;
using ::testing::Return;
using ::testing::StatusIs;

// A page format used for the tests.
template <std::size_t page_size>
class alignas(kFileSystemPageSize) Page
    : public std::array<std::byte, page_size> {
 public:
  constexpr static const auto kPageSize = page_size;

  operator std::span<const std::byte, kPageSize>() const {
    return std::span<const std::byte, kPageSize>{&(*this)[0], kPageSize};
  }

  operator std::span<std::byte, kPageSize>() {
    return std::span<std::byte, kPageSize>{&(*this)[0], kPageSize};
  }
};

TEST(TestPageTest, IsPage) {
  EXPECT_TRUE(carmen::backend::Page<Page<kFileSystemPageSize>>);
  EXPECT_TRUE(carmen::backend::Page<Page<2 * kFileSystemPageSize>>);
}

TEST(InMemoryFileTest, IsFile) {
  EXPECT_TRUE((File<InMemoryFile<4>>));
  EXPECT_TRUE((File<InMemoryFile<8>>));
  EXPECT_TRUE((File<InMemoryFile<16>>));
  EXPECT_TRUE((File<InMemoryFile<32>>));
}

TEST(InMemoryFileTest, InitialFileIsEmpty) {
  ASSERT_OK_AND_ASSIGN(auto file, InMemoryFile<32>::Open(""));
  EXPECT_EQ(0, file.GetNumPages());
}

TEST(InMemoryFileTest, PagesCanBeWrittenAndRead) {
  using Page = Page<8>;
  ASSERT_OK_AND_ASSIGN(auto file, InMemoryFile<Page::kPageSize>::Open(""));

  Page page_a{std::byte{0x01}, std::byte{0x02}};
  ASSERT_OK(file.StorePage(0, page_a));
  EXPECT_EQ(1, file.GetNumPages());

  Page restored;
  ASSERT_OK(file.LoadPage(0, restored));
  EXPECT_EQ(page_a, restored);
}

TEST(InMemoryFileTest, PagesAreDifferentiated) {
  using Page = Page<4>;
  ASSERT_OK_AND_ASSIGN(auto file, InMemoryFile<Page::kPageSize>::Open(""));

  Page page_a{std::byte{0x01}, std::byte{0x02}};
  Page page_b{std::byte{0x03}, std::byte{0x04}};

  ASSERT_OK(file.StorePage(0, page_a));
  ASSERT_OK(file.StorePage(1, page_b));
  EXPECT_EQ(2, file.GetNumPages());

  Page restored;
  ASSERT_OK(file.LoadPage(0, restored));
  EXPECT_EQ(page_a, restored);
  ASSERT_OK(file.LoadPage(1, restored));
  EXPECT_EQ(page_b, restored);
}

TEST(InMemoryFileTest, WritingPagesCreatesImplicitEmptyPages) {
  using Page = Page<8>;
  ASSERT_OK_AND_ASSIGN(auto file, InMemoryFile<Page::kPageSize>::Open(""));

  // Storing a page at position 2 implicitly creates pages 0 and 1.
  Page page_a{std::byte{0x01}, std::byte{0x02}};
  ASSERT_OK(file.StorePage(2, page_a));
  EXPECT_EQ(3, file.GetNumPages());

  Page zero{};
  Page restored;
  ASSERT_OK(file.LoadPage(0, restored));
  EXPECT_EQ(zero, restored);
  ASSERT_OK(file.LoadPage(1, restored));
  EXPECT_EQ(zero, restored);
  ASSERT_OK(file.LoadPage(2, restored));
  EXPECT_EQ(page_a, restored);
}

TEST(InMemoryFileTest, LoadingUninitializedPagesLeadsToZeros) {
  using Page = Page<4>;
  ASSERT_OK_AND_ASSIGN(auto file, InMemoryFile<Page::kPageSize>::Open(""));
  Page zero{};
  Page loaded;
  loaded.fill(std::byte{1});
  ASSERT_OK(file.LoadPage(0, loaded));
  EXPECT_EQ(zero, loaded);
}

template <typename F>
class SingleFileTest : public testing::Test {};

TYPED_TEST_SUITE_P(SingleFileTest);

TYPED_TEST_P(SingleFileTest, IsFile) {
  using RawFile = TypeParam;
  EXPECT_TRUE((File<SingleFileBase<8, RawFile>>));
  EXPECT_TRUE((File<SingleFileBase<32, RawFile>>));
}

TYPED_TEST_P(SingleFileTest, ExistingFileCanBeOpened) {
  using File = SingleFileBase<32, TypeParam>;
  TempFile temp_file;
  ASSERT_TRUE(std::filesystem::exists(temp_file.GetPath()));
  ASSERT_OK_AND_ASSIGN(auto file, File::Open(temp_file.GetPath()));
  EXPECT_EQ(0, file.GetNumPages());
}

TYPED_TEST_P(SingleFileTest, NonExistingFileIsCreated) {
  using File = SingleFileBase<32, TypeParam>;
  TempFile temp_file;
  ASSERT_TRUE(std::filesystem::exists(temp_file.GetPath()));
  std::filesystem::remove(temp_file.GetPath());
  ASSERT_FALSE(std::filesystem::exists(temp_file.GetPath()));
  ASSERT_OK_AND_ASSIGN(auto file, File::Open(temp_file.GetPath()));
  EXPECT_TRUE(std::filesystem::exists(temp_file.GetPath()));
  EXPECT_EQ(0, file.GetNumPages());
}

TYPED_TEST_P(SingleFileTest, NestedDirectoryIsCreatedIfNeeded) {
  using File = SingleFileBase<32, TypeParam>;
  TempDir temp_dir;
  ASSERT_OK_AND_ASSIGN(
      auto file, File::Open(temp_dir.GetPath() / "some" / "dir" / "file.dat"));
  EXPECT_TRUE(std::filesystem::exists(temp_dir.GetPath()));
  EXPECT_TRUE(std::filesystem::exists(temp_dir.GetPath() / "some"));
  EXPECT_TRUE(std::filesystem::exists(temp_dir.GetPath() / "some" / "dir"));
  EXPECT_TRUE(std::filesystem::exists(temp_dir.GetPath() / "some" / "dir" /
                                      "file.dat"));
  EXPECT_EQ(0, file.GetNumPages());
}

TYPED_TEST_P(SingleFileTest, InitialFileIsEmpty) {
  using File = SingleFileBase<32, TypeParam>;
  TempFile temp_file;
  ASSERT_OK_AND_ASSIGN(auto file, File::Open(temp_file.GetPath()));
  EXPECT_EQ(0, file.GetNumPages());
}

TYPED_TEST_P(SingleFileTest, PagesCanBeWrittenAndRead) {
  using Page = Page<kFileSystemPageSize>;
  using File = SingleFileBase<Page::kPageSize, TypeParam>;
  TempFile temp_file;
  ASSERT_OK_AND_ASSIGN(auto file, File::Open(temp_file.GetPath()));

  Page page_a{std::byte{0x01}, std::byte{0x02}};
  ASSERT_OK(file.StorePage(0, page_a));
  EXPECT_EQ(1, file.GetNumPages());

  Page restored;
  ASSERT_OK(file.LoadPage(0, restored));
  EXPECT_EQ(page_a, restored);
}

TYPED_TEST_P(SingleFileTest, PagesAreDifferentiated) {
  using Page = Page<kFileSystemPageSize>;
  using File = SingleFileBase<Page::kPageSize, TypeParam>;
  TempFile temp_file;
  ASSERT_OK_AND_ASSIGN(auto file, File::Open(temp_file.GetPath()));

  Page page_a{std::byte{0x01}, std::byte{0x02}};
  Page page_b{std::byte{0x03}, std::byte{0x04}};

  ASSERT_OK(file.StorePage(0, page_a));
  ASSERT_OK(file.StorePage(1, page_b));
  EXPECT_EQ(2, file.GetNumPages());

  Page restored;
  ASSERT_OK(file.LoadPage(0, restored));
  EXPECT_EQ(page_a, restored);
  ASSERT_OK(file.LoadPage(1, restored));
  EXPECT_EQ(page_b, restored);
}

TYPED_TEST_P(SingleFileTest, WritingPagesCreatesImplicitEmptyPages) {
  using Page = Page<kFileSystemPageSize>;
  using File = SingleFileBase<Page::kPageSize, TypeParam>;
  TempFile temp_file;
  ASSERT_OK_AND_ASSIGN(auto file, File::Open(temp_file.GetPath()));

  // Storing a page at position 2 implicitly creates pages 0 and 1.
  Page page_a{std::byte{0x01}, std::byte{0x02}};
  ASSERT_OK(file.StorePage(2, page_a));
  EXPECT_EQ(3, file.GetNumPages());

  Page zero{};
  Page restored;
  ASSERT_OK(file.LoadPage(0, restored));
  EXPECT_EQ(zero, restored);
  ASSERT_OK(file.LoadPage(1, restored));
  EXPECT_EQ(zero, restored);
  ASSERT_OK(file.LoadPage(2, restored));
  EXPECT_EQ(page_a, restored);
}

TYPED_TEST_P(SingleFileTest, LoadingUninitializedPagesLeadsToZeros) {
  using Page = Page<kFileSystemPageSize>;
  using File = SingleFileBase<Page::kPageSize, TypeParam>;
  TempFile temp_file;
  ASSERT_OK_AND_ASSIGN(auto file, File::Open(temp_file.GetPath()));
  Page zero{};
  Page loaded;
  loaded.fill(std::byte{1});
  ASSERT_OK(file.LoadPage(0, loaded));
  EXPECT_EQ(zero, loaded);
}

TYPED_TEST_P(SingleFileTest, EmptyFileCanBeClosedAndReopenedAsEmpty) {
  using Page = Page<kFileSystemPageSize>;
  using File = SingleFileBase<Page::kPageSize, TypeParam>;
  TempFile temp_file;
  {
    ASSERT_OK_AND_ASSIGN(auto file, File::Open(temp_file));
    EXPECT_EQ(file.GetNumPages(), 0);
    EXPECT_OK(file.Close());
  }
  {
    ASSERT_OK_AND_ASSIGN(auto file, File::Open(temp_file));
    EXPECT_EQ(file.GetNumPages(), 0);
    EXPECT_OK(file.Close());
  }
}

TYPED_TEST_P(SingleFileTest,
             NonEmptyFileCanBeClosedAndReopenedWithSameContent) {
  using Page = Page<kFileSystemPageSize>;
  using File = SingleFileBase<Page::kPageSize, TypeParam>;
  TempFile temp_file;
  Page content{std::byte{0x01}, std::byte{0x02}};
  {
    ASSERT_OK_AND_ASSIGN(auto file, File::Open(temp_file));
    EXPECT_EQ(file.GetNumPages(), 0);
    EXPECT_OK(file.StorePage(0, content));
    EXPECT_EQ(file.GetNumPages(), 1);
    EXPECT_OK(file.Close());
  }
  {
    ASSERT_OK_AND_ASSIGN(auto file, File::Open(temp_file));
    EXPECT_EQ(file.GetNumPages(), 1);
    Page restored;
    EXPECT_OK(file.LoadPage(0, restored));
    EXPECT_EQ(content, restored);
    EXPECT_OK(file.Close());
  }
}

TYPED_TEST_P(SingleFileTest, OpenFileErrorIsHandled) {
  using Page = Page<kFileSystemPageSize>;
  using File = SingleFileBase<Page::kPageSize, TypeParam>;
  // File should not be able to be opened if directory is passed instead of a
  // file.
  TempDir temp_dir;
  auto status = File::Open(temp_dir.GetPath());
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInternal, _));
}

REGISTER_TYPED_TEST_SUITE_P(SingleFileTest, IsFile, ExistingFileCanBeOpened,
                            NonExistingFileIsCreated,
                            NestedDirectoryIsCreatedIfNeeded,
                            InitialFileIsEmpty, PagesCanBeWrittenAndRead,
                            PagesAreDifferentiated,
                            WritingPagesCreatesImplicitEmptyPages,
                            LoadingUninitializedPagesLeadsToZeros,
                            EmptyFileCanBeClosedAndReopenedAsEmpty,
                            NonEmptyFileCanBeClosedAndReopenedWithSameContent,
                            OpenFileErrorIsHandled);

using RawFileTypes = ::testing::Types<internal::FStreamFile, internal::CFile,
                                      internal::PosixFile>;
INSTANTIATE_TYPED_TEST_SUITE_P(My, SingleFileTest, RawFileTypes);

class MockErrorFile {
 public:
  static absl::StatusOr<MockErrorFile> Open(const std::filesystem::path&) {
    return MockErrorFile();
  }
  auto GetFileSize() const { return mock_->GetFileSize(); }
  auto Read(auto a, auto b) { return mock_->Read(a, b); }
  auto Write(auto a, auto b) { return mock_->Write(a, b); }
  auto Flush() { return mock_->Flush(); }
  auto Close() { return mock_->Close(); }
  auto& GetMock() { return *mock_; }

 private:
  class Mock {
   public:
    MOCK_METHOD(size_t, GetFileSize, (), (const));
    MOCK_METHOD(absl::Status, Read, (std::size_t, std::span<std::byte>));
    MOCK_METHOD(absl::Status, Write, (std::size_t, std::span<const std::byte>));
    MOCK_METHOD(absl::Status, Flush, ());
    MOCK_METHOD(absl::Status, Close, ());
  };
  // Mock is wrapped in unique_ptr to allow move semantics.
  std::unique_ptr<Mock> mock_{std::make_unique<Mock>()};
};

TEST(SingleFileErrorTest, LoadPageErrorIsHandled) {
  using Page = Page<kFileSystemPageSize>;
  using File = SingleFileBase<Page::kPageSize, MockErrorFile>;
  TempFile temp_file;
  ASSERT_OK_AND_ASSIGN(auto file, File::Open(temp_file.GetPath()));

  auto& mock = file.GetRawFile().GetMock();
  EXPECT_CALL(mock, Read(0, _)).WillOnce(Return(absl::InternalError("")));

  auto status = file.LoadPage(0, Page{});
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInternal, _));
}

TEST(SingleFileErrorTest, StorePageErrorIsHandled) {
  using Page = Page<kFileSystemPageSize>;
  using File = SingleFileBase<Page::kPageSize, MockErrorFile>;
  TempFile temp_file;
  ASSERT_OK_AND_ASSIGN(auto file, File::Open(temp_file.GetPath()));

  auto& mock = file.GetRawFile().GetMock();
  EXPECT_CALL(mock, Write(0, _)).WillOnce(Return(absl::InternalError("")));

  auto status = file.StorePage(0, Page{});
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInternal, _));
}

TEST(SingleFileErrorTest, FlushErrorIsHandled) {
  using Page = Page<kFileSystemPageSize>;
  using File = SingleFileBase<Page::kPageSize, MockErrorFile>;
  TempFile temp_file;
  ASSERT_OK_AND_ASSIGN(auto file, File::Open(temp_file.GetPath()));

  auto& mock = file.GetRawFile().GetMock();
  EXPECT_CALL(mock, Flush).WillOnce(Return(absl::InternalError("")));

  auto status = file.Flush();
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInternal, _));
}

TEST(SingleFileErrorTest, CloseErrorIsHandled) {
  using Page = Page<kFileSystemPageSize>;
  using File = SingleFileBase<Page::kPageSize, MockErrorFile>;
  TempFile temp_file;
  ASSERT_OK_AND_ASSIGN(auto file, File::Open(temp_file.GetPath()));

  auto& mock = file.GetRawFile().GetMock();
  EXPECT_CALL(mock, Close).WillOnce(Return(absl::InternalError("")));

  auto status = file.Close();
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInternal, _));
}

}  // namespace
}  // namespace carmen::backend::store
