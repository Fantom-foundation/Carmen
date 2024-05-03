/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use of
 * this software will be governed by the GNU Lesser General Public License v3.
 */

#include "common/fstream.h"

#include <type_traits>

#include "common/file_util.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen {
namespace {

using ::testing::ElementsAre;
using ::testing::StartsWith;
using ::testing::StatusIs;

TEST(FStreamTest, TypeTraits) {
  EXPECT_TRUE(std::is_move_constructible_v<FStream>);
  EXPECT_TRUE(std::is_move_assignable_v<FStream>);
}

TEST(FStreamTest, OpenFileIsOk) {
  TempFile file;
  ASSERT_OK(FStream::Open(file.GetPath(), std::ios::out));
}

TEST(FStreamTest, OpenNonExistingFileReturnsError) {
  auto status = FStream::Open("non-existing-file", std::ios::in);
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInternal,
                               StartsWith("Failed to open file")));
}

TEST(FStreamTest, TestFileIsOpen) {
  TempFile file;
  ASSERT_OK_AND_ASSIGN(
      auto fs, FStream::Open(file.GetPath(), std::ios::binary | std::ios::out));
  EXPECT_TRUE(fs.IsOpen());

  // close the file
  ASSERT_OK(fs.Close());
  EXPECT_FALSE(fs.IsOpen());
}

TEST(FStreamTest, WriteIntoFileIsOk) {
  TempFile file;
  ASSERT_OK_AND_ASSIGN(
      auto fs, FStream::Open(file.GetPath(), std::ios::binary | std::ios::out));
  auto buffer = std::array{1, 2, 3, 4, 5};
  ASSERT_OK(fs.Write<int>(buffer));
}

TEST(FStreamTest, WriteIntoClosedFileReturnsError) {
  TempFile file;
  ASSERT_OK_AND_ASSIGN(
      auto fs, FStream::Open(file.GetPath(), std::ios::binary | std::ios::out));
  auto buffer = std::array{1, 2, 3, 4, 5};
  ASSERT_OK(fs.Close());
  auto status = fs.Write<int>(buffer);
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInternal,
                               StartsWith("Failed to write into file")));
}

TEST(FStreamTest, ReadingAndWritingAtPositionIsOk) {
  TempFile file;
  ASSERT_OK_AND_ASSIGN(
      auto fs, FStream::Open(file.GetPath(),
                             std::ios::binary | std::ios::in | std::ios::out));

  // position should be 0 because file is empty
  ASSERT_OK(fs.Seekg(0, std::ios::beg));
  ASSERT_OK_AND_ASSIGN(auto pos, fs.Tellg());
  EXPECT_EQ(pos, 0);

  // write 5 bytes
  std::array<char, 5> buffer = {'a', 'b', 'c', 'd', 'e'};
  ASSERT_OK(fs.Seekp(0, std::ios::beg));
  ASSERT_OK(fs.Write<char>(buffer));

  // position should be 5
  ASSERT_OK(fs.Seekg(0, std::ios::end));
  ASSERT_OK_AND_ASSIGN(pos, fs.Tellg());
  EXPECT_EQ(pos, 5);

  // seek to position 10 to write 5 more bytes
  ASSERT_OK(fs.Seekp(10, std::ios::beg));
  ASSERT_OK_AND_ASSIGN(pos, fs.Tellp());
  EXPECT_EQ(pos, 10);
  ASSERT_OK(fs.Write<char>(buffer));

  // position should be 15
  ASSERT_OK(fs.Seekg(0, std::ios::end));
  ASSERT_OK_AND_ASSIGN(pos, fs.Tellg());
  EXPECT_EQ(pos, 15);
}

TEST(FStreamTest, ReadingPositionFromClosedFileReturnsError) {
  TempFile file;
  ASSERT_OK_AND_ASSIGN(
      auto fs, FStream::Open(file.GetPath(),
                             std::ios::binary | std::ios::out | std::ios::in));
  ASSERT_OK(fs.Close());

  auto status = fs.Tellg();
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInternal,
                               StartsWith("Failed to get position")));

  status = fs.Seekg(0, std::ios::beg);
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInternal,
                               StartsWith("Failed to seek")));

  status = fs.Seekp(0, std::ios::beg);
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInternal,
                               StartsWith("Failed to seek")));

  status = fs.Tellp();
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInternal,
                               StartsWith("Failed to get position")));
}

TEST(FStreamTest, ReadFromFileIsOk) {
  TempFile file;
  ASSERT_OK_AND_ASSIGN(
      auto fs, FStream::Open(file.GetPath(),
                             std::ios::binary | std::ios::out | std::ios::in));
  auto buffer = std::array{1, 2, 3, 4, 5};
  ASSERT_OK(fs.Write<int>(buffer));
  ASSERT_OK(fs.Flush());

  auto read_buffer = std::array<int, 5>{};
  ASSERT_OK(fs.Seekg(0, std::ios::beg));
  ASSERT_OK(fs.Read<int>(read_buffer));
  EXPECT_THAT(read_buffer, ElementsAre(1, 2, 3, 4, 5));
}

TEST(FStreamTest, ReadFromClosedFileReturnsError) {
  TempFile file;
  ASSERT_OK_AND_ASSIGN(
      auto fs, FStream::Open(file.GetPath(),
                             std::ios::binary | std::ios::out | std::ios::in));
  auto buffer = std::array{1, 2, 3, 4, 5};
  ASSERT_OK(fs.Write<int>(buffer));
  ASSERT_OK(fs.Flush());
  ASSERT_OK(fs.Close());

  auto read_buffer = std::array<int, 5>{};
  auto status = fs.Read<int>(read_buffer);
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInternal,
                               StartsWith("Failed to read from file")));
}

TEST(FStreamTest, ReadFromFileUntilEofIsOk) {
  TempFile file;
  ASSERT_OK_AND_ASSIGN(
      auto fs, FStream::Open(file.GetPath(),
                             std::ios::binary | std::ios::out | std::ios::in));
  auto buffer = std::array{1, 2, 3, 4, 5};
  ASSERT_OK(fs.Write<int>(buffer));
  ASSERT_OK(fs.Flush());

  auto read_buffer = std::array<int, 6>{};
  ASSERT_OK(fs.Seekg(0, std::ios::beg));
  ASSERT_OK_AND_ASSIGN(auto count, fs.ReadUntilEof<int>(read_buffer));
  EXPECT_EQ(count, buffer.size());
  EXPECT_THAT(read_buffer, ElementsAre(1, 2, 3, 4, 5, 0));
}

TEST(FStreamTest, ReadFromClosedFileUntilEofReturnsError) {
  TempFile file;
  ASSERT_OK_AND_ASSIGN(
      auto fs, FStream::Open(file.GetPath(),
                             std::ios::binary | std::ios::out | std::ios::in));
  auto buffer = std::array{1, 2, 3, 4, 5};
  ASSERT_OK(fs.Write<int>(buffer));
  ASSERT_OK(fs.Flush());
  ASSERT_OK(fs.Close());

  auto read_buffer = std::array<int, 6>{};
  auto status = fs.ReadUntilEof<int>(read_buffer);
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInternal,
                               StartsWith("Failed to read from file")));
}

}  // namespace
}  // namespace carmen
