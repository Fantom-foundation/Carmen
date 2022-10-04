#include "common/file_util.h"

#include <filesystem>
#include <fstream>
#include <type_traits>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen {
namespace {

using ::testing::StartsWith;

TEST(TempFile, TypeTraits) {
  EXPECT_TRUE(std::is_default_constructible_v<TempFile>);
  EXPECT_FALSE(std::is_copy_constructible_v<TempFile>);
  EXPECT_FALSE(std::is_move_constructible_v<TempFile>);
  EXPECT_FALSE(std::is_copy_assignable_v<TempFile>);
  EXPECT_FALSE(std::is_move_assignable_v<TempFile>);
}

TEST(TempFile, MultipleTempFilesHaveDifferentPaths) {
  TempFile a;
  TempFile b;
  EXPECT_NE(a.GetPath(), b.GetPath());
}

TEST(TempFile, TempFileUsedDesiredPrefix) {
  TempFile a("my_prefix");
  EXPECT_THAT(a.GetPath().filename(), StartsWith("my_prefix"));
}

TEST(TempFile, TheTemporaryFileExistsAfterCreation) {
  TempFile a;
  EXPECT_TRUE(std::filesystem::exists(a.GetPath()));
}

TEST(TempFile, TheTemporaryFileIsAutomaticallyRemoved) {
  std::filesystem::path path;
  {
    TempFile a;
    path = a.GetPath();
    EXPECT_TRUE(std::filesystem::exists(path));
  }
  EXPECT_FALSE(std::filesystem::exists(path));
}

TEST(TempFile, TheTemporaryFileCanBeRemovedManually) {
  TempFile a;
  EXPECT_TRUE(std::filesystem::exists(a.GetPath()));
  std::filesystem::remove(a.GetPath());
  EXPECT_FALSE(std::filesystem::exists(a.GetPath()));
}

TEST(TempFile, TheTemporaryFileCanBeRemovedAndRecreatedManually) {
  TempFile a;
  EXPECT_TRUE(std::filesystem::exists(a.GetPath()));
  std::filesystem::remove(a.GetPath());
  EXPECT_FALSE(std::filesystem::exists(a.GetPath()));
  { std::fstream out(a.GetPath(), std::ios::out); }
  EXPECT_TRUE(std::filesystem::exists(a.GetPath()));
}

}  // namespace
}  // namespace carmen
