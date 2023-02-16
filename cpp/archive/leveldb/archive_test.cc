#include "archive/leveldb/archive.h"

#include <type_traits>

#include "archive/archive.h"
#include "common/file_util.h"
#include "common/hash.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::archive::leveldb {
namespace {

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::IsOkAndHolds;
using ::testing::StatusIs;

TEST(LevelDbArchive, TypeProperties) {
  EXPECT_FALSE(std::is_default_constructible_v<LevelDbArchive>);
  EXPECT_FALSE(std::is_copy_constructible_v<LevelDbArchive>);
  EXPECT_TRUE(std::is_move_constructible_v<LevelDbArchive>);
  EXPECT_FALSE(std::is_copy_assignable_v<LevelDbArchive>);
  EXPECT_TRUE(std::is_move_assignable_v<LevelDbArchive>);
  EXPECT_TRUE(std::is_destructible_v<LevelDbArchive>);

  EXPECT_TRUE(Archive<LevelDbArchive>);
}

TEST(LevelDbArchive, OpenAndClosingEmptyDbWorks) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, LevelDbArchive::Open(dir));
  EXPECT_OK(archive.Close());
}

}  // namespace
}  // namespace carmen::archive::leveldb
