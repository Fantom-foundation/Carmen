#include "backend/index/leveldb/common/level_db.h"

#include "common/file_util.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

using ::testing::IsOk;
using ::testing::Not;
using ::testing::StrEq;

TEST(LevelDBInterfaceTest, TestOpen) {
  TempDir dir;
  EXPECT_OK(internal::LevelDB::Open(dir.GetPath()));
}

TEST(LevelDBInterfaceTest, TestOpenIfMissingFalse) {
  TempDir dir;
  auto db = internal::LevelDB::Open(dir.GetPath(), false);
  EXPECT_THAT(db, Not(IsOk()));
}

TEST(LevelDBInterfaceTest, TestAddAndGet) {
  TempDir dir;
  auto db = *internal::LevelDB::Open(dir.GetPath());
  ASSERT_OK(db.Add(std::string("key"), std::string("value")));
  ASSERT_OK_AND_ASSIGN(auto result, db.Get(std::string("key")));
  EXPECT_THAT(std::string("value"), StrEq(result));
}

TEST(LevelDBInterfaceTest, TestAddBatchAndGet) {
  TempDir dir;
  auto db = *internal::LevelDB::Open(dir.GetPath());
  auto input =
      std::array<std::pair<std::span<const char>, std::span<const char>>, 2>{
          std::pair<std::span<const char>, std::span<const char>>{
              std::string("key_1"), std::string("value_1")},
          std::pair<std::span<const char>, std::span<const char>>{
              std::string("key_2"), std::string("value_2")}};
  ASSERT_OK(db.AddBatch(input));
  ASSERT_OK_AND_ASSIGN(auto result_1, db.Get(std::string("key_1")));
  ASSERT_OK_AND_ASSIGN(auto result_2, db.Get(std::string("key_2")));
  EXPECT_THAT(std::string("value_1"), StrEq(result_1));
  EXPECT_THAT(std::string("value_2"), StrEq(result_2));
}

}  // namespace
}  // namespace carmen::backend::index
