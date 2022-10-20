#include "backend/index/leveldb/common/ldb_instance.h"

#include "absl/status/statusor.h"
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
  EXPECT_OK(internal::LevelDBInstance::Open(dir.GetPath().string()));
}

TEST(LevelDBInterfaceTest, TestOpenIfMissingFalse) {
  TempDir dir;
  auto db = internal::LevelDBInstance::Open(dir.GetPath().string(), false);
  EXPECT_THAT(db, Not(IsOk()));
}

TEST(LevelDBInterfaceTest, TestAddAndGet) {
  TempDir dir;
  auto db = *internal::LevelDBInstance::Open(dir.GetPath().string());
  ASSERT_OK(db.Add("key", "value"));
  ASSERT_OK_AND_ASSIGN(auto value, db.Get("key"));
  EXPECT_THAT(value, StrEq("value"));
}

TEST(LevelDBInterfaceTest, TestAddBatchAndGet) {
  TempDir dir;
  auto db = *internal::LevelDBInstance::Open(dir.GetPath().string());
  auto input = std::array<std::pair<std::string_view, std::string_view>, 2>{
      std::pair<std::string_view, std::string_view>{"key1", "value1"},
      std::pair<std::string_view, std::string_view>{"key2", "value2"}};
  ASSERT_OK(db.AddBatch(input));
  ASSERT_OK_AND_ASSIGN(auto value1, db.Get("key1"));
  ASSERT_OK_AND_ASSIGN(auto value2, db.Get("key2"));
  EXPECT_THAT(value1, StrEq("value1"));
  EXPECT_THAT(value2, StrEq("value2"));
}

}  // namespace
}  // namespace carmen::backend::index
