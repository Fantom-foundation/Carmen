#include "backend/common/leveldb/leveldb.h"

#include "common/file_util.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend {
namespace {

using ::testing::IsOk;
using ::testing::Not;
using ::testing::StrEq;

TEST(LevelDb, TestOpen) {
  TempDir dir;
  EXPECT_OK(LevelDb::Open(dir.GetPath()));
}

TEST(LevelDb, TestOpenIfMissingFalse) {
  TempDir dir;
  auto db = LevelDb::Open(dir.GetPath(), false);
  EXPECT_THAT(db, Not(IsOk()));
}

TEST(LevelDb, TestAddAndGet) {
  TempDir dir;
  std::string key("key");
  std::string value("value");
  auto db = *LevelDb::Open(dir.GetPath());
  ASSERT_OK(db.Add({key, value}));
  ASSERT_OK_AND_ASSIGN(auto result, db.Get(key));
  EXPECT_THAT(value, StrEq(result));
}

TEST(LevelDb, TestAddBatchAndGet) {
  TempDir dir;
  auto db = *LevelDb::Open(dir.GetPath());
  std::string key1("key1");
  std::string key2("key2");
  std::string value1("value1");
  std::string value2("value2");
  auto input = std::array{LDBEntry{key1, value1}, LDBEntry{key2, value2}};
  ASSERT_OK(db.AddBatch(input));
  ASSERT_OK_AND_ASSIGN(auto result1, db.Get(key1));
  ASSERT_OK_AND_ASSIGN(auto result2, db.Get(key2));
  EXPECT_THAT(value1, StrEq(result1));
  EXPECT_THAT(value2, StrEq(result2));
}
}  // namespace
}  // namespace carmen::backend
