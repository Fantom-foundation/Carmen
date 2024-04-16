/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3.
 */

#include "backend/common/leveldb/leveldb.h"

#include "common/file_util.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend {
namespace {

using ::testing::_;
using ::testing::ElementsAreArray;
using ::testing::IsOk;
using ::testing::IsOkAndHolds;
using ::testing::Not;
using ::testing::StatusIs;
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
  ASSERT_OK_AND_ASSIGN(auto db, LevelDb::Open(dir.GetPath()));
  ASSERT_OK(db.Add({key, value}));
  EXPECT_THAT(db.Get(key), IsOkAndHolds(StrEq(value)));
}

TEST(LevelDb, TestAddBatchAndGet) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto db, LevelDb::Open(dir.GetPath()));
  std::string key1("key1");
  std::string key2("key2");
  std::string value1("value1");
  std::string value2("value2");
  auto input = std::array{LDBEntry{key1, value1}, LDBEntry{key2, value2}};
  ASSERT_OK(db.AddBatch(input));
  EXPECT_THAT(db.Get(key1), IsOkAndHolds(StrEq(value1)));
  EXPECT_THAT(db.Get(key2), IsOkAndHolds(StrEq(value2)));
}

TEST(LevelDb, TestAddLevelDbBatchAndGet) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto db, LevelDb::Open(dir.GetPath()));
  LevelDbWriteBatch batch;
  // placed in an extra scope to be destructed before applying the batch
  {
    std::string key1("key1");
    std::string key2("key2");
    std::string value1("value1");
    std::string value2("value2");
    batch.Put(key1, value1);
    batch.Put(key2, value2);
  }
  ASSERT_OK(db.Add(std::move(batch)));
  EXPECT_THAT(db.Get(std::string("key1")), IsOkAndHolds(StrEq("value1")));
  EXPECT_THAT(db.Get(std::string("key2")), IsOkAndHolds(StrEq("value2")));
}

TEST(LevelDb, DeleteRemovesKey) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto db, LevelDb::Open(dir.GetPath()));
  std::string key1("key1");
  std::string value1("value1");
  EXPECT_THAT(db.Get(key1), StatusIs(absl::StatusCode::kNotFound, _));
  ASSERT_OK(db.Add({key1, value1}));
  EXPECT_THAT(db.Get(key1), IsOkAndHolds(StrEq(value1)));
  EXPECT_OK(db.Delete(key1));
  EXPECT_THAT(db.Get(key1), StatusIs(absl::StatusCode::kNotFound, _));
}

TEST(LevelDb, BeginIteratorPointsToEndInEmptyDB) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto db, LevelDb::Open(dir.GetPath()));
  ASSERT_OK_AND_ASSIGN(auto iter, db.Begin());
  EXPECT_TRUE(iter.IsEnd());
}

TEST(LevelDb, CanIterateThroughKeysForward) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto db, LevelDb::Open(dir.GetPath()));
  EXPECT_OK(db.Add({"key1", "value1"}));
  EXPECT_OK(db.Add({"key3", "value3"}));
  EXPECT_OK(db.Add({"key2", "value2"}));

  ASSERT_OK_AND_ASSIGN(auto iter, db.Begin());
  EXPECT_OK(iter.Status());
  EXPECT_THAT(iter.Key(), ElementsAreArray("key1"));
  EXPECT_THAT(iter.Value(), ElementsAreArray("value1"));

  EXPECT_OK(iter.Next());
  EXPECT_THAT(iter.Key(), ElementsAreArray("key2"));
  EXPECT_THAT(iter.Value(), ElementsAreArray("value2"));

  EXPECT_OK(iter.Next());
  EXPECT_THAT(iter.Key(), ElementsAreArray("key3"));
  EXPECT_THAT(iter.Value(), ElementsAreArray("value3"));

  EXPECT_OK(iter.Next());
  EXPECT_TRUE(iter.IsEnd());
}

TEST(LevelDb, CanIterateThroughKeysBackward) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto db, LevelDb::Open(dir.GetPath()));
  EXPECT_OK(db.Add({"key1", "value1"}));
  EXPECT_OK(db.Add({"key3", "value3"}));
  EXPECT_OK(db.Add({"key2", "value2"}));

  ASSERT_OK_AND_ASSIGN(auto iter, db.End());
  EXPECT_OK(iter.Prev());
  EXPECT_THAT(iter.Key(), ElementsAreArray("key3"));
  EXPECT_THAT(iter.Value(), ElementsAreArray("value3"));

  EXPECT_OK(iter.Prev());
  EXPECT_THAT(iter.Key(), ElementsAreArray("key2"));
  EXPECT_THAT(iter.Value(), ElementsAreArray("value2"));

  EXPECT_OK(iter.Prev());
  EXPECT_THAT(iter.Key(), ElementsAreArray("key1"));
  EXPECT_THAT(iter.Value(), ElementsAreArray("value1"));

  EXPECT_OK(iter.Prev());
  EXPECT_TRUE(iter.IsBegin());
}

TEST(LevelDb, LowerBoundFindsKeyAndCanNavigate) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto db, LevelDb::Open(dir.GetPath()));
  EXPECT_OK(db.Add({"key1", "value1"}));
  EXPECT_OK(db.Add({"key3", "value3"}));
  EXPECT_OK(db.Add({"key2", "value2"}));

  ASSERT_OK_AND_ASSIGN(auto iter, db.GetLowerBound("key2"));
  EXPECT_TRUE(iter.Valid());
  EXPECT_THAT(iter.Key(), ElementsAreArray("key2"));
  EXPECT_THAT(iter.Value(), ElementsAreArray("value2"));

  EXPECT_OK(iter.Prev());
  EXPECT_THAT(iter.Key(), ElementsAreArray("key1"));
  EXPECT_THAT(iter.Value(), ElementsAreArray("value1"));

  EXPECT_OK(iter.Next());
  EXPECT_OK(iter.Next());
  EXPECT_THAT(iter.Key(), ElementsAreArray("key3"));
  EXPECT_THAT(iter.Value(), ElementsAreArray("value3"));
}

TEST(LevelDb, LowerBoundFindsNextHigherValueIfKeyIsMissing) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto db, LevelDb::Open(dir.GetPath()));
  EXPECT_OK(db.Add({"key1", "value1"}));
  EXPECT_OK(db.Add({"key3", "value3"}));
  EXPECT_OK(db.Add({"key3", "value3"}));

  ASSERT_OK_AND_ASSIGN(auto iter, db.GetLowerBound("key2"));
  EXPECT_TRUE(iter.Valid());
  EXPECT_THAT(iter.Key(), ElementsAreArray("key3"));
  EXPECT_THAT(iter.Value(), ElementsAreArray("value3"));
}

}  // namespace
}  // namespace carmen::backend
