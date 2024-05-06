// Copyright (c) 2024 Fantom Foundation
// 
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
// 
// Change Date: 2028-4-16
// 
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

#include "backend/common/sqlite/sqlite.h"

#include <array>
#include <cstdint>

#include "common/file_util.h"
#include "common/memory_usage.h"
#include "common/status_test_util.h"
#include "gtest/gtest.h"

// This test demonstrates the usage of SQLite.

namespace carmen::backend {
namespace {

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::Gt;
using ::testing::HasSubstr;
using ::testing::IsOkAndHolds;
using ::testing::Pair;
using ::testing::StatusIs;

TEST(Sqlite, TypeProperties) {
  EXPECT_FALSE(std::is_default_constructible_v<Sqlite>);
  EXPECT_TRUE(std::is_move_constructible_v<Sqlite>);
  EXPECT_TRUE(std::is_copy_constructible_v<Sqlite>);
  EXPECT_TRUE(std::is_move_assignable_v<Sqlite>);
  EXPECT_TRUE(std::is_copy_assignable_v<Sqlite>);
  EXPECT_TRUE(std::is_destructible_v<Sqlite>);
}

TEST(Sqlite, OpenClose) {
  TempFile file;
  ASSERT_OK_AND_ASSIGN(auto db, Sqlite::Open(file));
  EXPECT_OK(db.Close());
}

TEST(Sqlite, OpeningAFileCreatesTheFile) {
  TempFile file;
  EXPECT_TRUE(std::filesystem::exists(file));
  std::filesystem::remove(file);
  EXPECT_FALSE(std::filesystem::exists(file));
  ASSERT_OK_AND_ASSIGN(auto db, Sqlite::Open(file));
  EXPECT_TRUE(std::filesystem::exists(file));
  EXPECT_OK(db.Close());
  EXPECT_TRUE(std::filesystem::exists(file));
}

TEST(Sqlite, DatabaseCanBeOpenedMultipleTimes) {
  TempFile file;
  ASSERT_OK_AND_ASSIGN(auto db1, Sqlite::Open(file));
  ASSERT_OK_AND_ASSIGN(auto db2, Sqlite::Open(file));
}

TEST(Sqlite, RunCommands) {
  TempFile file;
  ASSERT_OK_AND_ASSIGN(auto db, Sqlite::Open(file));

  EXPECT_OK(db.Run("CREATE TABLE test (id INTEGER, txt TEXT);"));

  EXPECT_THAT(db.Run("something that is not a command"),
              StatusIs(_, HasSubstr("syntax error")));

  EXPECT_OK(db.Run("DROP TABLE test"));

  EXPECT_THAT(db.Run("DROP TABLE other"),
              StatusIs(_, HasSubstr("no such table")));

  EXPECT_OK(db.Close());
}

TEST(SqlStatement, TypeProperties) {
  EXPECT_FALSE(std::is_default_constructible_v<SqlStatement>);
  EXPECT_TRUE(std::is_move_constructible_v<SqlStatement>);
  EXPECT_FALSE(std::is_copy_constructible_v<SqlStatement>);
  EXPECT_FALSE(std::is_move_assignable_v<SqlStatement>);
  EXPECT_FALSE(std::is_copy_assignable_v<SqlStatement>);
  EXPECT_TRUE(std::is_destructible_v<SqlStatement>);
}

absl::Status CreateTestTable(
    Sqlite& db, std::vector<std::pair<int, std::string>> data = {}) {
  RETURN_IF_ERROR(db.Run("CREATE TABLE test (id INTEGER, text TEXT);"));
  ASSIGN_OR_RETURN(auto stmt,
                   db.Prepare("INSERT INTO test (id, text) VALUES (?,?)"));

  for (auto [id, text] : data) {
    RETURN_IF_ERROR(stmt.Run(id, text));
  }
  return absl::OkStatus();
}

absl::StatusOr<std::vector<std::pair<int, std::string>>> RunAndGetData(
    SqlStatement& query) {
  std::vector<std::pair<int, std::string>> data;
  RETURN_IF_ERROR(query.Execute([&](const SqlRow& row) {
    EXPECT_EQ(row.GetNumberOfColumns(), 2);
    int id = row.GetInt(0);
    std::string text(row.GetString(1));
    data.push_back({id, std::move(text)});
  }));
  return data;
}

TEST(SqlStatement, RunPreparedStatement) {
  TempFile file;
  ASSERT_OK_AND_ASSIGN(auto db, Sqlite::Open(file));
  EXPECT_OK(CreateTestTable(db, {{12, "hello"}, {14, "world"}}));
}

TEST(SqlStatement, RunPreparedQuery) {
  TempFile file;
  ASSERT_OK_AND_ASSIGN(auto db, Sqlite::Open(file));
  EXPECT_OK(CreateTestTable(db, {{12, "hello"}, {14, "world"}}));

  // Test whether data can be retrieved.
  ASSERT_OK_AND_ASSIGN(auto query,
                       db.Prepare("SELECT id, text FROM test ORDER BY id"));

  EXPECT_THAT(RunAndGetData(query),
              IsOkAndHolds(ElementsAre(Pair(12, "hello"), Pair(14, "world"))));
}

TEST(SqlStatement, RunParameterizedStatement) {
  TempFile file;
  ASSERT_OK_AND_ASSIGN(auto db, Sqlite::Open(file));
  EXPECT_OK(CreateTestTable(db, {}));

  // Test whether a parameterized statement can be directly issued.
  EXPECT_OK(db.Run("INSERT INTO test(id,text) VALUES (?,?)", 12, "hello"));
  EXPECT_OK(db.Run("INSERT INTO test(id,text) VALUES (?,?)", 14, "world"));

  ASSERT_OK_AND_ASSIGN(auto query,
                       db.Prepare("SELECT id, text FROM test ORDER BY id"));

  EXPECT_THAT(RunAndGetData(query),
              IsOkAndHolds(ElementsAre(Pair(12, "hello"), Pair(14, "world"))));
}

TEST(SqlStatement, RunParameterizedQuery) {
  TempFile file;
  ASSERT_OK_AND_ASSIGN(auto db, Sqlite::Open(file));
  EXPECT_OK(CreateTestTable(db, {{12, "hello"}, {14, "world"}}));

  // Test whether a parameterized query can be directly issued.
  {
    std::vector<std::pair<int, std::string>> data;
    ASSERT_OK_AND_ASSIGN(
        auto result,
        db.Query("SELECT id, text FROM test WHERE id > ? ORDER BY id", 10));

    EXPECT_OK(result.Consume([&](const SqlRow& row) {
      data.emplace_back(row.GetInt(0), row.GetString(1));
    }));

    EXPECT_THAT(data, ElementsAre(Pair(12, "hello"), Pair(14, "world")));
  }
  {
    std::vector<std::pair<int, std::string>> data;
    ASSERT_OK_AND_ASSIGN(auto result,
                         db.Query("SELECT id, text FROM test WHERE id > ? AND "
                                  "id < ? AND text = ? ORDER BY id",
                                  10, 20, "world"));

    EXPECT_OK(result.Consume([&](const SqlRow& row) {
      data.emplace_back(row.GetInt(0), row.GetString(1));
    }));

    EXPECT_THAT(data, ElementsAre(Pair(14, "world")));
  }
}

TEST(SqlStatement, ReusePreparedQuery) {
  TempFile file;
  ASSERT_OK_AND_ASSIGN(auto db, Sqlite::Open(file));
  EXPECT_OK(CreateTestTable(db, {{12, "hello"}, {14, "world"}}));

  // Test whether data can be retrieved.
  ASSERT_OK_AND_ASSIGN(auto query,
                       db.Prepare("SELECT id, text FROM test WHERE id == ?"));

  EXPECT_OK(query.Bind(1, 12));
  EXPECT_THAT(RunAndGetData(query),
              IsOkAndHolds(ElementsAre(Pair(12, "hello"))));

  EXPECT_OK(query.Reset());
  EXPECT_OK(query.Bind(1, 14));
  EXPECT_THAT(RunAndGetData(query),
              IsOkAndHolds(ElementsAre(Pair(14, "world"))));

  EXPECT_OK(query.Reset());
  EXPECT_OK(query.Bind(1, 16));
  EXPECT_THAT(RunAndGetData(query), IsOkAndHolds(ElementsAre()));
}

TEST(SqlStatement, DatabaseCanBeClosedAndReOpened) {
  TempFile file;

  {  // Create a test database and close it.
    ASSERT_OK_AND_ASSIGN(auto db, Sqlite::Open(file));
    EXPECT_OK(CreateTestTable(db, {{12, "hello"}, {14, "world"}}));
    EXPECT_OK(db.Close());
  }

  {  // Re-open it and see whether the data is there.
    ASSERT_OK_AND_ASSIGN(auto db, Sqlite::Open(file));
    ASSERT_OK_AND_ASSIGN(auto query,
                         db.Prepare("SELECT id, text FROM test ORDER BY id"));

    EXPECT_THAT(
        RunAndGetData(query),
        IsOkAndHolds(ElementsAre(Pair(12, "hello"), Pair(14, "world"))));
  }
}

TEST(SqlStatement, DatabaseSupportsInt64) {
  using Value = std::int64_t;

  Value a = 1;
  Value b = 2;
  Value c = -1;

  TempFile file;
  ASSERT_OK_AND_ASSIGN(auto db, Sqlite::Open(file));
  ASSERT_OK(db.Run("CREATE TABLE test (key INTEGER)"));

  // Insert elements out-of-order.
  ASSERT_OK_AND_ASSIGN(auto insert,
                       db.Prepare("INSERT INTO test(key) VALUES (?)"));
  EXPECT_OK(insert.Bind(1, a));
  EXPECT_OK(insert.Run());

  EXPECT_OK(insert.Reset());
  EXPECT_OK(insert.Bind(1, c));
  EXPECT_OK(insert.Run());

  EXPECT_OK(insert.Reset());
  EXPECT_OK(insert.Bind(1, b));
  EXPECT_OK(insert.Run());

  // Query elements in order.
  ASSERT_OK_AND_ASSIGN(auto query,
                       db.Prepare("SELECT key FROM test ORDER BY key"));
  std::vector<Value> data;
  EXPECT_OK(query.Execute(
      [&](const SqlRow& row) { data.push_back(row.GetInt64(0)); }));
  EXPECT_THAT(data, ElementsAre(c, a, b));
}

TEST(SqlStatement, DatabaseSupportsByteArrays) {
  using Value = std::array<std::byte, 32>;

  Value a{std::byte{0x01}};
  Value b{std::byte{0x01}};
  Value c{std::byte{0x01}};

  TempFile file;
  ASSERT_OK_AND_ASSIGN(auto db, Sqlite::Open(file));
  ASSERT_OK(db.Run("CREATE TABLE test (key BLOB)"));

  // Insert elements out-of-order.
  ASSERT_OK_AND_ASSIGN(auto insert,
                       db.Prepare("INSERT INTO test(key) VALUES (?)"));
  EXPECT_OK(insert.Bind(1, a));
  EXPECT_OK(insert.Run());

  EXPECT_OK(insert.Reset());
  EXPECT_OK(insert.Bind(1, c));
  EXPECT_OK(insert.Run());

  EXPECT_OK(insert.Reset());
  EXPECT_OK(insert.Bind(1, b));
  EXPECT_OK(insert.Run());

  // Query elements in order.
  ASSERT_OK_AND_ASSIGN(auto query,
                       db.Prepare("SELECT key FROM test ORDER BY key"));
  std::vector<Value> data;
  EXPECT_OK(query.Execute([&](const SqlRow& row) {
    auto key = row.GetBytes(0);
    ASSERT_EQ(key.size(), 32);
    Value value;
    for (std::size_t i = 0; i < 32; i++) {
      value[i] = key[i];
    }
    data.push_back(value);
  }));
  EXPECT_THAT(data, ElementsAre(a, b, c));
}

TEST(SqlStatement, DatabaseCanProvideMemoryFootPrint) {
  TempFile file;
  ASSERT_OK_AND_ASSIGN(auto db, Sqlite::Open(file));
  EXPECT_THAT(db.GetMemoryFootprint().GetTotal(), Gt(Memory(0)));
}

}  // namespace
}  // namespace carmen::backend
