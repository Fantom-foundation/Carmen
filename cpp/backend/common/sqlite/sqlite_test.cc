#include "backend/common/sqlite/sqlite.h"

#include "common/file_util.h"
#include "common/status_test_util.h"
#include "gtest/gtest.h"

// This test demonstrates the usage of SQLite.

namespace carmen::backend {
namespace {

using ::testing::_;
using ::testing::ElementsAre;
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
    RETURN_IF_ERROR(stmt.Reset());
    RETURN_IF_ERROR(stmt.Bind(0, id));
    RETURN_IF_ERROR(stmt.Bind(1, text));
    RETURN_IF_ERROR(stmt.Run());
  }
  return absl::OkStatus();
}

absl::StatusOr<std::vector<std::pair<int, std::string>>> RunAndGetData(
    SqlStatement& query) {
  std::vector<std::pair<int, std::string>> data;
  RETURN_IF_ERROR(query.Run([&](const SqlRow& row) {
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

TEST(SqlStatement, ReusePreparedQuery) {
  TempFile file;
  ASSERT_OK_AND_ASSIGN(auto db, Sqlite::Open(file));
  EXPECT_OK(CreateTestTable(db, {{12, "hello"}, {14, "world"}}));

  // Test whether data can be retrieved.
  ASSERT_OK_AND_ASSIGN(auto query,
                       db.Prepare("SELECT id, text FROM test WHERE id == ?"));

  EXPECT_OK(query.Bind(0, 12));
  EXPECT_THAT(RunAndGetData(query),
              IsOkAndHolds(ElementsAre(Pair(12, "hello"))));

  EXPECT_OK(query.Reset());
  EXPECT_OK(query.Bind(0, 14));
  EXPECT_THAT(RunAndGetData(query),
              IsOkAndHolds(ElementsAre(Pair(14, "world"))));

  EXPECT_OK(query.Reset());
  EXPECT_OK(query.Bind(0, 16));
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

}  // namespace
}  // namespace carmen::backend
