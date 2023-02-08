#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string_view>

#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/memory_usage.h"
#include "sqlite3.h"

namespace carmen::backend {

// This file provides C++ wrappers for an SQLite DB instance. The wrapper types
// manager the life-cycle of DB objects and adapt the error reporting to the
// absl::Status infrastructure.
//
// There are three elements involved in interacting with SQLite DBs:
//   - Sqlite       ... modeling the DB connection and a factory for statements
//   - SqlStatement ... a SQL statement that can be run one or more times
//   - SqlRow       ... a single row of a query result, to load data from a DB
//
// To get access to a SQLite DB, a Sqlite instance has to be created using
// Sqlite::Open(..). To run statements on the DB, Sqlite::Prepare(..) must be
// used to prepare the statement, and SqlStatement::Run() can be used to run the
// statement one or multiple times. For one-of statements, Sqlite::Run(..) is
// offered as a convenience function, skipping the intermediate SqlStatement
// step.

class Sqlite;
class SqlStatement;
class SqlIterator;
class SqlRow;

namespace internal {
class SqliteDb;
};

// A Sqlite instance is managing the connection to a single DB instance. It
// provides the necessary interface to open an existing DB, run statements and
// queries on it, and closing it.
class Sqlite {
 public:
  // Opens the DB stored in the given file. If the file does not exist, it is
  // created.
  static absl::StatusOr<Sqlite> Open(std::filesystem::path db_file);

  // Closes the connection to the underlying database. Instances should only be
  // destructed after all derived statements have been destructed. However,
  // statements may outliving the Sqlite instance, yet operations on those will
  // fails.
  ~Sqlite() = default;

  // Runs the given statement on the database. It is a shortcut for one-of
  // statements, skipping the statement preparation step.
  absl::Status Run(std::string_view statement);

  // Statements that should be repeated many times should use this preparation
  // call, returning a prepared statement which can be run multiple times. The
  // resulting statement should not (but may) outlive the Sqlite instance. In
  // the latter case, the statement will keep some internal state alive until
  // the last of the statements is destroyed.
  absl::StatusOr<SqlStatement> Prepare(absl::string_view statement);

  // Closes the DB connection. Use this to make sure the DB is properly closed
  // and to handle potential errors. Before closing the DB, all derived
  // statements should be destructed.
  absl::Status Close();

  // Estimates the total memory used by this DB connection.
  MemoryFootprint GetMemoryFootprint() const;

 private:
  Sqlite(std::shared_ptr<internal::SqliteDb> db) : db_(std::move(db)){};

  // The actual DB connection state wich shared ownership between this instance
  // and all derived statements.
  std::shared_ptr<internal::SqliteDb> db_;
};

// A SQL statement can be used to run statements/commands on the SQLite DB.
//
// Statements are created using the Sqlite::Prepare(..) factory function, during
// which syntax issues are checked and internal state for running statements
// multiple times is prepared.
//
// To run a prepared statement, bind potential statement parameters using
// various overloads of the `Bind` functions, and then invoke an overload of the
// `Run` member function. Before re-using a statement, Reset() needs to be
// called to discard previous parameter bindings and query state.
class SqlStatement {
 public:
  friend class Sqlite;

  SqlStatement(SqlStatement&&);

  ~SqlStatement();

  // Resets parameter bindings and prepares the statement for reuse. MAY be
  // called before the first query use and MUST be called before every
  // subsequent reuse.
  absl::Status Reset();

  // Parameter Binding: prepared SQL statements may contain parameters in the
  // form of ?. These parameters need to be bound before each execution. The
  // following overloads allow to bind those parameters to values. Parameters
  // are addressed through their index, where the first parameter in the query
  // has index 0.

  absl::Status Bind(int index, int value);

  absl::Status Bind(int index, std::int64_t value);

  absl::Status Bind(int index, absl::string_view str);

  absl::Status Bind(int index, std::span<const std::byte> data);

  // After the parameters are bound (if there are any), the following overloads
  // of Run(..) can be used to execute the actual operation.

  // Runs a statement that does not expect to produce any result rows (e.g.
  // CREATE TABLE).
  absl::Status Run();

  // Runs a statement that produces results and forward each row of the result
  // to the given consumer.
  absl::Status Run(absl::FunctionRef<void(const SqlRow& row)> consumer);

  // Runs this statement and returns an iterator on its results. Only one
  // iterator can be alife at any time and this statement must outlive the
  // returned iterator. If all results should be consumed, use the Run() methods
  // above for convenience.
  absl::StatusOr<SqlIterator> Open();

 private:
  SqlStatement(std::shared_ptr<internal::SqliteDb> db, sqlite3_stmt* stmt)
      : db_(std::move(db)), stmt_(stmt) {}

  absl::Status CheckState();

  // Shared ownership on the SqliteDB connection. This is used to make sure that
  // the connection is only closed after the last statement is finalized.
  std::shared_ptr<internal::SqliteDb> db_;

  // The SQLite3 internal handle to this statement.
  sqlite3_stmt* stmt_;
};

// A SQL row is a single row of a query result. This class is a simple wrapper
// over Sqlite internal structures and is used to provide convenient access to
// query results.
class SqlRow {
 public:
  friend class SqlIterator;

  // Retrieves the number of columns in this row.
  int GetNumberOfColumns() const;

  // The following functions provide type specific access to column values.
  // Columns are addressed through their number, starting with 0. Since columns
  // are not typed in Sqlite, the same column may be interpreted as different
  // types. To do so, implict conversions are applied. For details, see
  // https://www.sqlite.org/c3ref/column_blob.html.

  // Retrieves the integer interpretation of the value stored in the given
  // column.
  int GetInt(int column) const;

  // Retrieves the 64-bit integer interpretation of the value stored in the
  // given column.
  std::int64_t GetInt64(int column) const;

  // Retrieves the string interpretation of the value stored in the given
  // column. Note: the resulting string_view is only valid until the next
  // GetXXX() call or the end of the life cycle of this SqlRow.
  std::string_view GetString(int column) const;

  // Retrieves the bytes stored in the given column. Note: the resulting span is
  // only valid until the next GetXXX() call or the end of the life cycle of
  // this SqlRow.
  std::span<const std::byte> GetBytes(int column) const;

 private:
  SqlRow(sqlite3_stmt* stmt) : stmt_(stmt) {}
  sqlite3_stmt* stmt_;
};

// A SQL iterator provides a way to iterate through the results of an SQL query.
class SqlIterator {
 public:
  friend class SqlStatement;

  // Move to the next element, returning true if there is one, false otherwise.
  absl::StatusOr<bool> Next();

  // True when the end of the results has been reached, false otherwise.
  bool Finished();

  // Retrieves a reference to the current result row.
  SqlRow& operator*();
  SqlRow* operator->();

 private:
  SqlIterator(internal::SqliteDb* db, sqlite3_stmt* stmt)
      : db_(db), row_(stmt) {}
  internal::SqliteDb* db_;
  SqlRow row_;
};

}  // namespace carmen::backend
