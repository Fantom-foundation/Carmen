// Copyright (c) 2024 Fantom Foundation
// 
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
// 
// Change Date: 2028-4-16
// 
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string_view>

#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/memory_usage.h"
#include "common/status_util.h"
#include "common/type.h"
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
class SqlQueryResult;
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

  // Runs a given parameterized statement on the database using the provided
  // arguments. Results are ignored. This is a convenience version preparing the
  // statement internally, running it once, and discarding it afterwards. For
  // frequently used queries the prepared statements should be reused.
  template <typename... Args>
  absl::Status Run(std::string_view statement, const Args&... args);

  // Issues a parameterized query and returns an iterator on the results.
  template <typename... Args>
  absl::StatusOr<SqlQueryResult> Query(std::string_view statement,
                                       const Args&... args);

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
  // form of ? or ?NNN where NNN is a number > 0. These parameters need to be
  // bound before each execution. The following overloads allow to bind those
  // parameters to values. Parameters are addressed through their index, where
  // the first parameter in the query has index 1.

  absl::Status Bind(int index, int value);

  absl::Status Bind(int index, std::uint32_t value);

  absl::Status Bind(int index, std::int64_t value);

  absl::Status Bind(int index, absl::string_view str);

  absl::Status Bind(int index, std::span<const std::byte> data);

  // A convenience version of the function above, binding a list of parameters
  // in one go. The first argument is bound to parameter 0, the second to
  // parmeter 1, and so forth.
  template <typename... Args>
  absl::Status BindParameters(const Args&... args);

  // After the parameters are bound (if there are any), the following overloads
  // of Run(..) can be used to execute the actual operation.

  // Runs a statement that does not expect to produce any result rows (e.g.
  // CREATE TABLE).
  absl::Status Run();

  // Convenience method binding arguments and running the query in one go.
  template <typename... Args>
  absl::Status Run(const Args&... args);

  // Runs a statement that produces results and forward each row of the result
  // to the given consumer.
  absl::Status Execute(absl::FunctionRef<void(const SqlRow& row)> consumer);

  // Runs this statement and returns an iterator on its results. Only one
  // iterator can be alive at any time and this statement must outlive the
  // returned iterator. If all results should be consumed, use the Run() methods
  // above for convenience.
  absl::StatusOr<SqlIterator> Open();

  // Binds the provided parameters to this query and opens an iterator. This
  // function is equivalent to calling BindParameters() and Open() in one go.
  template <typename... Args>
  absl::StatusOr<SqlIterator> Open(const Args&... args);

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

  // Retrieves a trivial value from the store in the given value.
  template <Trivial T>
  T Get(int column) const;

 private:
  SqlRow(sqlite3_stmt* stmt) : stmt_(stmt) {}
  sqlite3_stmt* stmt_;
};

// A SQL iterator provides a way to iterate through the results of an SQL query.
class SqlIterator {
 public:
  friend class SqlStatement;
  friend class SqlQueryResult;

  SqlIterator(SqlIterator&&);
  ~SqlIterator();

  // Move to the next element, returning true if there is one, false otherwise.
  absl::StatusOr<bool> Next();

  // True when the end of the results has been reached, false otherwise.
  bool Finished();

  // Retrieves a reference to the current result row.
  SqlRow& operator*();
  SqlRow* operator->();

  // Closes this iterator and resets the underlying query. Close is called
  // implicitly on destruction.
  absl::Status Close();

 private:
  SqlIterator(internal::SqliteDb* db, SqlStatement* stmt,
              sqlite3_stmt* raw_stmt)
      : db_(db), stmt_(stmt), row_(raw_stmt) {}
  internal::SqliteDb* db_;
  SqlStatement* stmt_;
  SqlRow row_;
};

// A SQL query result is a wrapper object providing access to the result of a
// single, stand-alone query that can not be reused.
//
// The main reason for introducing this is to keep the statement alife until the
// result is consumed or discarded. For iterators, the associated statement
// needs to be kept alive independently.
class SqlQueryResult {
 public:
  friend class Sqlite;

  // Produces an iterator over the query result.
  absl::StatusOr<SqlIterator> Iterator();

  // Iterates through the results and passes each row to the consumer.
  absl::Status Consume(absl::FunctionRef<void(const SqlRow& row)> consumer);

 private:
  SqlQueryResult(SqlStatement stmt) : stmt_(std::move(stmt)){};
  SqlStatement stmt_;
};

// ----------------------------------------------------------------------------
//                               Definitions
// ----------------------------------------------------------------------------

template <typename... Args>
absl::Status Sqlite::Run(std::string_view statement, const Args&... args) {
  ASSIGN_OR_RETURN(auto stmt, Prepare(statement));
  RETURN_IF_ERROR(stmt.BindParameters(args...));
  return stmt.Run();
}

template <typename... Args>
absl::StatusOr<SqlQueryResult> Sqlite::Query(std::string_view statement,
                                             const Args&... args) {
  ASSIGN_OR_RETURN(auto stmt, Prepare(statement));
  RETURN_IF_ERROR(stmt.BindParameters(args...));
  return SqlQueryResult(std::move(stmt));
}

namespace internal {

inline absl::Status Bind(SqlStatement&, int) { return absl::OkStatus(); }

template <typename First, typename... Rest>
absl::Status Bind(SqlStatement& stmt, int index, const First& first,
                  const Rest&... rest) {
  RETURN_IF_ERROR(stmt.Bind(index, first));
  return Bind(stmt, index + 1, rest...);
}

}  // namespace internal

template <typename... Args>
absl::Status SqlStatement::BindParameters(const Args&... args) {
  return internal::Bind(*this, 1, args...);
}

template <typename... Args>
absl::Status SqlStatement::Run(const Args&... args) {
  RETURN_IF_ERROR(BindParameters(args...));
  return Run();
}

template <typename... Args>
absl::StatusOr<SqlIterator> SqlStatement::Open(const Args&... args) {
  RETURN_IF_ERROR(BindParameters(args...));
  return Open();
}

template <Trivial T>
T SqlRow::Get(int column) const {
  auto bytes = GetBytes(column);
  T res;
  std::memcpy(&res, bytes.data(), std::min(sizeof(T), bytes.size()));
  return res;
}

}  // namespace carmen::backend
