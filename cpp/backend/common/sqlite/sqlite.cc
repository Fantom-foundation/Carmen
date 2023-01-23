#include "backend/common/sqlite/sqlite.h"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string_view>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/status_util.h"
#include "sqlite3.h"

namespace carmen::backend {

// For reference, see the SQLite C documentation:
// https://www.sqlite.org/cintro.html

namespace internal {

class SqliteDb {
 public:
  SqliteDb(sqlite3* db) : db_(db) {}
  SqliteDb(const SqliteDb&) = delete;
  SqliteDb(SqliteDb&&) = delete;

  ~SqliteDb() {
    if (auto status = Close(); !status.ok()) {
      std::cout << "WARNING: Failed to close Sqlite DB, " << status
                << std::endl;
    }
  }

  absl::Status Run(std::string_view statement) {
    // See https://www.sqlite.org/c3ref/exec.html
    assert(db_ != nullptr);
    char* msg;
    int res = sqlite3_exec(db_, statement.data(), nullptr, nullptr, &msg);
    if (msg != nullptr) {
      auto status = absl::InvalidArgumentError(msg);
      sqlite3_free(msg);
      return status;
    }
    return HandleError(res);
  }

  absl::StatusOr<sqlite3_stmt*> Prepare(absl::string_view query) {
    assert(db_ != nullptr);
    sqlite3_stmt* stmt;
    RETURN_IF_ERROR(HandleError(
        sqlite3_prepare_v2(db_, query.data(), query.size(), &stmt, nullptr)));
    return stmt;
  }

  absl::Status HandleError(int error) {
    assert(db_ != nullptr);
    if (error == SQLITE_OK) {
      return absl::OkStatus();
    }
    return absl::InternalError(sqlite3_errmsg(db_));
  }

  absl::Status Close() {
    if (db_ != nullptr) {
      RETURN_IF_ERROR(HandleError(sqlite3_close(db_)));
      db_ = nullptr;
    }
    return absl::OkStatus();
  }

 private:
  sqlite3* db_;
};

}  // namespace internal

absl::StatusOr<Sqlite> Sqlite::Open(std::filesystem::path db_file) {
  sqlite3* db = nullptr;
  auto res = sqlite3_open(db_file.c_str(), &db);
  if (db == nullptr) {
    return absl::InternalError(
        "Unable to allocate memory for Sqlite instance.");
  }
  if (res != SQLITE_OK) {
    std::string err_msg(sqlite3_errmsg(db));
    sqlite3_free(db);
    return absl::InternalError(
        absl::StrCat("Unable to create Sqlite DB: ", err_msg));
  }
  return Sqlite(std::make_shared<internal::SqliteDb>(db));
}

absl::Status Sqlite::Run(std::string_view statement) {
  if (db_ == nullptr) {
    return absl::FailedPreconditionError("DB not open");
  }
  return db_->Run(statement);
}

absl::StatusOr<SqlStatement> Sqlite::Prepare(absl::string_view statement) {
  if (db_ == nullptr) {
    return absl::FailedPreconditionError("DB not open");
  }
  ASSIGN_OR_RETURN(sqlite3_stmt * stmt, db_->Prepare(statement));
  return SqlStatement(db_, stmt);
}

absl::Status Sqlite::Close() {
  if (db_ != nullptr) {
    RETURN_IF_ERROR(db_->Close());
    db_ = nullptr;
  }
  return absl::OkStatus();
}

SqlStatement::SqlStatement(SqlStatement&& other)
    : db_(std::move(other.db_)), stmt_(other.stmt_) {
  other.stmt_ = nullptr;
}

SqlStatement::~SqlStatement() {
  sqlite3_finalize(stmt_);
  stmt_ = nullptr;
}

absl::Status SqlStatement::Bind(int index, int value) {
  RETURN_IF_ERROR(CheckState());
  // See https://www.sqlite.org/c3ref/bind_blob.html
  return db_->HandleError(sqlite3_bind_int(stmt_, index + 1, value));
}

absl::Status SqlStatement::Bind(int index, std::int64_t value) {
  RETURN_IF_ERROR(CheckState());
  // See https://www.sqlite.org/c3ref/bind_blob.html
  return db_->HandleError(sqlite3_bind_int64(stmt_, index + 1, value));
}

absl::Status SqlStatement::Bind(int index, absl::string_view str) {
  RETURN_IF_ERROR(CheckState());
  // See https://www.sqlite.org/c3ref/bind_blob.html
  return db_->HandleError(sqlite3_bind_text(stmt_, index + 1, str.data(),
                                            str.size(), SQLITE_TRANSIENT));
}

absl::Status SqlStatement::Bind(int index, std::span<const std::byte> bytes) {
  RETURN_IF_ERROR(CheckState());
  // See https://www.sqlite.org/c3ref/bind_blob.html
  return db_->HandleError(sqlite3_bind_text(
      stmt_, index + 1, reinterpret_cast<const char*>(bytes.data()),
      bytes.size(), SQLITE_TRANSIENT));
}

absl::Status SqlStatement::Reset() {
  RETURN_IF_ERROR(CheckState());
  return db_->HandleError(sqlite3_reset(stmt_));
}

absl::Status SqlStatement::Run() {
  RETURN_IF_ERROR(CheckState());
  int result = sqlite3_step(stmt_);
  if (result == SQLITE_DONE) {
    return absl::OkStatus();
  }
  return db_->HandleError(result);
}

absl::Status SqlStatement::Run(
    absl::FunctionRef<void(const SqlRow& row)> consumer) {
  RETURN_IF_ERROR(CheckState());
  // See  https://www.sqlite.org/c3ref/step.html
  int result = sqlite3_step(stmt_);
  while (result == SQLITE_ROW) {
    consumer(SqlRow(stmt_));
    result = sqlite3_step(stmt_);
  }
  if (result != SQLITE_DONE) {
    return db_->HandleError(result);
  }
  return absl::OkStatus();
}

absl::Status SqlStatement::CheckState() {
  if (db_ == nullptr || stmt_ == nullptr) {
    return absl::FailedPreconditionError("Statement not ready");
  }
  return absl::OkStatus();
}

int SqlRow::GetNumberOfColumns() const {
  // See https://www.sqlite.org/c3ref/column_count.html
  return sqlite3_column_count(stmt_);
}

int SqlRow::GetInt(int column) const {
  // See https://www.sqlite.org/c3ref/column_blob.html
  return sqlite3_column_int(stmt_, column);
}

std::int64_t SqlRow::GetInt64(int column) const {
  // See https://www.sqlite.org/c3ref/column_blob.html
  return sqlite3_column_int64(stmt_, column);
}

std::string_view SqlRow::GetString(int column) const {
  // See https://www.sqlite.org/c3ref/column_blob.html
  const unsigned char* data = sqlite3_column_text(stmt_, column);
  int size = sqlite3_column_bytes(stmt_, column);
  return std::string_view(reinterpret_cast<const char*>(data), size);
}

std::span<const std::byte> SqlRow::GetBytes(int column) const {
  // See https://www.sqlite.org/c3ref/column_blob.html
  const unsigned char* data = sqlite3_column_text(stmt_, column);
  int size = sqlite3_column_bytes(stmt_, column);
  return std::span(reinterpret_cast<const std::byte*>(data), size);
}

}  // namespace carmen::backend
