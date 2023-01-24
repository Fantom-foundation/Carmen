#include "state/archive.h"

#include "absl/container/btree_map.h"
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "backend/common/sqlite/sqlite.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen {

using ::carmen::backend::Sqlite;
using ::carmen::backend::SqlRow;
using ::carmen::backend::SqlStatement;

namespace internal {

class Archive {
 public:
  // Opens an archive database stored in the given file.
  static absl::StatusOr<std::unique_ptr<Archive>> Open(
      std::filesystem::path file) {
    ASSIGN_OR_RETURN(auto db, Sqlite::Open(file));

    // TODO: check whether there is already some data in the proper format.

    // Create tables.
    RETURN_IF_ERROR(db.Run(kCreateValueTable));

    // Prepare query statements.
    ASSIGN_OR_RETURN(auto add_value, db.Prepare(kAddValueStmt));
    ASSIGN_OR_RETURN(auto get_value, db.Prepare(kGetValueStmt));

    return std::unique_ptr<Archive>(new Archive(
        std::move(db), std::make_unique<SqlStatement>(std::move(add_value)),
        std::make_unique<SqlStatement>(std::move(get_value))));
  }

  // Adds the block update for the given block.
  absl::Status Add(BlockId block, const BlockUpdate& update) {
    auto guard = absl::MutexLock(&add_value_lock_);
    if (!add_value_stmt_) return absl::FailedPreconditionError("DB Closed");
    RETURN_IF_ERROR(db_.Run("BEGIN TRANSACTION"));
    for (auto& [key, value] : update.GetStorage()) {
      RETURN_IF_ERROR(add_value_stmt_->Reset());
      RETURN_IF_ERROR(add_value_stmt_->Bind(0, key.account));
      RETURN_IF_ERROR(add_value_stmt_->Bind(1, key.slot));
      RETURN_IF_ERROR(add_value_stmt_->Bind(2, static_cast<int>(block)));
      RETURN_IF_ERROR(add_value_stmt_->Bind(3, value));
      RETURN_IF_ERROR(add_value_stmt_->Run());
    }
    return db_.Run("END TRANSACTION");
  }

  // Fetches the value of a storage slot at the given block height. If the value
  // was not defined at this block (or any time before) a zero value is
  // returned.
  absl::StatusOr<Value> GetStorage(BlockId block, const Address& account,
                                   const Key& key) {
    // TODO: once account states are tracked, make sure the account exists at
    // that block.
    auto guard = absl::MutexLock(&get_value_lock_);
    if (!get_value_stmt_) return absl::FailedPreconditionError("DB Closed");
    RETURN_IF_ERROR(get_value_stmt_->Reset());
    RETURN_IF_ERROR(get_value_stmt_->Bind(0, account));
    RETURN_IF_ERROR(get_value_stmt_->Bind(1, key));
    RETURN_IF_ERROR(get_value_stmt_->Bind(2, static_cast<int>(block)));

    // Here the first result is fetched. If there is no result, returning the
    // zero value is what is expected since this is the default value of storage
    // slots.
    Value result;
    RETURN_IF_ERROR(get_value_stmt_->Run(
        [&](const SqlRow& row) { result.SetBytes(row.GetBytes(0)); }));
    return result;
  }

  absl::Status Flush() {
    // Nothing to do.
    return absl::OkStatus();
  }

  // Closes this archive. After this, no more operations are allowed on it (not
  // checked).
  absl::Status Close() {
    // Before closing the DB all prepared statements need to be finalized.
    {
      auto guard = absl::MutexLock(&add_value_lock_);
      add_value_stmt_.reset();
    }
    {
      auto guard = absl::MutexLock(&get_value_lock_);
      get_value_stmt_.reset();
    }
    return db_.Close();
  }

 private:
  // See reference: https://www.sqlite.org/lang.html

  static constexpr const std::string_view kCreateValueTable =
      "CREATE TABLE storage (account BLOB, slot BLOB, block INT, value BLOB, "
      "PRIMARY KEY (account,slot,block))";

  static constexpr const std::string_view kAddValueStmt =
      "INSERT INTO storage(account,slot,block,value) VALUES (?,?,?,?)";

  static constexpr const std::string_view kGetValueStmt =
      "SELECT value FROM storage WHERE account = ? AND slot = ? AND block <= ? "
      "ORDER BY block DESC LIMIT 1";

  Archive(Sqlite db, std::unique_ptr<SqlStatement> add_value,
          std::unique_ptr<SqlStatement> get_value)
      : db_(std::move(db)),
        add_value_stmt_(std::move(add_value)),
        get_value_stmt_(std::move(get_value)) {}

  // The DB connection.
  Sqlite db_;

  // TODO: introduce pool of statements to support concurrent reads and writes.

  // The prepared statement for adding new values to the storage history.
  std::unique_ptr<SqlStatement> add_value_stmt_ GUARDED_BY(add_value_lock_);
  absl::Mutex add_value_lock_;

  // The prepared statement for fetching values from the storage history.
  std::unique_ptr<SqlStatement> get_value_stmt_ GUARDED_BY(get_value_lock_);
  absl::Mutex get_value_lock_;
};

}  // namespace internal

Archive::Archive(std::unique_ptr<internal::Archive> impl)
    : impl_(std::move(impl)) {}

Archive::Archive(Archive&&) = default;

Archive::~Archive() { Close().IgnoreError(); }

Archive& Archive::operator=(Archive&&) = default;

absl::StatusOr<Archive> Archive::Open(std::filesystem::path directory) {
  // TODO: create directory if it does not exist.
  ASSIGN_OR_RETURN(auto impl,
                   internal::Archive::Open(directory / "archive.sqlite"));
  return Archive(std::move(impl));
}

absl::Status Archive::Add(BlockId block, const BlockUpdate& update) {
  RETURN_IF_ERROR(CheckState());
  return impl_->Add(block, update);
}

absl::StatusOr<Value> Archive::GetStorage(BlockId block, const Address& account,
                                          const Key& key) {
  RETURN_IF_ERROR(CheckState());
  return impl_->GetStorage(block, account, key);
}

absl::Status Archive::Flush() {
  if (!impl_) return absl::OkStatus();
  return impl_->Flush();
}

absl::Status Archive::Close() {
  if (!impl_) return absl::OkStatus();
  auto result = impl_->Close();
  impl_ = nullptr;
  return result;
}

absl::Status Archive::CheckState() const {
  if (impl_) return absl::OkStatus();
  return absl::FailedPreconditionError("Archive not connected to DB.");
}

void BlockUpdate::Set(const Address& account, const Key& key,
                      const Value& value) {
  storage_[{account, key}] = value;
}

}  // namespace carmen
