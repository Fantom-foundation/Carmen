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
    RETURN_IF_ERROR(db.Run(kCreateBalanceTable));
    RETURN_IF_ERROR(db.Run(kCreateCodeTable));
    RETURN_IF_ERROR(db.Run(kCreateNonceTable));
    RETURN_IF_ERROR(db.Run(kCreateValueTable));

    // Prepare query statements.
    ASSIGN_OR_RETURN(auto add_balance, db.Prepare(kAddBalanceStmt));
    ASSIGN_OR_RETURN(auto get_balance, db.Prepare(kGetBalanceStmt));

    ASSIGN_OR_RETURN(auto add_code, db.Prepare(kAddCodeStmt));
    ASSIGN_OR_RETURN(auto get_code, db.Prepare(kGetCodeStmt));

    ASSIGN_OR_RETURN(auto add_nonce, db.Prepare(kAddNonceStmt));
    ASSIGN_OR_RETURN(auto get_nonce, db.Prepare(kGetNonceStmt));

    ASSIGN_OR_RETURN(auto add_value, db.Prepare(kAddValueStmt));
    ASSIGN_OR_RETURN(auto get_value, db.Prepare(kGetValueStmt));

    auto wrap = [](SqlStatement stmt) -> std::unique_ptr<SqlStatement> {
      return std::make_unique<SqlStatement>(std::move(stmt));
    };

    return std::unique_ptr<Archive>(
        new Archive(std::move(db), wrap(std::move(add_balance)),
                    wrap(std::move(get_balance)), wrap(std::move(add_code)),
                    wrap(std::move(get_code)), wrap(std::move(add_nonce)),
                    wrap(std::move(get_nonce)), wrap(std::move(add_value)),
                    wrap(std::move(get_value))));
  }

  // Adds the block update for the given block.
  absl::Status Add(BlockId block, const Update& update) {
    auto guard = absl::MutexLock(&mutation_lock_);
    if (!add_value_stmt_) return absl::FailedPreconditionError("DB Closed");
    RETURN_IF_ERROR(db_.Run("BEGIN TRANSACTION"));
    // TODO: support account creation / deletion;
    for (auto& [addr, balance] : update.GetBalances()) {
      RETURN_IF_ERROR(add_balance_stmt_->Reset());
      RETURN_IF_ERROR(add_balance_stmt_->Bind(0, addr));
      RETURN_IF_ERROR(add_balance_stmt_->Bind(1, static_cast<int>(block)));
      RETURN_IF_ERROR(add_balance_stmt_->Bind(2, balance));
      RETURN_IF_ERROR(add_balance_stmt_->Run());
    }

    for (auto& [addr, code] : update.GetCodes()) {
      RETURN_IF_ERROR(add_code_stmt_->Reset());
      RETURN_IF_ERROR(add_code_stmt_->Bind(0, addr));
      RETURN_IF_ERROR(add_code_stmt_->Bind(1, static_cast<int>(block)));
      RETURN_IF_ERROR(add_code_stmt_->Bind(2, code));
      RETURN_IF_ERROR(add_code_stmt_->Run());
    }

    for (auto& [addr, nonce] : update.GetNonces()) {
      RETURN_IF_ERROR(add_nonce_stmt_->Reset());
      RETURN_IF_ERROR(add_nonce_stmt_->Bind(0, addr));
      RETURN_IF_ERROR(add_nonce_stmt_->Bind(1, static_cast<int>(block)));
      RETURN_IF_ERROR(add_nonce_stmt_->Bind(2, nonce));
      RETURN_IF_ERROR(add_nonce_stmt_->Run());
    }

    for (auto& [addr, key, value] : update.GetStorage()) {
      RETURN_IF_ERROR(add_value_stmt_->Reset());
      RETURN_IF_ERROR(add_value_stmt_->Bind(0, addr));
      RETURN_IF_ERROR(add_value_stmt_->Bind(1, key));
      RETURN_IF_ERROR(add_value_stmt_->Bind(2, static_cast<int>(block)));
      RETURN_IF_ERROR(add_value_stmt_->Bind(3, value));
      RETURN_IF_ERROR(add_value_stmt_->Run());
    }
    return db_.Run("END TRANSACTION");
  }

  absl::StatusOr<Balance> GetBalance(BlockId block, const Address& account) {
    // TODO: once account states are tracked, make sure the account exists at
    // that block.
    auto guard = absl::MutexLock(&get_balance_lock_);
    if (!get_balance_stmt_) return absl::FailedPreconditionError("DB Closed");
    RETURN_IF_ERROR(get_balance_stmt_->Reset());
    RETURN_IF_ERROR(get_balance_stmt_->Bind(0, account));
    RETURN_IF_ERROR(get_balance_stmt_->Bind(1, static_cast<int>(block)));

    // The query produces 0 or 1 results. If there is no result, returning the
    // zero value is what is expected since this is the default balance.
    Balance result;
    RETURN_IF_ERROR(get_balance_stmt_->Run(
        [&](const SqlRow& row) { result.SetBytes(row.GetBytes(0)); }));
    return result;
  }

  absl::StatusOr<Code> GetCode(BlockId block, const Address& account) {
    // TODO: once account states are tracked, make sure the account exists at
    // that block.
    auto guard = absl::MutexLock(&get_code_lock_);
    if (!get_code_stmt_) return absl::FailedPreconditionError("DB Closed");
    RETURN_IF_ERROR(get_code_stmt_->Reset());
    RETURN_IF_ERROR(get_code_stmt_->Bind(0, account));
    RETURN_IF_ERROR(get_code_stmt_->Bind(1, static_cast<int>(block)));

    // The query produces 0 or 1 results. If there is no result, returning the
    // zero value is what is expected since this is the default code.
    Code result;
    RETURN_IF_ERROR(get_code_stmt_->Run(
        [&](const SqlRow& row) { result = Code(row.GetBytes(0)); }));
    return result;
  }

  absl::StatusOr<Nonce> GetNonce(BlockId block, const Address& account) {
    // TODO: once account states are tracked, make sure the account exists at
    // that block.
    auto guard = absl::MutexLock(&get_nonce_lock_);
    if (!get_nonce_stmt_) return absl::FailedPreconditionError("DB Closed");
    RETURN_IF_ERROR(get_nonce_stmt_->Reset());
    RETURN_IF_ERROR(get_nonce_stmt_->Bind(0, account));
    RETURN_IF_ERROR(get_nonce_stmt_->Bind(1, static_cast<int>(block)));

    // The query produces 0 or 1 results. If there is no result, returning the
    // zero value is what is expected since this is the default balance.
    Nonce result;
    RETURN_IF_ERROR(get_nonce_stmt_->Run(
        [&](const SqlRow& row) { result.SetBytes(row.GetBytes(0)); }));
    return result;
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

    // The query produces 0 or 1 results. If there is no result, returning the
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
      auto guard = absl::MutexLock(&mutation_lock_);
      add_balance_stmt_.reset();
      add_code_stmt_.reset();
      add_nonce_stmt_.reset();
      add_value_stmt_.reset();
    }
    {
      auto guard = absl::MutexLock(&get_balance_lock_);
      get_balance_stmt_.reset();
    }
    {
      auto guard = absl::MutexLock(&get_code_lock_);
      get_code_stmt_.reset();
    }
    {
      auto guard = absl::MutexLock(&get_nonce_lock_);
      get_nonce_stmt_.reset();
    }
    {
      auto guard = absl::MutexLock(&get_value_lock_);
      get_value_stmt_.reset();
    }
    return db_.Close();
  }

 private:
  // See reference: https://www.sqlite.org/lang.html

  // -- Balance --

  static constexpr const std::string_view kCreateBalanceTable =
      "CREATE TABLE balance (account BLOB, block INT, value BLOB, "
      "PRIMARY KEY (account,block))";

  static constexpr const std::string_view kAddBalanceStmt =
      "INSERT INTO balance(account,block,value) VALUES (?,?,?)";

  static constexpr const std::string_view kGetBalanceStmt =
      "SELECT value FROM balance WHERE account = ? AND block <= ? "
      "ORDER BY block DESC LIMIT 1";

  // -- Code --

  static constexpr const std::string_view kCreateCodeTable =
      "CREATE TABLE code (account BLOB, block INT, code BLOB, "
      "PRIMARY KEY (account,block))";

  static constexpr const std::string_view kAddCodeStmt =
      "INSERT INTO code(account,block,code) VALUES (?,?,?)";

  static constexpr const std::string_view kGetCodeStmt =
      "SELECT code FROM code WHERE account = ? AND block <= ? "
      "ORDER BY block DESC LIMIT 1";

  // -- Nonces --

  static constexpr const std::string_view kCreateNonceTable =
      "CREATE TABLE nonce (account BLOB, block INT, value BLOB, "
      "PRIMARY KEY (account,block))";

  static constexpr const std::string_view kAddNonceStmt =
      "INSERT INTO nonce(account,block,value) VALUES (?,?,?)";

  static constexpr const std::string_view kGetNonceStmt =
      "SELECT value FROM nonce WHERE account = ? AND block <= ? "
      "ORDER BY block DESC LIMIT 1";

  // -- Storage --

  static constexpr const std::string_view kCreateValueTable =
      "CREATE TABLE storage (account BLOB, slot BLOB, block INT, value BLOB, "
      "PRIMARY KEY (account,slot,block))";

  static constexpr const std::string_view kAddValueStmt =
      "INSERT INTO storage(account,slot,block,value) VALUES (?,?,?,?)";

  static constexpr const std::string_view kGetValueStmt =
      "SELECT value FROM storage WHERE account = ? AND slot = ? AND block <= ? "
      "ORDER BY block DESC LIMIT 1";

  Archive(Sqlite db, std::unique_ptr<SqlStatement> add_balance,
          std::unique_ptr<SqlStatement> get_balance,
          std::unique_ptr<SqlStatement> add_code,
          std::unique_ptr<SqlStatement> get_code,
          std::unique_ptr<SqlStatement> add_nonce,
          std::unique_ptr<SqlStatement> get_nonce,
          std::unique_ptr<SqlStatement> add_value,
          std::unique_ptr<SqlStatement> get_value)
      : db_(std::move(db)),
        add_balance_stmt_(std::move(add_balance)),
        get_balance_stmt_(std::move(get_balance)),
        add_code_stmt_(std::move(add_code)),
        get_code_stmt_(std::move(get_code)),
        add_nonce_stmt_(std::move(add_nonce)),
        get_nonce_stmt_(std::move(get_nonce)),
        add_value_stmt_(std::move(add_value)),
        get_value_stmt_(std::move(get_value)) {}

  // The DB connection.
  Sqlite db_;

  // TODO: introduce pool of statements to support concurrent reads and writes.

  // Prepared statemetns for logging new data to the archive.
  absl::Mutex mutation_lock_;

  absl::Mutex get_balance_lock_;
  std::unique_ptr<SqlStatement> add_balance_stmt_ GUARDED_BY(mutation_lock_);
  std::unique_ptr<SqlStatement> get_balance_stmt_ GUARDED_BY(get_balance_lock_);

  absl::Mutex get_code_lock_;
  std::unique_ptr<SqlStatement> add_code_stmt_ GUARDED_BY(mutation_lock_);
  std::unique_ptr<SqlStatement> get_code_stmt_ GUARDED_BY(get_code_lock_);

  absl::Mutex get_nonce_lock_;
  std::unique_ptr<SqlStatement> add_nonce_stmt_ GUARDED_BY(mutation_lock_);
  std::unique_ptr<SqlStatement> get_nonce_stmt_ GUARDED_BY(get_nonce_lock_);

  absl::Mutex get_value_lock_;
  std::unique_ptr<SqlStatement> add_value_stmt_ GUARDED_BY(mutation_lock_);
  std::unique_ptr<SqlStatement> get_value_stmt_ GUARDED_BY(get_value_lock_);
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

absl::Status Archive::Add(BlockId block, const Update& update) {
  RETURN_IF_ERROR(CheckState());
  return impl_->Add(block, update);
}

absl::StatusOr<Balance> Archive::GetBalance(BlockId block,
                                            const Address& account) {
  RETURN_IF_ERROR(CheckState());
  return impl_->GetBalance(block, account);
}

absl::StatusOr<Code> Archive::GetCode(BlockId block, const Address& account) {
  RETURN_IF_ERROR(CheckState());
  return impl_->GetCode(block, account);
}

absl::StatusOr<Nonce> Archive::GetNonce(BlockId block, const Address& account) {
  RETURN_IF_ERROR(CheckState());
  return impl_->GetNonce(block, account);
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

}  // namespace carmen
