// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

#include "archive/sqlite/archive.h"

#include <algorithm>
#include <queue>

#include "absl/container/btree_map.h"
#include "absl/container/flat_hash_map.h"
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/synchronization/mutex.h"
#include "backend/common/file.h"
#include "backend/common/sqlite/sqlite.h"
#include "common/hash.h"
#include "common/memory_usage.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen::archive::sqlite {

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
    RETURN_IF_ERROR(db.Run(kCreateBlockTable));
    RETURN_IF_ERROR(db.Run(kCreateAccountHashTable));
    RETURN_IF_ERROR(db.Run(kCreateStatusTable));
    RETURN_IF_ERROR(db.Run(kCreateBalanceTable));
    RETURN_IF_ERROR(db.Run(kCreateCodeTable));
    RETURN_IF_ERROR(db.Run(kCreateNonceTable));
    RETURN_IF_ERROR(db.Run(kCreateValueTable));

    // Prepare query statements.
    ASSIGN_OR_RETURN(auto add_block, db.Prepare(kAddBlockStmt));
    ASSIGN_OR_RETURN(auto get_block_hash, db.Prepare(kGetBlockHashStmt));
    ASSIGN_OR_RETURN(auto get_block_height, db.Prepare(kGetBlockHeightStmt));

    ASSIGN_OR_RETURN(auto add_account_hash, db.Prepare(kAddAccountHashStmt));
    ASSIGN_OR_RETURN(auto get_account_hash, db.Prepare(kGetAccountHashStmt));

    ASSIGN_OR_RETURN(auto create_account, db.Prepare(kCreateAccountStmt));
    ASSIGN_OR_RETURN(auto delete_account, db.Prepare(kDeleteAccountStmt));
    ASSIGN_OR_RETURN(auto get_status, db.Prepare(kGetStatusStmt));

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

    return std::unique_ptr<Archive>(new Archive(
        std::move(db), wrap(std::move(add_block)),
        wrap(std::move(get_block_hash)), wrap(std::move(get_block_height)),
        wrap(std::move(add_account_hash)), wrap(std::move(get_account_hash)),
        wrap(std::move(create_account)), wrap(std::move(delete_account)),
        wrap(std::move(get_status)), wrap(std::move(add_balance)),
        wrap(std::move(get_balance)), wrap(std::move(add_code)),
        wrap(std::move(get_code)), wrap(std::move(add_nonce)),
        wrap(std::move(get_nonce)), wrap(std::move(add_value)),
        wrap(std::move(get_value))));
  }

  // Adds the block update for the given block.
  absl::Status Add(BlockId block, const Update& update) {
    // Check that new block is newer than anything before.
    ASSIGN_OR_RETURN(std::int64_t newestBlock, GetLastBlockHeight());
    if (newestBlock >= 0 && BlockId(newestBlock) >= block) {
      return absl::FailedPreconditionError(absl::StrFormat(
          "Unable to insert block %d, archive already contains block %d", block,
          newestBlock));
    }

    // Empty updates are ignored since non-logged blocks are empty by default.
    // However, this is important since the hash of a block introducing no
    // changes is equivalent to the hash of its predecessor. If an empty block
    // would be added, the hash would change.
    if (update.Empty()) {
      return absl::OkStatus();
    }

    // Compute hashes of account updates.
    absl::btree_map<Address, Hash> diff_hashes;
    for (const auto& [addr, diff] : AccountUpdate::From(update)) {
      diff_hashes[addr] = diff.GetHash();
    }

    // Fill in data in a single transaction.
    auto guard = absl::MutexLock(&mutation_lock_);
    if (!add_value_stmt_) return absl::FailedPreconditionError("DB Closed");
    RETURN_IF_ERROR(db_.Run("BEGIN TRANSACTION"));

    for (auto& addr : update.GetDeletedAccounts()) {
      RETURN_IF_ERROR(delete_account_stmt_->Run(addr, block));
    }

    for (auto& addr : update.GetCreatedAccounts()) {
      RETURN_IF_ERROR(create_account_stmt_->Run(addr, block));
    }

    for (auto& [addr, balance] : update.GetBalances()) {
      RETURN_IF_ERROR(add_balance_stmt_->Run(addr, block, balance));
    }

    for (auto& [addr, code] : update.GetCodes()) {
      RETURN_IF_ERROR(add_code_stmt_->Run(addr, block, code));
    }

    for (auto& [addr, nonce] : update.GetNonces()) {
      RETURN_IF_ERROR(add_nonce_stmt_->Run(addr, block, nonce));
    }

    for (auto& [addr, key, value] : update.GetStorage()) {
      RETURN_IF_ERROR(add_value_stmt_->Run(addr, key, block, value));
    }

    Sha256Hasher hasher;
    ASSIGN_OR_RETURN(auto last_block_hash, GetHash(block));
    hasher.Ingest(last_block_hash);

    for (auto& [addr, hash] : diff_hashes) {
      ASSIGN_OR_RETURN(auto last_hash, GetAccountHash(block, addr));
      auto new_hash = GetSha256Hash(last_hash, hash);
      RETURN_IF_ERROR(add_account_hash_stmt_->Run(addr, block, new_hash));
      hasher.Ingest(new_hash);
    }

    RETURN_IF_ERROR(add_block_stmt_->Run(block, hasher.GetHash()));

    return db_.Run("END TRANSACTION");
  }

  // Gets the maximum block height insert so far, returns -1 if there is none.
  absl::StatusOr<std::int64_t> GetLastBlockHeight() {
    auto guard = absl::MutexLock(&get_block_height_lock_);
    if (!get_block_height_stmt_)
      return absl::FailedPreconditionError("DB Closed");
    std::int64_t result = -1;
    RETURN_IF_ERROR(get_block_height_stmt_->Execute(
        [&](const SqlRow& row) { result = row.GetInt64(0); }));
    return result;
  }

  absl::StatusOr<bool> Exists(BlockId block, const Address& account) {
    auto guard = absl::MutexLock(&get_status_lock_);
    if (!get_status_stmt_) return absl::FailedPreconditionError("DB Closed");
    RETURN_IF_ERROR(get_status_stmt_->BindParameters(account, block));

    // The query produces 0 or 1 results. If there is no result, returning false
    // is what is expected since this is the default account state.
    bool result = false;
    RETURN_IF_ERROR(get_status_stmt_->Execute(
        [&](const SqlRow& row) { result = (row.GetInt(0) != 0); }));
    return result;
  }

  absl::StatusOr<Balance> GetBalance(BlockId block, const Address& account) {
    // TODO: once account states are tracked, make sure the account exists at
    // that block.
    auto guard = absl::MutexLock(&get_balance_lock_);
    if (!get_balance_stmt_) return absl::FailedPreconditionError("DB Closed");
    RETURN_IF_ERROR(get_balance_stmt_->BindParameters(account, block));

    // The query produces 0 or 1 results. If there is no result, returning the
    // zero value is what is expected since this is the default balance.
    Balance result{};
    RETURN_IF_ERROR(get_balance_stmt_->Execute(
        [&](const SqlRow& row) { result.SetBytes(row.GetBytes(0)); }));
    return result;
  }

  absl::StatusOr<Code> GetCode(BlockId block, const Address& account) {
    // TODO: once account states are tracked, make sure the account exists at
    // that block.
    auto guard = absl::MutexLock(&get_code_lock_);
    if (!get_code_stmt_) return absl::FailedPreconditionError("DB Closed");
    RETURN_IF_ERROR(get_code_stmt_->BindParameters(account, block));

    // The query produces 0 or 1 results. If there is no result, returning the
    // zero value is what is expected since this is the default code.
    Code result{};
    RETURN_IF_ERROR(get_code_stmt_->Execute(
        [&](const SqlRow& row) { result = Code(row.GetBytes(0)); }));
    return result;
  }

  absl::StatusOr<Nonce> GetNonce(BlockId block, const Address& account) {
    // TODO: once account states are tracked, make sure the account exists at
    // that block.
    auto guard = absl::MutexLock(&get_nonce_lock_);
    if (!get_nonce_stmt_) return absl::FailedPreconditionError("DB Closed");
    RETURN_IF_ERROR(get_nonce_stmt_->BindParameters(account, block));

    // The query produces 0 or 1 results. If there is no result, returning the
    // zero value is what is expected since this is the default balance.
    Nonce result{};
    RETURN_IF_ERROR(get_nonce_stmt_->Execute(
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
    RETURN_IF_ERROR(get_value_stmt_->BindParameters(account, key, block));

    // The query produces 0 or 1 results. If there is no result, returning the
    // zero value is what is expected since this is the default value of storage
    // slots.
    Value result{};
    RETURN_IF_ERROR(get_value_stmt_->Execute(
        [&](const SqlRow& row) { result.SetBytes(row.GetBytes(0)); }));
    return result;
  }

  absl::StatusOr<Hash> GetHash(BlockId block) {
    auto guard = absl::MutexLock(&get_block_hash_lock_);
    if (!get_block_hash_stmt_)
      return absl::FailedPreconditionError("DB Closed");
    RETURN_IF_ERROR(get_block_hash_stmt_->BindParameters(block));

    // If there is no block in the archive, the hash is supposed to be zero.
    Hash result{};
    RETURN_IF_ERROR(get_block_hash_stmt_->Execute(
        [&](const SqlRow& row) { result.SetBytes(row.GetBytes(0)); }));
    return result;
  }

  absl::StatusOr<std::vector<Address>> GetAccountList(BlockId block) {
    std::vector<Address> res;
    ASSIGN_OR_RETURN(auto query,
                     db_.Prepare("SELECT DISTINCT account FROM account_hash "
                                 "WHERE block <= ? ORDER BY account"));
    RETURN_IF_ERROR(query.BindParameters(block));
    RETURN_IF_ERROR(query.Execute([&](const SqlRow& row) {
      Address addr;
      addr.SetBytes(row.GetBytes(0));
      res.push_back(addr);
    }));
    return res;
  }

  // Fetches the hash of the given account on the given block height. The hash
  // of an account is initially zero. Subsequent updates create a hash chain
  // covering the previous state and the hash of applied diffs.
  absl::StatusOr<Hash> GetAccountHash(BlockId block, const Address& account) {
    auto guard = absl::MutexLock(&get_account_hash_lock_);
    if (!get_account_hash_stmt_)
      return absl::FailedPreconditionError("DB Closed");
    RETURN_IF_ERROR(get_account_hash_stmt_->BindParameters(account, block));

    // The query produces 0 or 1 results. If there is no result, returning the
    // zero hash is expected, since it is the hash of a non-existing account.
    Hash result{};
    RETURN_IF_ERROR(get_account_hash_stmt_->Execute(
        [&](const SqlRow& row) { result.SetBytes(row.GetBytes(0)); }));
    return result;
  }

  absl::Status Verify(
      BlockId block, const Hash& expected_hash,
      absl::FunctionRef<void(std::string_view)> progress_callback) {
    progress_callback("DB integrity check");
    // Start by checking the DB integrity.
    ASSIGN_OR_RETURN(auto integrity_check_stmt,
                     db_.Prepare("PRAGMA integrity_check"));
    std::vector<std::string> issues;
    RETURN_IF_ERROR(integrity_check_stmt.Execute([&](const SqlRow& row) {
      auto msg = row.GetString(0);
      if (msg != "ok") {
        issues.emplace_back(msg);
      }
    }));
    if (!issues.empty()) {
      std::stringstream out;
      for (const auto& cur : issues) {
        out << "\t" << cur << "\n";
      }
      return absl::InternalError("Encountered DB integrity issues:\n" +
                                 out.str());
    }

    // Next, check the expected hash.
    progress_callback("checking archive root hash");
    ASSIGN_OR_RETURN(auto hash, GetHash(block));
    if (hash != expected_hash) {
      return absl::InternalError("Archive hash does not match expected hash.");
    }

    // Verify that the block hashes are consistent within the archive.
    RETURN_IF_ERROR(VerifyHashes(block));

    // Validate all individual accounts.
    progress_callback("getting list of accounts");
    ASSIGN_OR_RETURN(auto accounts, GetAccountList(block));
    progress_callback(absl::StrFormat("checking %d accounts", accounts.size()));
    for (const auto& cur : accounts) {
      RETURN_IF_ERROR(VerifyAccount(block, cur));
    }

    // Check that there is no extra information in any of the content tables.
    ASSIGN_OR_RETURN(BlockId latestBlock, GetLastBlockHeight());
    progress_callback("checking for extra data in tables");
    for (auto table : {"status", "balance", "nonce", "code", "storage"}) {
      // Check that there are no additional addresses referenced.
      ASSIGN_OR_RETURN(
          auto no_extra_address_check,
          db_.Prepare(absl::StrFormat(
              "SELECT 1 FROM (SELECT account FROM %s WHERE block "
              "<= ?1 EXCEPT SELECT account FROM account_hash WHERE "
              "block <= ?1) LIMIT 1",
              table)));
      RETURN_IF_ERROR(no_extra_address_check.BindParameters(block));

      bool found = false;
      RETURN_IF_ERROR(
          no_extra_address_check.Execute([&](const auto&) { found = true; }));
      if (found) {
        return absl::InternalError(
            absl::StrFormat("Found extra row of data in table `%s`.", table));
      }

      // Check that there is no future information for a block not covered yet.
      // This depends on the fact that blocks can only be added in order.
      ASSIGN_OR_RETURN(auto no_future_block_check,
                       db_.Prepare(absl::StrFormat(
                           "SELECT 1 FROM %s WHERE block > ? LIMIT 1", table)));
      RETURN_IF_ERROR(no_future_block_check.BindParameters(latestBlock));

      RETURN_IF_ERROR(
          no_future_block_check.Execute([&](const auto&) { found = true; }));
      if (found) {
        return absl::InternalError(absl::StrFormat(
            "Found entry of future block height in `%s`.", table));
      }
    }

    // All checks have passed. DB is verified.
    return absl::OkStatus();
  }

  // Verifies the consistency of the stored full archive hashes up until (and
  // including) the given block number.
  absl::Status VerifyHashes(BlockId block) {
    ASSIGN_OR_RETURN(auto block_hashes,
                     db_.Query("SELECT number, hash FROM block WHERE number "
                               "<= ? ORDER BY number",
                               block));
    ASSIGN_OR_RETURN(auto diff_hashes,
                     db_.Query("SELECT block, hash FROM account_hash WHERE "
                               "block <= ? ORDER BY block, account",
                               block));

    ASSIGN_OR_RETURN(auto block_iter, block_hashes.Iterator());
    ASSIGN_OR_RETURN(auto diff_iter, diff_hashes.Iterator());
    RETURN_IF_ERROR(block_iter.Next());
    RETURN_IF_ERROR(diff_iter.Next());

    Hash hash{};
    Sha256Hasher hasher;
    while (!block_iter.Finished()) {
      hasher.Reset();
      hasher.Ingest(hash);
      auto block = block_iter->GetInt64(0);
      while (!diff_iter.Finished()) {
        auto diff_block = diff_iter->GetInt64(0);
        if (diff_block == block) {
          hasher.Ingest(diff_iter->Get<Hash>(1));
          RETURN_IF_ERROR(diff_iter.Next());
        } else if (diff_block < block) {
          return absl::InternalError(absl::StrFormat(
              "Found account update for block %d but no hash for this block.",
              diff_block));
        } else {
          break;
        }
      }
      hash = hasher.GetHash();
      if (hash != block_iter->Get<Hash>(1)) {
        return absl::InternalError(
            absl::StrFormat("Validation of hash of block %d failed.", block));
      }
      RETURN_IF_ERROR(block_iter.Next());
    }

    if (!diff_iter.Finished()) {
      return absl::InternalError(absl::StrFormat(
          "Found change in block %d not covered by archive hash.",
          diff_iter->GetInt64(0)));
    }
    return absl::OkStatus();
  }

  // Verifyies the consistency of the provides account up until the given block.
  absl::Status VerifyAccount(BlockId block, const Address& account) {
    using ::carmen::backend::SqlIterator;
    ASSIGN_OR_RETURN(auto list_diffs,
                     db_.Prepare("SELECT block, hash FROM account_hash WHERE "
                                 "account = ? AND block <= ? ORDER BY block"));

    ASSIGN_OR_RETURN(
        auto list_state,
        db_.Prepare("SELECT block, exist, reincarnation FROM status WHERE "
                    "account = ? AND block <= ? ORDER BY block"));

    ASSIGN_OR_RETURN(auto list_balance,
                     db_.Prepare("SELECT block, value FROM balance WHERE "
                                 "account = ? AND block <= ? ORDER BY block"));

    ASSIGN_OR_RETURN(auto list_nonce,
                     db_.Prepare("SELECT block, value FROM nonce WHERE "
                                 "account = ? AND block <= ? ORDER BY block"));

    ASSIGN_OR_RETURN(auto list_code,
                     db_.Prepare("SELECT block, code FROM code WHERE "
                                 "account = ? AND block <= ? ORDER BY block"));

    ASSIGN_OR_RETURN(
        auto list_storage,
        db_.Prepare(
            "SELECT block, slot, value, reincarnation FROM storage WHERE "
            "account = ? AND block <= ? ORDER BY block, slot"));

    // Open individual result iterators.
    ASSIGN_OR_RETURN(auto hash_iter, list_diffs.Open(account, block));
    ASSIGN_OR_RETURN(auto state_iter, list_state.Open(account, block));
    ASSIGN_OR_RETURN(auto balance_iter, list_balance.Open(account, block));
    ASSIGN_OR_RETURN(auto nonce_iter, list_nonce.Open(account, block));
    ASSIGN_OR_RETURN(auto code_iter, list_code.Open(account, block));
    ASSIGN_OR_RETURN(auto storage_iter, list_storage.Open(account, block));

    // Find the first block referencing the account.
    BlockId next = block + 1;
    for (SqlIterator* iter :
         {&state_iter, &balance_iter, &nonce_iter, &code_iter, &storage_iter}) {
      ASSIGN_OR_RETURN(auto has_next, iter->Next());
      if (has_next) {
        next = std::min<BlockId>(next, (*iter)->GetInt64(0));
      }
    }

    // Keep track of the reincarnation number.
    int reincarnation = -1;

    Hash hash{};
    std::optional<BlockId> last;
    while (next <= block) {
      BlockId current = next;
      if (last.has_value() && current <= last) {
        // This should only be possible if primary key constraints are violated.
        return absl::InternalError(
            absl::StrFormat("Multiple updates for block %d found", current));
      }
      last = current;

      // --- Recreate Update for Current Block ---
      AccountUpdate update;

      if (!state_iter.Finished() && state_iter->GetInt64(0) == current) {
        if (state_iter->GetInt(1) == 0) {
          update.deleted = true;
        } else {
          update.created = true;
        }
        int new_reincarnation_number = state_iter->GetInt(2);
        if (new_reincarnation_number != reincarnation + 1) {
          return absl::InternalError(absl::StrFormat(
              "Reincarnation numbers are not incremental, at block %d the "
              "value moves from %d to %d",
              current, reincarnation, new_reincarnation_number));
        }
        reincarnation = new_reincarnation_number;
        RETURN_IF_ERROR(state_iter.Next());
      }

      if (!balance_iter.Finished() && balance_iter->GetInt64(0) == current) {
        Balance balance;
        balance.SetBytes(balance_iter->GetBytes(1));
        update.balance = balance;
        RETURN_IF_ERROR(balance_iter.Next());
      }

      if (!nonce_iter.Finished() && nonce_iter->GetInt64(0) == current) {
        Nonce nonce;
        nonce.SetBytes(nonce_iter->GetBytes(1));
        update.nonce = nonce;
        RETURN_IF_ERROR(nonce_iter.Next());
      }

      if (!code_iter.Finished() && code_iter->GetInt64(0) == current) {
        update.code = Code(code_iter->GetBytes(1));
        RETURN_IF_ERROR(code_iter.Next());
      }

      while (!storage_iter.Finished() && storage_iter->GetInt64(0) == current) {
        int cur_reincarnation = storage_iter->GetInt(3);
        if (cur_reincarnation != reincarnation) {
          return absl::InternalError(
              absl::StrFormat("Invalid reincarnation number for storage value "
                              "at block %d, expected %d, got %d",
                              current, reincarnation, cur_reincarnation));
        }
        Key key;
        key.SetBytes(storage_iter->GetBytes(1));
        Value value;
        value.SetBytes(storage_iter->GetBytes(2));
        update.storage.push_back({key, value});
        RETURN_IF_ERROR(storage_iter.Next());
      }

      // --- Check that the current update matches the current block ---

      // Check the update against the list of per-account hashes.
      ASSIGN_OR_RETURN(auto has_next, hash_iter.Next());
      BlockId diff_block = hash_iter->GetInt64(0);
      if (!has_next || diff_block != current) {
        if (diff_block < current) {
          return absl::InternalError(
              absl::StrFormat("Archive contains hash for update at block %d "
                              "but no change for it.",
                              diff_block));
        } else {
          return absl::InternalError(absl::StrFormat(
              "Archive contains update for block %d but no hash for it.",
              current));
        }
      }

      // Compute the hash based on the diff.
      hash = GetSha256Hash(hash, update.GetHash());

      // Compare with hash stored in DB.
      Hash should;
      should.SetBytes(hash_iter->GetBytes(1));
      if (hash != should) {
        return absl::InternalError(absl::StrFormat(
            "Hash for diff at block %d does not match.", current));
      }

      // Find next block to be processed.
      next = block + 1;
      for (SqlIterator* iter : {&state_iter, &balance_iter, &nonce_iter,
                                &code_iter, &storage_iter}) {
        if (!iter->Finished()) {
          next = std::min<BlockId>(next, (*iter)->GetInt64(0));
        }
      }
    }

    // Check whether there are additional updates in the hash table.
    ASSIGN_OR_RETURN(auto has_more, hash_iter.Next());
    if (has_more) {
      return absl::InternalError(absl::StrFormat(
          "DB contains hash for update on block %d but no data.",
          hash_iter->GetInt64(0)));
    }

    return absl::OkStatus();
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
      add_block_stmt_.reset();
      create_account_stmt_.reset();
      delete_account_stmt_.reset();
      add_balance_stmt_.reset();
      add_code_stmt_.reset();
      add_nonce_stmt_.reset();
      add_value_stmt_.reset();
      add_account_hash_stmt_.reset();
    }
    {
      auto guard = absl::MutexLock(&get_block_hash_lock_);
      get_block_hash_stmt_.reset();
    }
    {
      auto guard = absl::MutexLock(&get_block_height_lock_);
      get_block_height_stmt_.reset();
    }
    {
      auto guard = absl::MutexLock(&get_account_hash_lock_);
      get_account_hash_stmt_.reset();
    }
    {
      auto guard = absl::MutexLock(&get_status_lock_);
      get_status_stmt_.reset();
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

  MemoryFootprint GetMemoryFootprint() const {
    MemoryFootprint res(*this);
    res.Add("sqlite", db_.GetMemoryFootprint());
    return res;
  }

 private:
  // See reference: https://www.sqlite.org/lang.html

  // -- Blocks --

  static constexpr const std::string_view kCreateBlockTable =
      "CREATE TABLE IF NOT EXISTS block (number INT PRIMARY KEY, hash BLOB)";

  static constexpr const std::string_view kAddBlockStmt =
      "INSERT INTO block(number,hash) VALUES (?,?)";

  static constexpr const std::string_view kGetBlockHashStmt =
      "SELECT hash FROM block WHERE number <= ? ORDER BY number DESC LIMIT 1";

  static constexpr const std::string_view kGetBlockHeightStmt =
      "SELECT number FROM block ORDER BY number DESC LIMIT 1";

  // -- Account Hashes --

  static constexpr const std::string_view kCreateAccountHashTable =
      "CREATE TABLE IF NOT EXISTS account_hash (account BLOB, block INT, hash "
      "BLOB, PRIMARY KEY(account,block))";

  static constexpr const std::string_view kAddAccountHashStmt =
      "INSERT INTO account_hash(account, block, hash) VALUES (?,?,?)";

  static constexpr const std::string_view kGetAccountHashStmt =
      "SELECT hash FROM account_hash WHERE account = ? AND block <= ? ORDER BY "
      "block DESC LIMIT 1";

  // -- Account Status --

  static constexpr const std::string_view kCreateStatusTable =
      "CREATE TABLE IF NOT EXISTS status (account BLOB, block INT, exist INT, "
      "reincarnation INT, PRIMARY KEY (account,block))";

  static constexpr const std::string_view kCreateAccountStmt =
      "INSERT INTO status(account,block,exist,reincarnation) VALUES "
      "(?1,?2,1,(SELECT IFNULL(MAX(reincarnation)+1,0) FROM status WHERE "
      "account = ?1))";

  static constexpr const std::string_view kDeleteAccountStmt =
      "INSERT INTO status(account,block,exist,reincarnation) VALUES "
      "(?1,?2,0,(SELECT IFNULL(MAX(reincarnation)+1,0) FROM status WHERE "
      "account = ?1))";

  static constexpr const std::string_view kGetStatusStmt =
      "SELECT exist FROM status WHERE account = ? AND block <= ? ORDER BY "
      "block DESC LIMIT 1";

  // -- Balance --

  static constexpr const std::string_view kCreateBalanceTable =
      "CREATE TABLE IF NOT EXISTS balance (account BLOB, block INT, value "
      "BLOB, PRIMARY KEY (account,block))";

  static constexpr const std::string_view kAddBalanceStmt =
      "INSERT INTO balance(account,block,value) VALUES (?,?,?)";

  static constexpr const std::string_view kGetBalanceStmt =
      "SELECT value FROM balance WHERE account = ? AND block <= ? "
      "ORDER BY block DESC LIMIT 1";

  // -- Code --

  static constexpr const std::string_view kCreateCodeTable =
      "CREATE TABLE IF NOT EXISTS code (account BLOB, block INT, code BLOB, "
      "PRIMARY KEY (account,block))";

  static constexpr const std::string_view kAddCodeStmt =
      "INSERT INTO code(account,block,code) VALUES (?,?,?)";

  static constexpr const std::string_view kGetCodeStmt =
      "SELECT code FROM code WHERE account = ? AND block <= ? "
      "ORDER BY block DESC LIMIT 1";

  // -- Nonces --

  static constexpr const std::string_view kCreateNonceTable =
      "CREATE TABLE IF NOT EXISTS nonce (account BLOB, block INT, value BLOB, "
      "PRIMARY KEY (account,block))";

  static constexpr const std::string_view kAddNonceStmt =
      "INSERT INTO nonce(account,block,value) VALUES (?,?,?)";

  static constexpr const std::string_view kGetNonceStmt =
      "SELECT value FROM nonce WHERE account = ? AND block <= ? "
      "ORDER BY block DESC LIMIT 1";

  // -- Storage --

  static constexpr const std::string_view kCreateValueTable =
      "CREATE TABLE IF NOT EXISTS storage (account BLOB, reincarnation INT, "
      "slot BLOB, block INT, value BLOB, PRIMARY KEY "
      "(account,reincarnation,slot,block))";

  static constexpr const std::string_view kAddValueStmt =
      "INSERT INTO storage(account,reincarnation,slot,block,value) VALUES "
      "(?1,(SELECT IFNULL(MAX(reincarnation),0) FROM status WHERE account = ?1 "
      "AND block <= ?2),?2,?3,?4)";

  static constexpr const std::string_view kGetValueStmt =
      "SELECT value FROM storage WHERE account = ?1 AND reincarnation = "
      "(SELECT "
      "IFNULL(MAX(reincarnation),0) FROM status WHERE account = ?1 AND block "
      "<= "
      "?3) AND slot = ?2 AND block <= ?3 ORDER BY block DESC LIMIT 1";

  Archive(Sqlite db, std::unique_ptr<SqlStatement> add_block,
          std::unique_ptr<SqlStatement> get_block_hash,
          std::unique_ptr<SqlStatement> get_block_height,
          std::unique_ptr<SqlStatement> add_account_hash,
          std::unique_ptr<SqlStatement> get_account_hash,
          std::unique_ptr<SqlStatement> create_account,
          std::unique_ptr<SqlStatement> delete_account,
          std::unique_ptr<SqlStatement> get_status,
          std::unique_ptr<SqlStatement> add_balance,
          std::unique_ptr<SqlStatement> get_balance,
          std::unique_ptr<SqlStatement> add_code,
          std::unique_ptr<SqlStatement> get_code,
          std::unique_ptr<SqlStatement> add_nonce,
          std::unique_ptr<SqlStatement> get_nonce,
          std::unique_ptr<SqlStatement> add_value,
          std::unique_ptr<SqlStatement> get_value)
      : db_(std::move(db)),
        add_block_stmt_(std::move(add_block)),
        get_block_hash_stmt_(std::move(get_block_hash)),
        get_block_height_stmt_(std::move(get_block_height)),
        add_account_hash_stmt_(std::move(add_account_hash)),
        get_account_hash_stmt_(std::move(get_account_hash)),
        create_account_stmt_(std::move(create_account)),
        delete_account_stmt_(std::move(delete_account)),
        get_status_stmt_(std::move(get_status)),
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

  std::unique_ptr<SqlStatement> add_block_stmt_ ABSL_GUARDED_BY(mutation_lock_);

  absl::Mutex get_block_hash_lock_;
  std::unique_ptr<SqlStatement> get_block_hash_stmt_
      ABSL_GUARDED_BY(get_block_hash_lock_);

  absl::Mutex get_block_height_lock_;
  std::unique_ptr<SqlStatement> get_block_height_stmt_
      ABSL_GUARDED_BY(get_block_height_lock_);

  absl::Mutex get_account_hash_lock_;
  std::unique_ptr<SqlStatement> add_account_hash_stmt_
      ABSL_GUARDED_BY(mutation_lock_);
  std::unique_ptr<SqlStatement> get_account_hash_stmt_
      ABSL_GUARDED_BY(get_account_hash_lock_);

  absl::Mutex get_status_lock_;
  std::unique_ptr<SqlStatement> create_account_stmt_
      ABSL_GUARDED_BY(mutation_lock_);
  std::unique_ptr<SqlStatement> delete_account_stmt_
      ABSL_GUARDED_BY(mutation_lock_);
  std::unique_ptr<SqlStatement> get_status_stmt_
      ABSL_GUARDED_BY(get_status_lock_);

  absl::Mutex get_balance_lock_;
  std::unique_ptr<SqlStatement> add_balance_stmt_
      ABSL_GUARDED_BY(mutation_lock_);
  std::unique_ptr<SqlStatement> get_balance_stmt_
      ABSL_GUARDED_BY(get_balance_lock_);

  absl::Mutex get_code_lock_;
  std::unique_ptr<SqlStatement> add_code_stmt_ ABSL_GUARDED_BY(mutation_lock_);
  std::unique_ptr<SqlStatement> get_code_stmt_ ABSL_GUARDED_BY(get_code_lock_);

  absl::Mutex get_nonce_lock_;
  std::unique_ptr<SqlStatement> add_nonce_stmt_ ABSL_GUARDED_BY(mutation_lock_);
  std::unique_ptr<SqlStatement> get_nonce_stmt_
      ABSL_GUARDED_BY(get_nonce_lock_);

  absl::Mutex get_value_lock_;
  std::unique_ptr<SqlStatement> add_value_stmt_ ABSL_GUARDED_BY(mutation_lock_);
  std::unique_ptr<SqlStatement> get_value_stmt_
      ABSL_GUARDED_BY(get_value_lock_);
};

}  // namespace internal

SqliteArchive::SqliteArchive(std::unique_ptr<internal::Archive> impl)
    : impl_(std::move(impl)) {}

SqliteArchive::SqliteArchive(SqliteArchive&&) = default;

SqliteArchive::~SqliteArchive() { Close().IgnoreError(); }

SqliteArchive& SqliteArchive::operator=(SqliteArchive&&) = default;

absl::StatusOr<SqliteArchive> SqliteArchive::Open(
    std::filesystem::path directory) {
  // Make sure the directory exists.
  RETURN_IF_ERROR(backend::CreateDirectory(directory));
  auto path = directory;
  if (std::filesystem::is_directory(directory)) {
    path = path / "archive.sqlite";
  }
  ASSIGN_OR_RETURN(auto impl, internal::Archive::Open(path));
  return SqliteArchive(std::move(impl));
}

absl::Status SqliteArchive::Add(BlockId block, const Update& update) {
  RETURN_IF_ERROR(CheckState());
  return impl_->Add(block, update);
}

absl::StatusOr<bool> SqliteArchive::Exists(BlockId block,
                                           const Address& account) {
  RETURN_IF_ERROR(CheckState());
  return impl_->Exists(block, account);
}

absl::StatusOr<Balance> SqliteArchive::GetBalance(BlockId block,
                                                  const Address& account) {
  RETURN_IF_ERROR(CheckState());
  return impl_->GetBalance(block, account);
}

absl::StatusOr<Code> SqliteArchive::GetCode(BlockId block,
                                            const Address& account) {
  RETURN_IF_ERROR(CheckState());
  return impl_->GetCode(block, account);
}

absl::StatusOr<Nonce> SqliteArchive::GetNonce(BlockId block,
                                              const Address& account) {
  RETURN_IF_ERROR(CheckState());
  return impl_->GetNonce(block, account);
}

absl::StatusOr<Value> SqliteArchive::GetStorage(BlockId block,
                                                const Address& account,
                                                const Key& key) {
  RETURN_IF_ERROR(CheckState());
  return impl_->GetStorage(block, account, key);
}

absl::StatusOr<BlockId> SqliteArchive::GetLatestBlock() {
  RETURN_IF_ERROR(CheckState());
  return impl_->GetLastBlockHeight();
}

absl::StatusOr<Hash> SqliteArchive::GetHash(BlockId block) {
  RETURN_IF_ERROR(CheckState());
  return impl_->GetHash(block);
}

absl::StatusOr<std::vector<Address>> SqliteArchive::GetAccountList(
    BlockId block) {
  RETURN_IF_ERROR(CheckState());
  return impl_->GetAccountList(block);
}

absl::StatusOr<Hash> SqliteArchive::GetAccountHash(BlockId block,
                                                   const Address& account) {
  RETURN_IF_ERROR(CheckState());
  return impl_->GetAccountHash(block, account);
}

absl::Status SqliteArchive::Verify(
    BlockId block, const Hash& expected_hash,
    absl::FunctionRef<void(std::string_view)> progress_callback) {
  RETURN_IF_ERROR(CheckState());
  return impl_->Verify(block, expected_hash, progress_callback);
}

absl::Status SqliteArchive::VerifyAccount(BlockId block,
                                          const Address& account) const {
  RETURN_IF_ERROR(CheckState());
  return impl_->VerifyAccount(block, account);
}

absl::Status SqliteArchive::Flush() {
  if (!impl_) return absl::OkStatus();
  return impl_->Flush();
}

absl::Status SqliteArchive::Close() {
  if (!impl_) return absl::OkStatus();
  auto result = impl_->Close();
  impl_ = nullptr;
  return result;
}

MemoryFootprint SqliteArchive::GetMemoryFootprint() const {
  MemoryFootprint res(*this);
  if (impl_) {
    res.Add("impl", impl_->GetMemoryFootprint());
  }
  return res;
}

absl::Status SqliteArchive::CheckState() const {
  if (impl_) return absl::OkStatus();
  return absl::FailedPreconditionError("Archive not connected to DB.");
}

}  // namespace carmen::archive::sqlite
