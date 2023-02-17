#include "archive/leveldb/archive.h"

#include <filesystem>
#include <limits>
#include <memory>
#include <span>
#include <type_traits>

#include "absl/base/attributes.h"
#include "absl/container/btree_map.h"
#include "absl/strings/str_format.h"
#include "archive/leveldb/keys.h"
#include "backend/common/leveldb/leveldb.h"
#include "common/byte_util.h"
#include "common/hash.h"
#include "common/status_util.h"

namespace carmen::archive::leveldb {

using ::carmen::backend::LDBEntry;
using ::carmen::backend::LevelDb;

namespace internal {

class Archive {
 public:
  static absl::StatusOr<std::unique_ptr<Archive>> Open(
      const std::filesystem::path directory) {
    ASSIGN_OR_RETURN(auto db, LevelDb::Open(directory));
    return std::unique_ptr<Archive>(new Archive(std::move(db)));
  }

  absl::Status Add(BlockId block, const Update& update) {
    // TODO: use a batch insert.

    ASSIGN_OR_RETURN(std::int64_t latest, GetLatestBlock());
    if (std::int64_t(block) <= latest) {
      return absl::InternalError(absl::StrFormat(
          "Unable to insert block %d, archive already contains block %d", block,
          latest));
    }

    // Empty updates are ignored, no hashes are altered.
    if (update.Empty()) {
      return absl::OkStatus();
    }

    // Compute hashes of account updates.
    absl::btree_map<Address, Hash> diff_hashes;
    for (const auto& [addr, diff] : AccountUpdate::From(update)) {
      diff_hashes[addr] = diff.GetHash();
    }

    for (const auto& addr : update.GetDeletedAccounts()) {
      ASSIGN_OR_RETURN((auto state), GetAccountState(block, addr));
      state.exists = false;
      state.reincarnation_number++;
      RETURN_IF_ERROR(db_.Add({GetAccountKey(addr, block), state.Encode()}));
    }

    for (const auto& addr : update.GetCreatedAccounts()) {
      ASSIGN_OR_RETURN((auto state), GetAccountState(block, addr));
      state.exists = true;
      state.reincarnation_number++;
      RETURN_IF_ERROR(db_.Add({GetAccountKey(addr, block), state.Encode()}));
    }

    for (const auto& [addr, balance] : update.GetBalances()) {
      RETURN_IF_ERROR(db_.Add({GetBalanceKey(addr, block), AsChars(balance)}));
    }

    for (const auto& [addr, code] : update.GetCodes()) {
      RETURN_IF_ERROR(db_.Add(
          {GetCodeKey(addr, block),
           std::span<const char>(reinterpret_cast<const char*>(code.Data()),
                                 code.Size())}));
    }

    for (const auto& [addr, nonce] : update.GetNonces()) {
      RETURN_IF_ERROR(db_.Add({GetNonceKey(addr, block), AsChars(nonce)}));
    }

    for (const auto& [addr, key, value] : update.GetStorage()) {
      ASSIGN_OR_RETURN((auto [_, r]), GetAccountState(block, addr));
      RETURN_IF_ERROR(
          db_.Add({GetStorageKey(addr, r, key, block), AsChars(value)}));
    }

    Sha256Hasher hasher;
    ASSIGN_OR_RETURN(auto last_block_hash, GetHash(block));
    hasher.Ingest(last_block_hash);

    for (auto& [addr, hash] : diff_hashes) {
      ASSIGN_OR_RETURN(auto last_hash, GetAccountHash(block, addr));
      auto new_hash = GetSha256Hash(last_hash, hash);
      RETURN_IF_ERROR(
          db_.Add({GetAccountHashKey(addr, block), AsChars(new_hash)}));
      hasher.Ingest(new_hash);
    }

    RETURN_IF_ERROR(db_.Add({GetBlockKey(block), AsChars(hasher.GetHash())}));

    return absl::OkStatus();
  }

  absl::StatusOr<bool> Exists(BlockId block, const Address& address) {
    ASSIGN_OR_RETURN((auto [exists, _]), GetAccountState(block, address));
    return exists;
  }

  absl::StatusOr<Balance> GetBalance(BlockId block, const Address& address) {
    return FindMostRecentFor<Balance>(block, GetBalanceKey(address, block));
  }

  absl::StatusOr<Code> GetCode(BlockId block, const Address& address) {
    return FindMostRecentFor<Code>(block, GetCodeKey(address, block));
  }

  absl::StatusOr<Nonce> GetNonce(BlockId block, const Address& address) {
    return FindMostRecentFor<Nonce>(block, GetNonceKey(address, block));
  }

  absl::StatusOr<Value> GetStorage(BlockId block, const Address& address,
                                   const Key& key) {
    ASSIGN_OR_RETURN((auto [_, r]), GetAccountState(block, address));
    return FindMostRecentFor<Value>(block,
                                    GetStorageKey(address, r, key, block));
  }

  // Gets the maximum block height insert so far, returns -1 if there is none.
  absl::StatusOr<std::int64_t> GetLatestBlock() {
    BlockId max_block = std::numeric_limits<BlockId>::max();
    auto key = GetBlockKey(max_block);
    ASSIGN_OR_RETURN(auto iter, db_.GetLowerBound(key));
    if (iter.IsEnd()) {
      RETURN_IF_ERROR(iter.Prev());
    } else if (Equal(key, iter.Key())) {
      return max_block;
    } else {
      RETURN_IF_ERROR(iter.Prev());
    }
    if (iter.IsBegin()) {
      return -1;
    }
    auto got = iter.Key();
    if (key.size() != got.size() || key[0] != got[0]) {
      return -1;
    }
    return GetBlockFromKey(got);
  }

  absl::StatusOr<Hash> GetHash(BlockId block) {
    return FindMostRecentFor<Hash>(block, GetBlockKey(block));
  }

  absl::StatusOr<std::vector<Address>> GetAccountList(BlockId block) {
    std::vector<Address> result;
    auto min_key = GetAccountHashKey(Address{}, 0);
    ASSIGN_OR_RETURN(auto iter, db_.GetLowerBound(min_key));
    while (!iter.IsEnd() && iter.Key()[0] == min_key[0]) {
      auto current_block = GetBlockFromKey(iter.Key());
      const Address* current =
          reinterpret_cast<const Address*>(iter.Key().data() + 1);
      if (current_block <= block &&
          (result.empty() || result.back() != *current)) {
        result.push_back(*current);
      }
      RETURN_IF_ERROR(iter.Next());
    }
    return result;
  }

  absl::StatusOr<Hash> GetAccountHash(BlockId block, const Address& address) {
    return FindMostRecentFor<Hash>(block, GetAccountHashKey(address, block));
  }

  absl::Status Verify(BlockId, const Hash&,
                      absl::FunctionRef<void(std::string_view)>) {
    return absl::UnimplementedError("to be implemented");
  }

  absl::Status VerifyAccount(BlockId, const Address&) const {
    return absl::UnimplementedError("to be implemented");
  }

  absl::Status Flush() { return db_.Flush(); }

  absl::Status Close() { return db_.Close(); }

 private:
  Archive(LevelDb db) : db_(std::move(db)) {}

  // Utility function to compare two spans of charaters for equaltity.
  static bool Equal(std::span<const char> a, std::span<const char> b) {
    return a.size() == b.size() &&
           std::memcmp(a.data(), b.data(), a.size()) == 0;
  }

  // A utility function to locate the value mapped to the given key, or, if not
  // present, the value mapped to the same key with the next smaller block
  // number. If there is no such entry, the default value is returned.
  template <typename Value>
  absl::StatusOr<Value> FindMostRecentFor(BlockId block,
                                          std::span<const char> key) {
    ASSIGN_OR_RETURN(auto iter, db_.GetLowerBound(key));
    if (iter.IsEnd()) {
      RETURN_IF_ERROR(iter.Prev());
    } else {
      if (!Equal(key, iter.Key())) {
        RETURN_IF_ERROR(iter.Prev());
      }
    }
    if (!iter.Valid() || iter.Key().size() != key.size()) {
      return Value{};
    }

    auto want_without_block = key.subspan(0, key.size() - kBlockIdSize);
    auto have_without_block = iter.Key().subspan(0, key.size() - kBlockIdSize);
    if (block < GetBlockFromKey(iter.Key()) ||
        !Equal(want_without_block, have_without_block)) {
      return Value{};
    }

    auto expected_size = std::is_same_v<Value, AccountState>
                             ? sizeof(AccountState().Encode())
                             : sizeof(Value);
    if (!std::is_same_v<Value, Code> && iter.Value().size() != expected_size) {
      return absl::InternalError("stored value has wrong format");
    }

    Value result;
    result.SetBytes(std::as_bytes(iter.Value()));
    return result;
  }

  absl::StatusOr<AccountState> GetAccountState(BlockId block,
                                               const Address& account) {
    return FindMostRecentFor<AccountState>(block,
                                           GetAccountKey(account, block));
  }

  LevelDb db_;
};

}  // namespace internal

LevelDbArchive::LevelDbArchive(LevelDbArchive&&) = default;

LevelDbArchive::LevelDbArchive(std::unique_ptr<internal::Archive> archive)
    : impl_(std::move(archive)){};

LevelDbArchive& LevelDbArchive::operator=(LevelDbArchive&&) = default;

LevelDbArchive::~LevelDbArchive() { Close().IgnoreError(); };

absl::StatusOr<LevelDbArchive> LevelDbArchive::Open(
    std::filesystem::path directory) {
  // TODO: create directory if it does not exist.
  auto path = directory;
  if (std::filesystem::is_directory(directory)) {
    path = path / "archive.sqlite";
  }
  ASSIGN_OR_RETURN(auto impl, internal::Archive::Open(path));
  return LevelDbArchive(std::move(impl));
}

absl::Status LevelDbArchive::Add(BlockId block, const Update& update) {
  RETURN_IF_ERROR(CheckState());
  return impl_->Add(block, update);
}

absl::StatusOr<bool> LevelDbArchive::Exists(BlockId block,
                                            const Address& account) {
  RETURN_IF_ERROR(CheckState());
  return impl_->Exists(block, account);
}

absl::StatusOr<Balance> LevelDbArchive::GetBalance(BlockId block,
                                                   const Address& account) {
  RETURN_IF_ERROR(CheckState());
  return impl_->GetBalance(block, account);
}

absl::StatusOr<Code> LevelDbArchive::GetCode(BlockId block,
                                             const Address& account) {
  RETURN_IF_ERROR(CheckState());
  return impl_->GetCode(block, account);
}

absl::StatusOr<Nonce> LevelDbArchive::GetNonce(BlockId block,
                                               const Address& account) {
  RETURN_IF_ERROR(CheckState());
  return impl_->GetNonce(block, account);
}

absl::StatusOr<Value> LevelDbArchive::GetStorage(BlockId block,
                                                 const Address& account,
                                                 const Key& key) {
  RETURN_IF_ERROR(CheckState());
  return impl_->GetStorage(block, account, key);
}

absl::StatusOr<BlockId> LevelDbArchive::GetLatestBlock() {
  RETURN_IF_ERROR(CheckState());
  return impl_->GetLatestBlock();
}

absl::StatusOr<Hash> LevelDbArchive::GetHash(BlockId block) {
  RETURN_IF_ERROR(CheckState());
  return impl_->GetHash(block);
}

absl::StatusOr<std::vector<Address>> LevelDbArchive::GetAccountList(
    BlockId block) {
  RETURN_IF_ERROR(CheckState());
  return impl_->GetAccountList(block);
}

absl::StatusOr<Hash> LevelDbArchive::GetAccountHash(BlockId block,
                                                    const Address& account) {
  RETURN_IF_ERROR(CheckState());
  return impl_->GetAccountHash(block, account);
}

absl::Status LevelDbArchive::Verify(
    BlockId block, const Hash& expected_hash,
    absl::FunctionRef<void(std::string_view)> progress_callback) {
  RETURN_IF_ERROR(CheckState());
  return impl_->Verify(block, expected_hash, progress_callback);
}

absl::Status LevelDbArchive::VerifyAccount(BlockId block,
                                           const Address& account) const {
  RETURN_IF_ERROR(CheckState());
  return impl_->VerifyAccount(block, account);
}

absl::Status LevelDbArchive::Flush() {
  if (!impl_) return absl::OkStatus();
  return impl_->Flush();
}

absl::Status LevelDbArchive::Close() {
  if (!impl_) return absl::OkStatus();
  auto result = impl_->Close();
  impl_ = nullptr;
  return result;
}

absl::Status LevelDbArchive::CheckState() const {
  if (impl_) return absl::OkStatus();
  return absl::FailedPreconditionError("Archive not connected to DB.");
}

}  // namespace carmen::archive::leveldb
