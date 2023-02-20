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
using ::carmen::backend::LevelDbIterator;
using ::carmen::backend::LevelDbWriteBatch;

namespace internal {

namespace {

// Utility function to check whether one span is a prefix of another.
bool IsPrefix(std::span<const char> prefix, std::span<const char> value) {
  return prefix.size() <= value.size() &&
         std::memcmp(prefix.data(), value.data(), prefix.size()) == 0;
}

// Utility function to compare two spans of charaters for equaltity.
bool Equal(std::span<const char> a, std::span<const char> b) {
  return a.size() == b.size() && std::memcmp(a.data(), b.data(), a.size()) == 0;
}

// An abstract utility class to iterate over a range of key/value pairs in a
// LevelDB instance with a common prefix. It provides a common base type for
// iterating over ranges with fixed key/value format (see TypedKeyRangeIterator
// below).
class KeyRangeIterator {
 public:
  // True, if all elements in the range have been consumed.
  bool Finished() const { return finished_; }

  // Moves this iterator to the next element. If there is no more element in the
  // range, the iterator is marked as finished.
  absl::Status Next() {
    RETURN_IF_ERROR(iterator_.Next());
    UpdateFinishState();
    return absl::OkStatus();
  }

  // Retrieves the block number referenced by the current iterator position.
  virtual BlockId GetBlock() const = 0;

 protected:
  KeyRangeIterator(LevelDbIterator iter, std::span<const char> prefix)
      : iterator_(std::move(iter)), prefix_(prefix) {
    UpdateFinishState();
  }

  LevelDbIterator iterator_;

 private:
  void UpdateFinishState() {
    finished_ = iterator_.IsEnd() || !IsPrefix(prefix_, iterator_.Key());
  }

  std::span<const char> prefix_;
  bool finished_ = false;
};

// A Key range iterator for a specific type of key and value, simplifying the
// implementation of the verification of archives.
template <Trivial K, typename V>
class TypedKeyRangeIterator final : public KeyRangeIterator {
 public:
  // Creates a range for the given prefix in the DB.
  static absl::StatusOr<TypedKeyRangeIterator> Get(const LevelDb& db,
                                                   const K& example_key) {
    auto prefix = GetAccountPrefix(example_key);
    ASSIGN_OR_RETURN(auto iter, db.GetLowerBound(prefix));
    return TypedKeyRangeIterator(std::move(iter), prefix);
  }

  // The block the entry pointed to by the iterator is associated to. Invalid if
  // the iterator is finished.
  BlockId GetBlock() const override { return GetBlockFromKey(iterator_.Key()); }

  // Returns a length-checked view on the current key.
  StatusOrRef<const K> Key() const {
    auto key = iterator_.Key();
    if (key.size() != sizeof(K)) {
      return absl::InternalError(absl::StrFormat(
          "Invalid key length, expected %d, got %d", sizeof(K), key.size()));
    }
    return *reinterpret_cast<const K*>(key.data());
  }

  // Returns a length-checked view on the current value.
  absl::StatusOr<V> Value() const {
    auto value = iterator_.Value();
    if constexpr (std::is_same_v<V, AccountState>) {
      if (value.size() != sizeof(AccountState().Encode())) {
        return absl::InternalError(
            absl::StrFormat("Invalid value length, expected %d, got %d",
                            sizeof(AccountState().Encode()), value.size()));
      }
      return AccountState::From(std::as_bytes(value));
    } else if constexpr (std::is_same_v<V, Code>) {
      return Code(value);
    } else {
      static_assert(Trivial<V>);
      if (value.size() != sizeof(V)) {
        return absl::InternalError(
            absl::StrFormat("Invalid value length, expected %d, got %d",
                            sizeof(V), value.size()));
      }
      return *reinterpret_cast<const V*>(value.data());
    }
  }

 private:
  using KeyRangeIterator::KeyRangeIterator;
};

}  // namespace

class Archive {
 public:
  static absl::StatusOr<std::unique_ptr<Archive>> Open(
      const std::filesystem::path directory) {
    ASSIGN_OR_RETURN(auto db, LevelDb::Open(directory));
    return std::unique_ptr<Archive>(new Archive(std::move(db)));
  }

  absl::Status Add(BlockId block, const Update& update) {
    absl::MutexLock guard(&update_lock_);
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

    // Utility to fetch the latest reincarnation number of an account.
    auto get_reincarnation =
        [&](const Address& addr) -> absl::StatusOr<ReincarnationNumber> {
      auto pos = reincarnation_cache_.find(addr);
      if (pos != reincarnation_cache_.end()) {
        return pos->second;
      }
      ASSIGN_OR_RETURN((auto [_, r]), GetAccountState(block, addr));
      reincarnation_cache_[addr] = r;
      return r;
    };

    LevelDbWriteBatch batch;
    for (const auto& addr : update.GetDeletedAccounts()) {
      ASSIGN_OR_RETURN((auto state), GetAccountState(block, addr));
      state.exists = false;
      state.reincarnation_number++;
      reincarnation_cache_[addr] = state.reincarnation_number;
      batch.Put(GetAccountKey(addr, block), state.Encode());
    }

    for (const auto& addr : update.GetCreatedAccounts()) {
      ASSIGN_OR_RETURN((auto state), GetAccountState(block, addr));
      state.exists = true;
      state.reincarnation_number++;
      reincarnation_cache_[addr] = state.reincarnation_number;
      batch.Put(GetAccountKey(addr, block), state.Encode());
    }

    for (const auto& [addr, balance] : update.GetBalances()) {
      batch.Put(GetBalanceKey(addr, block), AsChars(balance));
    }

    for (const auto& [addr, code] : update.GetCodes()) {
      batch.Put(GetCodeKey(addr, block),
                std::span<const char>(
                    reinterpret_cast<const char*>(code.Data()), code.Size()));
    }

    for (const auto& [addr, nonce] : update.GetNonces()) {
      batch.Put(GetNonceKey(addr, block), AsChars(nonce));
    }

    for (const auto& [addr, key, value] : update.GetStorage()) {
      ASSIGN_OR_RETURN(auto r, get_reincarnation(addr));
      batch.Put(GetStorageKey(addr, r, key, block), AsChars(value));
    }

    Sha256Hasher hasher;
    ASSIGN_OR_RETURN(auto last_block_hash, GetHash(block));
    hasher.Ingest(last_block_hash);

    for (auto& [addr, hash] : diff_hashes) {
      ASSIGN_OR_RETURN(auto last_hash, GetAccountHash(block, addr));
      auto new_hash = GetSha256Hash(last_hash, hash);
      batch.Put(GetAccountHashKey(addr, block), AsChars(new_hash));
      hasher.Ingest(new_hash);
    }

    batch.Put(GetBlockKey(block), AsChars(hasher.GetHash()));

    return db_.Add(std::move(batch));
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

  absl::Status Verify(
      BlockId block, const Hash& expected_hash,
      absl::FunctionRef<void(std::string_view)> progress_callback) {
    // First, check the expected hash.
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
    progress_callback("checking for extra data not covered by hashes");
    ASSIGN_OR_RETURN(auto latest_block, GetLatestBlock());
    absl::flat_hash_set<Address> valid_accounts(accounts.begin(),
                                                accounts.end());
    for (KeyType type :
         {KeyType::kAccount, KeyType::kAccountHash, KeyType::kBalance,
          KeyType::kCode, KeyType::kNonce, KeyType::kStorage}) {
      char prefix = static_cast<char>(type);
      ASSIGN_OR_RETURN(auto iter, db_.GetLowerBound(std::span(&prefix, 1)));
      while (!iter.IsEnd() && iter.Key()[0] == prefix) {
        auto current_block = GetBlockFromKey(iter.Key());

        // Make sure there are no extra accounts included.
        if (current_block <= block) {
          auto& addr = GetAddressFromKey(iter.Key());
          if (!valid_accounts.contains(addr)) {
            return absl::InternalError(absl::StrFormat(
                "Found extra key/value pair with prefix `%c`.", prefix));
          }
        }

        // Make sure there is are no future blocks included.
        if (current_block > latest_block) {
          return absl::InternalError(absl::StrFormat(
              "Found entry of future block height with prefix `%c`.", prefix));
        }
        RETURN_IF_ERROR(iter.Next());
      }
    }

    // All checks have passed. DB is verified.
    return absl::OkStatus();
  }

  // Verifies the consistency of the stored full archive hashes up until (and
  // including) the given block number.
  absl::Status VerifyHashes(BlockId max_block) {
    // For the verification we need to have all account hashes indexed by block
    // height. However, the key store is sorted by account. Thus, we need to
    // create a temporary index. We place this currently in memory, if this
    // becomes a problem, a disk-backed index (e.g. Btree) will be required.

    // Used to index the diff hashes for each block, in order of the account
    // addresses.
    absl::btree_map<std::pair<BlockId, int>, Hash> account_hashes;

    {
      // Used to count the number of diffs per block.
      absl::btree_map<BlockId, int> num_diffs;
      char prefix = static_cast<char>(KeyType::kAccountHash);
      ASSIGN_OR_RETURN(auto iter, db_.GetLowerBound(std::span(&prefix, 1)));
      while (!iter.IsEnd() && iter.Key()[0] == prefix) {
        if (iter.Key().size() != sizeof(AccountHashKey)) {
          return absl::InternalError(
              "Invalid account hash key length encountered.");
        }
        if (iter.Value().size() != sizeof(Hash)) {
          return absl::InternalError(
              "Invalid account hash value length encountered.");
        }
        BlockId block = GetBlockFromKey(iter.Key());
        if (block <= max_block) {
          auto pos = num_diffs[block]++;
          account_hashes[{block, pos}].SetBytes(iter.Value());
        }
        RETURN_IF_ERROR(iter.Next());
      }
    }

    // Verify the block hash for each block.
    auto account_hash_iter = account_hashes.begin();

    Hash hash{};
    Sha256Hasher hasher;
    char prefix = static_cast<char>(KeyType::kBlock);
    ASSIGN_OR_RETURN(auto block_hash_iter,
                     db_.GetLowerBound(std::span(&prefix, 1)));
    while (!block_hash_iter.IsEnd() && block_hash_iter.Key()[0] == prefix) {
      if (block_hash_iter.Key().size() != sizeof(BlockKey)) {
        return absl::InternalError("Invalid block key length encountered.");
      }
      if (block_hash_iter.Value().size() != sizeof(Hash)) {
        return absl::InternalError("Invalid block value length encountered.");
      }

      auto current_block = GetBlockFromKey(block_hash_iter.Key());
      if (current_block > max_block) {
        break;
      }

      if (account_hash_iter == account_hashes.end() ||
          account_hash_iter->first.first != current_block) {
        return absl::InternalError(
            absl::StrFormat("No diffs found for block %d.", current_block));
      }

      // Re-compute hash for current block.
      hasher.Reset();
      hasher.Ingest(hash);
      while (account_hash_iter != account_hashes.end() &&
             account_hash_iter->first.first == current_block) {
        hasher.Ingest(account_hash_iter->second);
        account_hash_iter++;
      }
      hash = hasher.GetHash();

      Hash have;
      have.SetBytes(block_hash_iter.Value());
      if (hash != have) {
        return absl::InternalError(absl::StrFormat(
            "Validation of hash of block %d failed.", current_block));
      }

      RETURN_IF_ERROR(block_hash_iter.Next());
    }

    return absl::OkStatus();
  }

  absl::Status VerifyAccount(BlockId block, const Address& account) const {
    using ::carmen::backend::LevelDbIterator;

    // Open iterators on various account properties.
    auto account_hash_key = GetAccountHashKey(account, 0);
    ASSIGN_OR_RETURN(auto hash_iter,
                     (TypedKeyRangeIterator<AccountHashKey, Hash>::Get(
                         db_, account_hash_key)));

    auto state_key = GetAccountKey(account, 0);
    ASSIGN_OR_RETURN(
        auto state_iter,
        (TypedKeyRangeIterator<AccountKey, AccountState>::Get(db_, state_key)));

    auto balance_key = GetBalanceKey(account, 0);
    ASSIGN_OR_RETURN(
        auto balance_iter,
        (TypedKeyRangeIterator<BalanceKey, Balance>::Get(db_, balance_key)));

    auto nonce_key = GetNonceKey(account, 0);
    ASSIGN_OR_RETURN(
        auto nonce_iter,
        (TypedKeyRangeIterator<NonceKey, Nonce>::Get(db_, nonce_key)));

    auto code_key = GetCodeKey(account, 0);
    ASSIGN_OR_RETURN(auto code_iter, (TypedKeyRangeIterator<CodeKey, Code>::Get(
                                         db_, code_key)));

    // Storage data is stored in DB using [account,reincarnation,key,block]
    // order, but for the verification we need it in [account,block,key] order.
    absl::btree_map<std::pair<BlockId, Key>,
                    std::pair<ReincarnationNumber, Value>>
        storage_data;

    {
      auto storage_key = GetStorageKey(account, 0, Key{}, 0);
      ASSIGN_OR_RETURN(
          auto storage_iter,
          (TypedKeyRangeIterator<StorageKey, Value>::Get(db_, storage_key)));

      while (!storage_iter.Finished()) {
        ASSIGN_OR_RETURN(StorageKey storage_key, storage_iter.Key());
        auto current_block = GetBlockFromKey(storage_key);
        if (current_block <= block) {
          auto key = GetSlotKey(storage_key);
          auto reincarnation = GetReincarnationNumber(storage_key);
          ASSIGN_OR_RETURN(Value value, storage_iter.Value());
          storage_data[{current_block, key}] = {reincarnation, value};
        }
        RETURN_IF_ERROR(storage_iter.Next());
      }
    }
    auto storage_iter = storage_data.begin();
    auto storage_iter_end = storage_data.end();

    KeyRangeIterator* property_iterators[] = {&state_iter, &balance_iter,
                                              &nonce_iter, &code_iter};

    // Find the first block referencing the account.
    auto get_next_block = [&]() -> BlockId {
      BlockId next = block + 1;
      for (KeyRangeIterator* iter : property_iterators) {
        if (!iter->Finished()) {
          next = std::min<BlockId>(next, iter->GetBlock());
        }
      }
      if (storage_iter != storage_iter_end) {
        next = std::min<BlockId>(next, storage_iter->first.first);
      }
      return next;
    };
    BlockId next = get_next_block();

    // Keep track of the reincarnation number.
    ReincarnationNumber reincarnation = 0;

    Hash hash{};
    std::optional<BlockId> last;
    while (next <= block) {
      BlockId current = next;
      if (last.has_value() && current <= last) {
        // This should only be possible if if the DB is corrupted and has
        // multiple identical keys ore keys out-of-order.
        return absl::InternalError(absl::StrFormat(
            "Corrupted DB: multiple updates for block %d found", current));
      }
      last = current;

      // --- Recreate Update for Current Block ---
      AccountUpdate update;

      if (!state_iter.Finished() && state_iter.GetBlock() == current) {
        ASSIGN_OR_RETURN(auto state, state_iter.Value());
        if (state.exists) {
          update.created = true;
        } else {
          update.deleted = true;
        }
        auto new_reincarnation_number = state.reincarnation_number;
        if (new_reincarnation_number != reincarnation + 1) {
          return absl::InternalError(absl::StrFormat(
              "Reincarnation numbers are not incremental, at block %d the "
              "value moves from %d to %d",
              current, reincarnation, new_reincarnation_number));
        }
        reincarnation = new_reincarnation_number;
        RETURN_IF_ERROR(state_iter.Next());
      }

      if (!balance_iter.Finished() && balance_iter.GetBlock() == current) {
        ASSIGN_OR_RETURN(update.balance, balance_iter.Value());
        RETURN_IF_ERROR(balance_iter.Next());
      }

      if (!nonce_iter.Finished() && nonce_iter.GetBlock() == current) {
        ASSIGN_OR_RETURN(update.nonce, nonce_iter.Value());
        RETURN_IF_ERROR(nonce_iter.Next());
      }

      if (!code_iter.Finished() && code_iter.GetBlock() == current) {
        ASSIGN_OR_RETURN(update.code, code_iter.Value());
        RETURN_IF_ERROR(code_iter.Next());
      }

      while (storage_iter != storage_iter_end &&
             storage_iter->first.first == current) {
        ReincarnationNumber cur_reincarnation = storage_iter->second.first;
        if (cur_reincarnation != reincarnation) {
          return absl::InternalError(
              absl::StrFormat("Invalid reincarnation number for storage value "
                              "at block %d, expected %d, got %d",
                              current, reincarnation, cur_reincarnation));
        }
        Key key = storage_iter->first.second;
        Value value = storage_iter->second.second;
        update.storage.push_back({key, value});
        ++storage_iter;
      }

      // --- Check that the current update matches the current block ---

      // Check the update against the list of per-account hashes.
      if (hash_iter.Finished()) {
        return absl::InternalError(absl::StrFormat(
            "Archive contains update for block %d but no hash for it.",
            current));
      }
      BlockId diff_block = hash_iter.GetBlock();
      if (diff_block != current) {
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
      ASSIGN_OR_RETURN(Hash should, hash_iter.Value());
      if (hash != should) {
        return absl::InternalError(absl::StrFormat(
            "Hash for diff at block %d does not match.", current));
      }
      RETURN_IF_ERROR(hash_iter.Next());

      // Find next block to be processed.
      next = get_next_block();
    }

    // Check whether there are additional updates in the hash table.
    if (!hash_iter.Finished() && hash_iter.GetBlock() < block) {
      return absl::InternalError(absl::StrFormat(
          "DB contains hash for update on block %d but no data.",
          hash_iter.GetBlock()));
    }

    return absl::OkStatus();
  }

  absl::Status Flush() { return db_.Flush(); }

  absl::Status Close() { return db_.Close(); }

  MemoryFootprint GetMemoryFootprint() const {
    MemoryFootprint res(*this);
    res.Add("leveldb", db_.GetMemoryFootprint());
    return res;
  }

 private:
  Archive(LevelDb db) : db_(std::move(db)) {}

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

  // A cache holding the reincarnation number of all addresses at the latest
  // block height.
  absl::flat_hash_map<Address, ReincarnationNumber> reincarnation_cache_;

  // A mutex making sure that Archive updates are written with exclusive access
  // to the DB. This exclusive access is required to keep the internal
  // reincarnation cache in sync.
  absl::Mutex update_lock_;
};

}  // namespace internal

LevelDbArchive::LevelDbArchive(LevelDbArchive&&) = default;

LevelDbArchive::LevelDbArchive(std::unique_ptr<internal::Archive> archive)
    : impl_(std::move(archive)){};

LevelDbArchive& LevelDbArchive::operator=(LevelDbArchive&&) = default;

LevelDbArchive::~LevelDbArchive() { Close().IgnoreError(); };

absl::StatusOr<LevelDbArchive> LevelDbArchive::Open(
    std::filesystem::path directory) {
  ASSIGN_OR_RETURN(auto impl, internal::Archive::Open(directory));
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

MemoryFootprint LevelDbArchive::GetMemoryFootprint() const {
  MemoryFootprint res(*this);
  if (impl_) {
    res.Add("impl", impl_->GetMemoryFootprint());
  }
  return res;
}

absl::Status LevelDbArchive::CheckState() const {
  if (impl_) return absl::OkStatus();
  return absl::FailedPreconditionError("Archive not connected to DB.");
}

}  // namespace carmen::archive::leveldb
