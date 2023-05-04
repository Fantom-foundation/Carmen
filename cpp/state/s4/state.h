#pragma once

#include <cstdint>
#include <filesystem>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/container/flat_hash_set.h"
#include "archive/archive.h"
#include "backend/structure.h"
#include "common/account_state.h"
#include "common/hash.h"
#include "common/memory_usage.h"
#include "common/status_util.h"
#include "common/type.h"
#include "state/s4/nodes.h"
#include "state/schema.h"
#include "state/update.h"

namespace carmen::s4 {

// This implementation of a state uses an Merkle Patricia Trie (MPT) for
// retaining its information.
//
// Currently, the implementation is in-memory only. Configuration parameters are
// ignored.
template <typename Config>
class State {
 public:
  using Archive = typename Config::Archive;

  static constexpr Schema GetSchema() {
    return Schema();  // TODO: return schema details.
  }

  // Creates a new state by opening the content stored in the given directory.
  static absl::StatusOr<State> Open(const std::filesystem::path& directory,
                                    bool with_archive = false);

  State() = default;
  State(State&&) = default;

  absl::Status CreateAccount(const Address& address);

  absl::StatusOr<AccountState> GetAccountState(const Address& address) const;

  absl::Status DeleteAccount(const Address& address);

  absl::StatusOr<Balance> GetBalance(const Address& address) const;

  absl::Status SetBalance(const Address& address, Balance value);

  absl::StatusOr<Nonce> GetNonce(const Address& address) const;

  absl::Status SetNonce(const Address& address, Nonce value);

  // Obtains the current value of the given storage slot.
  absl::StatusOr<Value> GetStorageValue(const Address& address,
                                        const Key& key) const;

  // Updates the current value of the given storage slot.
  absl::Status SetStorageValue(const Address& address, const Key& key,
                               const Value& value);

  // Retrieve the code stored under the given address.
  absl::StatusOr<Code> GetCode(const Address& address) const;

  // Updates the code stored under the given address.
  absl::Status SetCode(const Address& address, std::span<const std::byte> code);

  // Retrieve the code size stored under the given address.
  absl::StatusOr<std::uint32_t> GetCodeSize(const Address& address) const;

  // Retrieves the hash of the code stored under the given address.
  absl::StatusOr<Hash> GetCodeHash(const Address& address) const;

  // Applies the given block updates to this state.
  absl::Status Apply(BlockId block, const Update& update);

  // Applies the changes of the provided update to the current state.
  absl::Status ApplyToState(const Update& update);

  // Retrieves a pointer to the owned archive or nullptr, if no archive is
  // maintained.
  Archive* GetArchive() { return archive_.get(); }

  // Obtains a state hash providing a unique cryptographic fingerprint of the
  // entire maintained state.
  absl::StatusOr<Hash> GetHash();

  // Syncs internally modified write-buffers to disk.
  absl::Status Flush();

  // Flushes the content of the state to disk and closes all resource
  // references. After the state has been closed, no more operations may be
  // performed on it.
  absl::Status Close();

  // Summarizes the memory usage of this state object.
  MemoryFootprint GetMemoryFootprint() const;

 protected:
  // A constant for the hash of the empty code.
  static const Hash kEmptyCodeHash;

  // The information stored per account.
  struct Account {
    // TODO: this is currently necessary since the state interface is demanding
    // it, but in general, there should not be a difference between an
    // non-existing and an empty (=default valued) account.
    bool exists = false;
    Nonce nonce{};
    Balance balance{};
    Hash code_hash{};
    NodeId state = NodeId::Empty();

    // TODO: this is a copy of the information stored in the values_ forrest.
    // Consider removing it to safe a bit of disk space.
    Hash state_hash{};

    bool operator==(const Account&) const = default;

    friend std::ostream& operator<<(std::ostream& out, const Account& account) {
      return out << "Account{" << account.exists << "," << account.nonce << ","
                 << account.balance << "," << account.state.GetIndex() << "}";
    }
  };

  struct AccountHasher {
    template<typename Hasher>
    void operator()(Hasher& hasher, const Account& account) const {
      hasher.template Ingest<std::uint8_t>(account.exists ? 1 : 0);
      hasher.Ingest(account.nonce);
      hasher.Ingest(account.balance);
      hasher.Ingest(account.code_hash);
      // account.state deliberately skipped!
      hasher.Ingest(account.state_hash);
    }
  };


  // Make the state constructor protected to prevent direct instantiation. The
  // state should be created by calling the static Open method. This allows
  // the state to be mocked in tests.
  State(MerklePatriciaTrie<Address, Account, AccountHasher> accounts,
        MerklePatriciaTrieForrest<Key, Value> values,
        std::unique_ptr<Archive> archive);

  // A single trie storing all accounts.
  MerklePatriciaTrie<Address, Account, AccountHasher> accounts_;

  // A forrest of tries storing account values.
  MerklePatriciaTrieForrest<Key, Value> values_;

  // A map of maintained codes.
  absl::flat_hash_map<Hash, Code> codes_;

  // A pointer to the optionally included archive.
  std::unique_ptr<Archive> archive_;

  // A set of accounts that need to be re-hashed.
  absl::flat_hash_set<Address> dirty_accounts_;
};

// ----------------------------- Definitions ----------------------------------

template <typename Config>
const Hash State<Config>::kEmptyCodeHash = GetKeccak256Hash({});

template <typename Config>
absl::StatusOr<State<Config>> State<Config>::Open(
    const std::filesystem::path& dir, bool with_archive) {
  backend::Context context;

  MerklePatriciaTrie<Address, Account, AccountHasher> accounts;
  MerklePatriciaTrieForrest<Key, Value> values;

  std::unique_ptr<Archive> archive;
  if (with_archive) {
    ASSIGN_OR_RETURN(auto instance, Archive::Open(dir / "archive"));
    archive = std::make_unique<Archive>(std::move(instance));
  }

  return State(std::move(accounts), std::move(values), std::move(archive));
}

template <typename Config>
State<Config>::State(MerklePatriciaTrie<Address, Account, AccountHasher> accounts,
                     MerklePatriciaTrieForrest<Key, Value> values,
                     std::unique_ptr<Archive> archive)
    : accounts_(std::move(accounts)),
      values_(std::move(values)),
      archive_(std::move(archive)) {}

template <typename Config>
absl::Status State<Config>::CreateAccount(const Address& address) {
  // Creating an account means reseting its state (which is deleting it).
  RETURN_IF_ERROR(DeleteAccount(address));
  Account account;
  account.exists = true;
  if (accounts_.Set(address, account)) {
    dirty_accounts_.insert(address);
  }
  return absl::OkStatus();
}

template <typename Config>
absl::StatusOr<AccountState> State<Config>::GetAccountState(
    const Address& address) const {
  // An account exists if its value is not empty.
  if (accounts_.Get(address) == Account{}) {
    return AccountState::kUnknown;
  }
  return AccountState::kExists;
}

template <typename Config>
absl::Status State<Config>::DeleteAccount(const Address& address) {
  Account account = accounts_.Get(address);
  if (account == Account{}) {
    return absl::OkStatus();
  }

  values_.RemoveTree(account.state);
  // TODO: remove code?
  accounts_.Set(address, Account{});
  dirty_accounts_.erase(address);
  return absl::OkStatus();
}

template <typename Config>
absl::StatusOr<Balance> State<Config>::GetBalance(
    const Address& address) const {
  return accounts_.Get(address).balance;
}

template <typename Config>
absl::Status State<Config>::SetBalance(const Address& address, Balance value) {
  Account account = accounts_.Get(address);
  account.balance = value;
  if (accounts_.Set(address, account)) {
    dirty_accounts_.insert(address);
  }
  return absl::OkStatus();
}

template <typename Config>
absl::StatusOr<Nonce> State<Config>::GetNonce(const Address& address) const {
  return accounts_.Get(address).nonce;
}

template <typename Config>
absl::Status State<Config>::SetNonce(const Address& address, Nonce value) {
  Account account = accounts_.Get(address);
  account.nonce = value;
  if (accounts_.Set(address, account)) {
    dirty_accounts_.insert(address);
  }
  return absl::OkStatus();
}

template <typename Config>
absl::StatusOr<Value> State<Config>::GetStorageValue(const Address& address,
                                                     const Key& key) const {
  NodeId root = accounts_.Get(address).state;
  return values_.Get(root, key);
}

template <typename Config>
absl::Status State<Config>::SetStorageValue(const Address& address,
                                            const Key& key,
                                            const Value& value) {
  Account account = accounts_.Get(address);
  NodeId root = account.state;
  if (values_.Set(root, key, value)) {
    dirty_accounts_.insert(address);
  }
  if (root != account.state) {
    account.state = root;
    accounts_.Set(address, account);
  }
  return absl::OkStatus();
}

template <typename Config>
absl::StatusOr<Code> State<Config>::GetCode(const Address& address) const {
  auto account = accounts_.Get(address);
  if (!account.exists) {
    return Code{};
  }
  auto pos = codes_.find(account.code_hash);
  if (pos != codes_.end()) {
    return pos->second;
  }
  return Code{};
}

template <typename Config>
absl::Status State<Config>::SetCode(const Address& address,
                                    std::span<const std::byte> code) {
  auto code_hash = GetKeccak256Hash(code);
  auto account = accounts_.Get(address);
  if (account.code_hash == code_hash) {
    return absl::OkStatus();
  }
  codes_[code_hash] = code;
  account.exists = true;
  account.code_hash = code_hash;
  if (accounts_.Set(address, account)) {
    dirty_accounts_.insert(address);
  }
  return absl::OkStatus();
}

template <typename Config>
absl::StatusOr<std::uint32_t> State<Config>::GetCodeSize(
    const Address& address) const {
  auto account = accounts_.Get(address);
  auto pos = codes_.find(account.code_hash);
  if (pos == codes_.end()) {
    return 0;
  }
  return pos->second.Size();
}

template <typename Config>
absl::StatusOr<Hash> State<Config>::GetCodeHash(const Address& address) const {
  auto account = accounts_.Get(address);
  if (account.exists) {
    return account.code_hash;
  }
  return kEmptyCodeHash;
}

template <typename Config>
absl::Status State<Config>::Apply(BlockId block, const Update& update) {
  // Add updates the current state only.
  RETURN_IF_ERROR(ApplyToState(update));
  // If there is an active archive, the update is also added to its log.
  if (archive_) {
    // TODO: run in background thread
    RETURN_IF_ERROR(archive_->Add(block, update));
  }
  return absl::OkStatus();
}

template <typename Config>
absl::Status State<Config>::ApplyToState(const Update& update) {
  // It is important to keep the update order.
  for (auto& addr : update.GetDeletedAccounts()) {
    RETURN_IF_ERROR(DeleteAccount(addr));
  }
  for (auto& addr : update.GetCreatedAccounts()) {
    RETURN_IF_ERROR(CreateAccount(addr));
  }
  for (auto& [addr, value] : update.GetBalances()) {
    RETURN_IF_ERROR(SetBalance(addr, value));
  }
  for (auto& [addr, value] : update.GetNonces()) {
    RETURN_IF_ERROR(SetNonce(addr, value));
  }
  for (auto& [addr, code] : update.GetCodes()) {
    RETURN_IF_ERROR(SetCode(addr, code));
  }
  for (auto& [addr, key, value] : update.GetStorage()) {
    RETURN_IF_ERROR(SetStorageValue(addr, key, value));
  }
  return absl::OkStatus();
}

template <typename Config>
absl::StatusOr<Hash> State<Config>::GetHash() {
  // Update state trie hash of all dirty accounts.
  for (const auto& addr : dirty_accounts_) {
    auto account = accounts_.Get(addr);
    account.state_hash = values_.GetHash(account.state);
  }
  dirty_accounts_.clear();

  // Compute hash of account trie.
  return accounts_.GetHash();
}

template <typename Config>
absl::Status State<Config>::Flush() {
  if (archive_) {
    RETURN_IF_ERROR(archive_->Flush());
  }
  return absl::OkStatus();
}

template <typename Config>
absl::Status State<Config>::Close() {
  if (archive_) {
    RETURN_IF_ERROR(archive_->Close());
  }
  return absl::OkStatus();
}

template <typename Config>
MemoryFootprint State<Config>::GetMemoryFootprint() const {
  MemoryFootprint res(*this);
  res.Add("accounts", accounts_.GetMemoryFootprint());
  res.Add("values", values_.GetMemoryFootprint());

  Memory code_size = SizeOf(codes_);
  for (auto& [_,code] : codes_) {
    code_size += Memory(code.Size());
  }
  res.Add("codes", code_size);

  if (archive_) {
    res.Add("archive", archive_->GetMemoryFootprint());
  }
  return res;
}

}  // namespace carmen::s4
