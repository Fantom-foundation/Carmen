#pragma once

#include <cstddef>
#include <span>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "common/type.h"

namespace carmen {

// A BlockUpdate summarizes all the updates produced by processing a block in
// the chain. It is the unit of data used to update archives and to synchronize
// data between archive instances.
class Update {
 public:
  struct BalanceUpdate {
    Address account;
    Balance balance;
    friend auto operator<=>(const BalanceUpdate&,
                            const BalanceUpdate&) = default;
  };

  struct NonceUpdate {
    Address account;
    Nonce nonce;
    friend auto operator<=>(const NonceUpdate&, const NonceUpdate&) = default;
  };

  struct CodeUpdate {
    Address account;
    Code code;
    friend auto operator<=>(const CodeUpdate&, const CodeUpdate&) = default;
  };

  // The update of a slot.
  struct SlotUpdate {
    Address account;
    Key key;
    Value value;
    friend auto operator<=>(const SlotUpdate&, const SlotUpdate&) = default;
  };

  // --- Mutators ---

  // Adds the given account to the list of deleted accounts. May invalidate
  // views on deleted accounts.
  void Delete(const Address& account) { deleted_accounts_.push_back(account); }

  // Adds the given account to the list of created accounts. May invalidate
  // views on created accounts.
  void Create(const Address& account) { created_accounts_.push_back(account); }

  // Adds an update to the given balance. May invalidate views on balance
  // updates aquired using GetBlances().
  void Set(const Address& account, const Balance& balance) {
    balances_.push_back(BalanceUpdate{account, balance});
  }

  // Adds an update to the given nonce. May invalidate views on nonces
  // updates aquired using GetNonces().
  void Set(const Address& account, const Nonce& nonce) {
    nonces_.push_back(NonceUpdate{account, nonce});
  }

  // Adds an update to the given code. May invalidate views on codes
  // updates aquired using GetCodes().
  void Set(const Address& account, const Code& code) {
    codes_.push_back(CodeUpdate{account, code});
  }

  // Adds an update to the given storage value. May invalidate views on state
  // updates aquired using GetStorage().
  void Set(const Address& account, const Key& key, const Value& value) {
    storage_.push_back(SlotUpdate{account, key, value});
  }

  // --- Observers ---

  // Returns a span of deleted addresses, valid until the next modification or
  // the end of the life cycle of this update.
  const std::vector<Address>& GetDeletedAccounts() const {
    return deleted_accounts_;
  }

  // Returns a span of created addresses, valid until the next modification or
  // the end of the life cycle of this update.
  const std::vector<Address>& GetCreatedAccounts() const {
    return created_accounts_;
  }

  // Returns a span of balance updates, valid until the next modification or
  // the end of the life cycle of this update.
  std::span<const BalanceUpdate> GetBalances() const { return balances_; }

  // Returns a span of nonce updates, valid until the next modification or
  // the end of the life cycle of this update.
  std::span<const NonceUpdate> GetNonces() const { return nonces_; }

  // Returns a span of code updates, valid until the next modification or
  // the end of the life cycle of this update.
  std::span<const CodeUpdate> GetCodes() const { return codes_; }

  // Returns a span of storage updates, valid until the next modification or
  // the end of the life cycle of this update.
  std::span<const SlotUpdate> GetStorage() const { return storage_; }

  // --- Serialization ---

  // Parses the encoded update into an update object.
  static absl::StatusOr<Update> FromBytes(std::span<const std::byte> data);

  // Encodes this update into a byte string.
  absl::StatusOr<std::vector<std::byte>> ToBytes() const;

  // --- Hashing ---

  // Computes a cryptographic hash of this update.
  absl::StatusOr<Hash> GetHash() const;

  // --- Operators ---

  friend bool operator==(const Update&, const Update&) = default;

 private:
  // The list of accounts that should be deleted / cleared by this update.
  std::vector<Address> deleted_accounts_;

  // The list of accounts that should be created by this update. Note, accounts
  // may be deleted and (re-)created in the same update.
  std::vector<Address> created_accounts_;

  // The list of balance updates.
  std::vector<BalanceUpdate> balances_;

  // The list of nonce updates.
  std::vector<NonceUpdate> nonces_;

  // The list of code updates.
  std::vector<CodeUpdate> codes_;

  // Retains all storage modifications of slots.
  std::vector<SlotUpdate> storage_;
};

// An AccountUpdate combines the updates applied to a single account in one
// block. Its main intention is to u
struct AccountUpdate {
  // The update of a slot.
  struct SlotUpdate {
    Key key;
    Value value;
    friend auto operator<=>(const SlotUpdate&, const SlotUpdate&) = default;
    friend std::ostream& operator<<(std::ostream& out, const SlotUpdate&);
  };

  // Converts the provided update in a list of account updates. If the update
  // was normalized, the entries of the resulting list are normalized.
  static absl::flat_hash_map<Address, AccountUpdate> From(const Update& update);

  // --- Normalization ---

  // Checks whether this update is in normal form. In particular, it validates
  // that slot updates are in order and unique.
  absl::Status IsNormalized() const;

  // Attempts to normalize the content of this update by sorting slot updates.
  // Normalization fails if there are slot update duplciates. If normalization
  // fails, the update is in an undefined state and should be discarded.
  absl::Status Normalize();

  // --- Hashing ---

  // Computes a cryptographic hash of this update.
  Hash GetHash() const;

  // --- Operators ---

  friend bool operator==(const AccountUpdate&, const AccountUpdate&) = default;

  friend std::ostream& operator<<(std::ostream& out, const AccountUpdate&);

  bool deleted = false;
  bool created = false;
  std::optional<Balance> balance;
  std::optional<Nonce> nonce;
  std::optional<Code> code;
  std::vector<SlotUpdate> storage;
};

}  // namespace carmen
