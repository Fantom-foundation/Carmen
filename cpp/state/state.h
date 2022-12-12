#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/structure.h"
#include "common/account_state.h"
#include "common/hash.h"
#include "common/memory_usage.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen {

// A state maintains all persistent state of the blockchain. In particular,
// it maintains the balance of accounts, accounts nonces, and storage.
//
// This implementation of the state can be parameterized by the implementation
// of index and store types, which are instantiated internally to form the
// data infrastructure required to maintain all necessary information.
template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
class State {
 public:
  // The types used for internal indexing.
  using AddressId = std::uint32_t;
  using KeyId = std::uint32_t;
  using SlotId = std::uint32_t;

  // Identifies a single slot by its address/key values.
  struct Slot {
    AddressId address;
    KeyId key;

    friend auto operator<=>(const Slot& a, const Slot& b) = default;

    template <typename H>
    friend H AbslHashValue(H h, const Slot& l) {
      return H::combine(std::move(h), l.address, l.key);
    }
  };

  // Creates a new state by opening the content stored in the given directory.
  static absl::StatusOr<State> Open(const std::filesystem::path& directory);

  State() = default;
  State(State&&) = default;

  State(IndexType<Address, AddressId> address_index,
        IndexType<Key, KeyId> key_index, IndexType<Slot, SlotId> slot_index,
        StoreType<AddressId, Balance> balances,
        StoreType<AddressId, Nonce> nonces,
        StoreType<SlotId, Value> value_store,
        StoreType<AddressId, AccountState> account_states,
        DepotType<AddressId> codes, StoreType<AddressId, Hash> code_hashes);

  absl::Status CreateAccount(const Address& address);

  absl::StatusOr<AccountState> GetAccountState(const Address& address) const;

  absl::Status DeleteAccount(const Address& address);

  StatusOrRef<Balance> GetBalance(const Address& address) const;

  absl::Status SetBalance(const Address& address, Balance value);

  StatusOrRef<Nonce> GetNonce(const Address& address) const;

  absl::Status SetNonce(const Address& address, Nonce value);

  // Obtains the current value of the given storage slot.
  StatusOrRef<Value> GetStorageValue(const Address& address,
                                     const Key& key) const;

  // Updates the current value of the given storage slot.
  absl::Status SetStorageValue(const Address& address, const Key& key,
                               const Value& value);

  // Retrieve the code stored under the given address.
  absl::StatusOr<std::span<const std::byte>> GetCode(
      const Address& address) const;

  // Updates the code stored under the given address.
  absl::Status SetCode(const Address& address, std::span<const std::byte> code);

  // Retrieve the code size stored under the given address.
  absl::StatusOr<std::uint32_t> GetCodeSize(const Address& address) const;

  // Retrieves the hash of the code stored under the given address.
  absl::StatusOr<Hash> GetCodeHash(const Address& address) const;

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

 private:
  // A constant for the hash of the empty code.
  static const Hash kEmptyCodeHash;

  // Indexes for mapping address, keys, and slots to dense, numeric IDs.
  IndexType<Address, AddressId> address_index_;
  IndexType<Key, KeyId> key_index_;
  IndexType<Slot, SlotId> slot_index_;

  // A store retaining the current balance of all accounts.
  StoreType<AddressId, Balance> balances_;

  // A store retaining the current nonces of all accounts.
  StoreType<AddressId, Nonce> nonces_;

  // The store retaining all values for the covered storage slots.
  StoreType<SlotId, Value> value_store_;

  // The store retaining account state information.
  StoreType<AddressId, AccountState> account_states_;

  // The code depot to retain account contracts.
  DepotType<AddressId> codes_;

  // A store to retain code hashes.
  StoreType<AddressId, Hash> code_hashes_;
};

// ----------------------------- Definitions ----------------------------------

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
const Hash State<IndexType, StoreType, DepotType>::kEmptyCodeHash =
    GetKeccak256Hash({});

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
absl::StatusOr<State<IndexType, StoreType, DepotType>>
State<IndexType, StoreType, DepotType>::Open(const std::filesystem::path& dir) {
  backend::Context context;
  ASSIGN_OR_RETURN(auto address_index, (IndexType<Address, AddressId>::Open(
                                           context, dir / "addresses")));
  ASSIGN_OR_RETURN(auto key_index,
                   (IndexType<Key, KeyId>::Open(context, dir / "keys")));
  ASSIGN_OR_RETURN(auto slot_index,
                   (IndexType<Slot, SlotId>::Open(context, dir / "slots")));

  ASSIGN_OR_RETURN(auto balances, (StoreType<AddressId, Balance>::Open(
                                      context, dir / "balances")));
  ASSIGN_OR_RETURN(auto nonces, (StoreType<AddressId, Nonce>::Open(
                                    context, dir / "nonces")));
  ASSIGN_OR_RETURN(auto values,
                   (StoreType<SlotId, Value>::Open(context, dir / "values")));
  ASSIGN_OR_RETURN(auto account_state,
                   (StoreType<AddressId, AccountState>::Open(
                       context, dir / "account_states")));
  ASSIGN_OR_RETURN(auto code_hashes, (StoreType<AddressId, Hash>::Open(
                                         context, dir / "code_hashes")));

  ASSIGN_OR_RETURN(auto codes,
                   (DepotType<AddressId>::Open(context, dir / "codes")));

  return State(std::move(address_index), std::move(key_index),
               std::move(slot_index), std::move(balances), std::move(nonces),
               std::move(values), std::move(account_state), std::move(codes),
               std::move(code_hashes));
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
State<IndexType, StoreType, DepotType>::State(
    IndexType<Address, AddressId> address_index,
    IndexType<Key, KeyId> key_index, IndexType<Slot, SlotId> slot_index,
    StoreType<AddressId, Balance> balances, StoreType<AddressId, Nonce> nonces,
    StoreType<SlotId, Value> value_store,
    StoreType<AddressId, AccountState> account_states,
    DepotType<AddressId> codes, StoreType<AddressId, Hash> code_hashes)
    : address_index_(std::move(address_index)),
      key_index_(std::move(key_index)),
      slot_index_(std::move(slot_index)),
      balances_(std::move(balances)),
      nonces_(std::move(nonces)),
      value_store_(std::move(value_store)),
      account_states_(std::move(account_states)),
      codes_(std::move(codes)),
      code_hashes_(std::move(code_hashes)) {}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
absl::Status State<IndexType, StoreType, DepotType>::CreateAccount(
    const Address& address) {
  ASSIGN_OR_RETURN(auto addr_id, address_index_.GetOrAdd(address));
  account_states_.Set(addr_id.first, AccountState::kExists);
  return absl::OkStatus();
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
absl::StatusOr<AccountState>
State<IndexType, StoreType, DepotType>::GetAccountState(
    const Address& address) const {
  auto addr_id = address_index_.Get(address);
  if (absl::IsNotFound(addr_id.status())) {
    return AccountState::kUnknown;
  }
  RETURN_IF_ERROR(addr_id);
  return account_states_.Get(*addr_id);
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
absl::Status State<IndexType, StoreType, DepotType>::DeleteAccount(
    const Address& address) {
  auto addr_id = address_index_.Get(address);
  if (absl::IsNotFound(addr_id.status())) {
    return absl::OkStatus();
  }
  RETURN_IF_ERROR(addr_id);
  account_states_.Set(*addr_id, AccountState::kDeleted);
  return absl::OkStatus();
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
StatusOrRef<Balance> State<IndexType, StoreType, DepotType>::GetBalance(
    const Address& address) const {
  constexpr static const Balance kZero;
  auto addr_id = address_index_.Get(address);
  if (absl::IsNotFound(addr_id.status())) {
    return kZero;
  }
  RETURN_IF_ERROR(addr_id);
  return balances_.Get(*addr_id);
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
absl::Status State<IndexType, StoreType, DepotType>::SetBalance(
    const Address& address, Balance value) {
  ASSIGN_OR_RETURN(auto addr_id, address_index_.GetOrAdd(address));
  balances_.Set(addr_id.first, value);
  return absl::OkStatus();
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
StatusOrRef<Nonce> State<IndexType, StoreType, DepotType>::GetNonce(
    const Address& address) const {
  constexpr static const Nonce kZero;
  auto addr_id = address_index_.Get(address);
  if (absl::IsNotFound(addr_id.status())) {
    return kZero;
  }
  RETURN_IF_ERROR(addr_id);
  return nonces_.Get(*addr_id);
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
absl::Status State<IndexType, StoreType, DepotType>::SetNonce(
    const Address& address, Nonce value) {
  ASSIGN_OR_RETURN(auto addr_id, address_index_.GetOrAdd(address));
  nonces_.Set(addr_id.first, value);
  return absl::OkStatus();
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
StatusOrRef<Value> State<IndexType, StoreType, DepotType>::GetStorageValue(
    const Address& address, const Key& key) const {
  constexpr static const Value kZero;
  auto addr_id = address_index_.Get(address);
  if (absl::IsNotFound(addr_id.status())) {
    return kZero;
  }
  RETURN_IF_ERROR(addr_id);
  auto key_id = key_index_.Get(key);
  if (absl::IsNotFound(key_id.status())) {
    return kZero;
  }
  RETURN_IF_ERROR(key_id);
  Slot slot{*addr_id, *key_id};
  auto slot_id = slot_index_.Get(slot);
  if (absl::IsNotFound(slot_id.status())) {
    return kZero;
  }
  RETURN_IF_ERROR(slot_id);
  return value_store_.Get(*slot_id);
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
absl::Status State<IndexType, StoreType, DepotType>::SetStorageValue(
    const Address& address, const Key& key, const Value& value) {
  ASSIGN_OR_RETURN(auto addr_id, address_index_.GetOrAdd(address));
  ASSIGN_OR_RETURN(auto key_id, key_index_.GetOrAdd(key));
  Slot slot{addr_id.first, key_id.first};
  ASSIGN_OR_RETURN(auto slot_id, slot_index_.GetOrAdd(slot));
  value_store_.Set(slot_id.first, value);
  return absl::OkStatus();
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
absl::StatusOr<std::span<const std::byte>>
State<IndexType, StoreType, DepotType>::GetCode(const Address& address) const {
  constexpr static const std::span<const std::byte> kZero;
  auto addr_id = address_index_.Get(address);
  if (absl::IsNotFound(addr_id.status())) {
    return kZero;
  }
  RETURN_IF_ERROR(addr_id);
  auto code = codes_.Get(*addr_id);
  if (absl::IsNotFound(code.status())) {
    return kZero;
  }
  RETURN_IF_ERROR(code);
  return *code;
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
absl::Status State<IndexType, StoreType, DepotType>::SetCode(
    const Address& address, std::span<const std::byte> code) {
  ASSIGN_OR_RETURN(auto addr_id, address_index_.GetOrAdd(address));
  RETURN_IF_ERROR(codes_.Set(addr_id.first, code));
  code_hashes_.Set(addr_id.first,
                   code.empty() ? kEmptyCodeHash : GetKeccak256Hash(code));
  return absl::OkStatus();
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
absl::StatusOr<std::uint32_t>
State<IndexType, StoreType, DepotType>::GetCodeSize(
    const Address& address) const {
  constexpr static const std::uint32_t kZero = 0;
  auto addr_id = address_index_.Get(address);
  if (absl::IsNotFound(addr_id.status())) {
    return kZero;
  }
  RETURN_IF_ERROR(addr_id);
  auto size = codes_.GetSize(*addr_id);
  if (absl::IsNotFound(size.status())) {
    return kZero;
  }
  RETURN_IF_ERROR(size);
  return *size;
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
absl::StatusOr<Hash> State<IndexType, StoreType, DepotType>::GetCodeHash(
    const Address& address) const {
  auto addr_id = address_index_.Get(address);
  if (absl::IsNotFound(addr_id.status())) {
    return kEmptyCodeHash;
  }
  RETURN_IF_ERROR(addr_id);
  auto res = code_hashes_.Get(*addr_id);
  // The default value of hashes in the store is the zero hash.
  // However, for empty codes, the hash of an empty code should
  // be returned. The only exception would be the very unlikely
  // case where the hash of the stored code is indeed zero.
  ASSIGN_OR_RETURN(auto code_size, GetCodeSize(address));
  if (res == Hash{} && code_size == 0) {
    return kEmptyCodeHash;
  }
  return res;
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
absl::StatusOr<Hash> State<IndexType, StoreType, DepotType>::GetHash() {
  ASSIGN_OR_RETURN(auto addr_idx_hash, address_index_.GetHash());
  ASSIGN_OR_RETURN(auto key_idx_hash, key_index_.GetHash());
  ASSIGN_OR_RETURN(auto slot_idx_hash, slot_index_.GetHash());
  ASSIGN_OR_RETURN(auto bal_hash, balances_.GetHash());
  ASSIGN_OR_RETURN(auto nonces_hash, nonces_.GetHash());
  ASSIGN_OR_RETURN(auto val_store_hash, value_store_.GetHash());
  ASSIGN_OR_RETURN(auto acc_states_hash, account_states_.GetHash());
  ASSIGN_OR_RETURN(auto codes_hash, codes_.GetHash());
  return GetSha256Hash(addr_idx_hash, key_idx_hash, slot_idx_hash, bal_hash,
                       nonces_hash, val_store_hash, acc_states_hash,
                       codes_hash);
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
absl::Status State<IndexType, StoreType, DepotType>::Flush() {
  RETURN_IF_ERROR(address_index_.Flush());
  RETURN_IF_ERROR(key_index_.Flush());
  RETURN_IF_ERROR(slot_index_.Flush());
  RETURN_IF_ERROR(account_states_.Flush());
  RETURN_IF_ERROR(balances_.Flush());
  RETURN_IF_ERROR(nonces_.Flush());
  RETURN_IF_ERROR(value_store_.Flush());
  RETURN_IF_ERROR(codes_.Flush());
  RETURN_IF_ERROR(code_hashes_.Flush());
  return absl::OkStatus();
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
absl::Status State<IndexType, StoreType, DepotType>::Close() {
  RETURN_IF_ERROR(address_index_.Close());
  RETURN_IF_ERROR(key_index_.Close());
  RETURN_IF_ERROR(slot_index_.Close());
  RETURN_IF_ERROR(account_states_.Close());
  RETURN_IF_ERROR(balances_.Close());
  RETURN_IF_ERROR(nonces_.Close());
  RETURN_IF_ERROR(value_store_.Close());
  RETURN_IF_ERROR(codes_.Close());
  RETURN_IF_ERROR(code_hashes_.Close());
  return absl::OkStatus();
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
MemoryFootprint State<IndexType, StoreType, DepotType>::GetMemoryFootprint()
    const {
  MemoryFootprint res(*this);
  res.Add("address_index", address_index_.GetMemoryFootprint());
  res.Add("key_index", key_index_.GetMemoryFootprint());
  res.Add("slot_index", slot_index_.GetMemoryFootprint());
  res.Add("balances", balances_.GetMemoryFootprint());
  res.Add("nonces", nonces_.GetMemoryFootprint());
  res.Add("value_store", value_store_.GetMemoryFootprint());
  res.Add("codes", codes_.GetMemoryFootprint());
  res.Add("code_hashes", code_hashes_.GetMemoryFootprint());
  return res;
}

}  // namespace carmen
