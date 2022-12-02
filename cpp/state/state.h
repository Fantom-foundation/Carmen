#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <utility>

#include "absl/status/statusor.h"
#include "backend/structure.h"
#include "common/account_state.h"
#include "common/hash.h"
#include "common/memory_usage.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen {

// A state maintains all persistent state of the block chain. In particular
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

  void CreateAccount(const Address& address);

  AccountState GetAccountState(const Address& address) const;

  void DeleteAccount(const Address& address);

  const Balance& GetBalance(const Address& address) const;

  void SetBalance(const Address& address, Balance value);

  const Nonce& GetNonce(const Address& address) const;

  void SetNonce(const Address& address, Nonce value);

  // Obtains the current value of the given storage slot.
  const Value& GetStorageValue(const Address& address, const Key& key) const;

  // Updates the current value of the given storage slot.
  void SetStorageValue(const Address& address, const Key& key,
                       const Value& value);

  // Retrieve the code stored under the given address.
  std::span<const std::byte> GetCode(const Address& address) const;

  // Updates the code stored under the given address.
  void SetCode(const Address& address, std::span<const std::byte> code);

  // Retrieve the code size stored under the given address.
  std::uint32_t GetCodeSize(const Address& address) const;

  // Retrieves the hash of the code stored under the given address.
  Hash GetCodeHash(const Address& address) const;

  // Obtains a state hash providing a unique cryptographic fingerprint of the
  // entire maintained state.
  Hash GetHash();

  // Syncs internally modified write-buffers to disk.
  void Flush();

  // Flushes the content of the state to disk and closes all resource
  // references. After the state has been closed, no more operations may be
  // performed on it.
  void Close();

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
void State<IndexType, StoreType, DepotType>::CreateAccount(
    const Address& address) {
  auto addr_id = address_index_.GetOrAdd(address);
  account_states_.Set(addr_id.first, AccountState::kExists);
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
AccountState State<IndexType, StoreType, DepotType>::GetAccountState(
    const Address& address) const {
  auto addr_id = address_index_.Get(address);
  if (!addr_id.has_value()) return AccountState::kUnknown;
  return account_states_.Get(*addr_id);
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
void State<IndexType, StoreType, DepotType>::DeleteAccount(
    const Address& address) {
  auto addr_id = address_index_.Get(address);
  if (!addr_id.has_value()) return;
  account_states_.Set(*addr_id, AccountState::kDeleted);
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
const Balance& State<IndexType, StoreType, DepotType>::GetBalance(
    const Address& address) const {
  constexpr static const Balance kZero;
  auto addr_id = address_index_.Get(address);
  if (!addr_id.has_value()) return kZero;
  return balances_.Get(*addr_id);
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
void State<IndexType, StoreType, DepotType>::SetBalance(const Address& address,
                                                        Balance value) {
  auto addr_id = address_index_.GetOrAdd(address).first;
  balances_.Set(addr_id, value);
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
const Nonce& State<IndexType, StoreType, DepotType>::GetNonce(
    const Address& address) const {
  constexpr static const Nonce kZero;
  auto addr_id = address_index_.Get(address);
  if (!addr_id.has_value()) return kZero;
  return nonces_.Get(*addr_id);
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
void State<IndexType, StoreType, DepotType>::SetNonce(const Address& address,
                                                      Nonce value) {
  auto addr_id = address_index_.GetOrAdd(address).first;
  nonces_.Set(addr_id, value);
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
const Value& State<IndexType, StoreType, DepotType>::GetStorageValue(
    const Address& address, const Key& key) const {
  constexpr static const Value kZero;
  auto addr_id = address_index_.Get(address);
  if (!addr_id.has_value()) return kZero;
  auto key_id = key_index_.Get(key);
  if (!key_id.has_value()) return kZero;
  Slot slot{*addr_id, *key_id};
  auto slot_id = slot_index_.Get(slot);
  if (!slot_id.has_value()) return kZero;
  return value_store_.Get(*slot_id);
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
void State<IndexType, StoreType, DepotType>::SetStorageValue(
    const Address& address, const Key& key, const Value& value) {
  auto addr_id = address_index_.GetOrAdd(address).first;
  auto key_id = key_index_.GetOrAdd(key).first;
  Slot slot{addr_id, key_id};
  auto slot_id = slot_index_.GetOrAdd(slot).first;
  value_store_.Set(slot_id, value);
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
std::span<const std::byte> State<IndexType, StoreType, DepotType>::GetCode(
    const Address& address) const {
  auto addr_id = address_index_.Get(address);
  if (!addr_id.has_value()) return {};
  auto code = codes_.Get(*addr_id);
  if (code.ok()) return *code;
  return {};
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
void State<IndexType, StoreType, DepotType>::SetCode(
    const Address& address, std::span<const std::byte> code) {
  auto addr_id = address_index_.GetOrAdd(address).first;
  codes_.Set(addr_id, code).IgnoreError();
  code_hashes_.Set(addr_id,
                   code.empty() ? kEmptyCodeHash : GetKeccak256Hash(code));
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
std::uint32_t State<IndexType, StoreType, DepotType>::GetCodeSize(
    const Address& address) const {
  auto addr_id = address_index_.Get(address);
  if (!addr_id.has_value()) return 0;
  auto size = codes_.GetSize(*addr_id);
  if (!size.ok()) return 0;
  return *size;
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
Hash State<IndexType, StoreType, DepotType>::GetCodeHash(
    const Address& address) const {
  auto addr_id = address_index_.Get(address);
  if (!addr_id.has_value()) return kEmptyCodeHash;
  auto res = code_hashes_.Get(*addr_id);
  // The default value of hashes in the store is the zero hash.
  // However, for empty codes, the hash of an empty code should
  // returned. The only exception would be the very unlikely
  // case where the hash of the stored code is indeed zero.
  if (res == Hash{} && GetCodeSize(address) == 0) {
    return kEmptyCodeHash;
  }
  return res;
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
Hash State<IndexType, StoreType, DepotType>::GetHash() {
  return GetSha256Hash(address_index_.GetHash(), key_index_.GetHash(),
                       slot_index_.GetHash(), balances_.GetHash(),
                       nonces_.GetHash(), value_store_.GetHash(),
                       account_states_.GetHash(), *codes_.GetHash());
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
void State<IndexType, StoreType, DepotType>::Flush() {
  address_index_.Flush();
  key_index_.Flush();
  slot_index_.Flush();
  account_states_.Flush();
  balances_.Flush();
  nonces_.Flush();
  value_store_.Flush();
  codes_.Flush().IgnoreError();  // until function returns error itself
  code_hashes_.Flush();
}

template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType>
void State<IndexType, StoreType, DepotType>::Close() {
  address_index_.Close();
  key_index_.Close();
  slot_index_.Close();
  account_states_.Close();
  balances_.Close();
  nonces_.Close();
  value_store_.Close();
  codes_.Close().IgnoreError();  // until function returns error itself
  code_hashes_.Close();
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
