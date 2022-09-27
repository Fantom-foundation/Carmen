#pragma once

#include <cstdint>
#include <optional>
#include <utility>

#include "common/hash.h"
#include "common/type.h"

namespace carmen {

using Balance = std::uint64_t;
using Nonce = std::uint64_t;

// A state maintains all persistent state of the block chain. In particular
// it maintains the balance of accounts, accounts nonces, and storage.
//
// This implementation of the state can be parameterized by the implementation
// of index and store types, which are instantiated internally to form the
// data infrastructure required to maintain all necessary information.
template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType>
class State {
 public:

  Balance GetBalance(const Address& address) const {
    auto addr_id = address_index_.Get(address);
    if (!addr_id.has_value()) return 0;
    return balances_.Get(*addr_id);
  }

  void SetBalance(const Address& address, Balance value) {
    auto addr_id = address_index_.GetOrAdd(address);
    balances_.Set(addr_id, value);
  }

  Nonce GetNonce(const Address& address) const {
    auto addr_id = address_index_.Get(address);
    if (!addr_id.has_value()) return 0;
    return nonces_.Get(*addr_id);
  }

  void SetNonce(const Address& address, Nonce value) {
    auto addr_id = address_index_.GetOrAdd(address);
    nonces_.Set(addr_id, value);
  }

  // Obtains the current value of the given storage slot.
  const Value& GetStorageValue(const Address& address, const Key& key) const {
    auto addr_id = address_index_.Get(address);
    if (!addr_id.has_value()) return kDefaultValue;
    auto key_id = key_index_.Get(key);
    if (!key_id.has_value()) return kDefaultValue;
    Slot slot{*addr_id, *key_id};
    auto slot_id = slot_index_.Get(slot);
    if (!slot_id.has_value()) return kDefaultValue;
    return value_store_.Get(*slot_id);
  }

  // Updates the current value of the given storage slot.
  void SetStorageValue(const Address& address, const Key& key,
                       const Value& value) {
    auto addr_id = address_index_.GetOrAdd(address);
    auto key_id = key_index_.GetOrAdd(key);
    Slot slot{addr_id, key_id};
    auto slot_id = slot_index_.GetOrAdd(slot);
    value_store_.Set(slot_id, value);
  }

  // Obtains a state hash providing a unique cryptographic fingerprint of the
  // entire maintained state.
  Hash GetHash() const {
    return GetSha256Hash(address_index_.GetHash(), key_index_.GetHash(),
                         slot_index_.GetHash(), balances_.GetHash(),
                         nonces_.GetHash(), value_store_.GetHash());
  }

 private:
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

  // The value to be returned for any uninitialized storage location.
  constexpr static const Value kDefaultValue;

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
};

}  // namespace carmen
