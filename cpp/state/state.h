#pragma once

#include <cstdint>
#include <optional>
#include <utility>

#include "common/hash.h"
#include "common/type.h"

namespace carmen {

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

  State() = default;

  State(IndexType<Address, AddressId> address_index,
        IndexType<Key, KeyId> key_index, IndexType<Slot, SlotId> slot_index,
        StoreType<AddressId, Balance> balances,
        StoreType<AddressId, Nonce> nonces,
        StoreType<SlotId, Value> value_store)
      : address_index_(std::move(address_index)),
        key_index_(std::move(key_index)),
        slot_index_(std::move(slot_index)),
        balances_(std::move(balances)),
        nonces_(std::move(nonces)),
        value_store_(std::move(value_store)) {}

  const Balance& GetBalance(const Address& address) const {
    constexpr static const Balance kZero;
    auto addr_id = address_index_.Get(address);
    if (!addr_id.has_value()) return kZero;
    return balances_.Get(*addr_id);
  }

  void SetBalance(const Address& address, Balance value) {
    auto addr_id = address_index_.GetOrAdd(address).first;
    balances_.Set(addr_id, value);
  }

  const Nonce& GetNonce(const Address& address) const {
    constexpr static const Nonce kZero;
    auto addr_id = address_index_.Get(address);
    if (!addr_id.has_value()) return kZero;
    return nonces_.Get(*addr_id);
  }

  void SetNonce(const Address& address, Nonce value) {
    auto addr_id = address_index_.GetOrAdd(address).first;
    nonces_.Set(addr_id, value);
  }

  // Obtains the current value of the given storage slot.
  const Value& GetStorageValue(const Address& address, const Key& key) const {
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

  // Updates the current value of the given storage slot.
  void SetStorageValue(const Address& address, const Key& key,
                       const Value& value) {
    auto addr_id = address_index_.GetOrAdd(address).first;
    auto key_id = key_index_.GetOrAdd(key).first;
    Slot slot{addr_id, key_id};
    auto slot_id = slot_index_.GetOrAdd(slot).first;
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
