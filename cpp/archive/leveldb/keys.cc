/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use of
 * this software will be governed by the GNU Lesser General Public Licence v3.
 */

#include "archive/leveldb/keys.h"

#include <array>
#include <cstring>
#include <span>

#include "archive/leveldb/encoding.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen::archive::leveldb {

std::string_view ToString(KeyType type) {
  switch (type) {
    case KeyType::kAccountState:
      return "account_state";
    case KeyType::kAccountHash:
      return "account_hash";
    case KeyType::kBlock:
      return "block";
    case KeyType::kBalance:
      return "balance";
    case KeyType::kCode:
      return "code";
    case KeyType::kNonce:
      return "nonce";
    case KeyType::kStorage:
      return "storage";
  }
  return "unknown";
}

namespace {

template <std::size_t offset, std::size_t count, typename T, std::size_t Extend>
std::span<T, count> subspan(std::array<T, Extend>& array) {
  static_assert(Extend >= offset + count);
  return std::span<T, count>(&array[offset], count);
}

template <std::size_t offset, std::size_t count, typename T, std::size_t Extend>
std::span<const T, count> subspan(const std::array<T, Extend>& array) {
  static_assert(Extend >= offset + count);
  return std::span<const T, count>(&array[offset], count);
}

template <KeyType type, typename Key>
Key Get(const Address& address, BlockId block) {
  Key res;
  res[0] = static_cast<char>(type);
  Write(address, subspan<1, 20>(res));
  Write(block, subspan<1 + 20, 4>(res));
  return res;
}

}  // namespace

BlockKey GetBlockKey(BlockId block) {
  BlockKey res;
  res[0] = static_cast<char>(KeyType::kBlock);
  Write(block, subspan<1, 4>(res));
  return res;
}

AccountStateKey GetAccountStateKey(const Address& address, BlockId block) {
  return Get<KeyType::kAccountState, AccountStateKey>(address, block);
}

BalanceKey GetBalanceKey(const Address& address, BlockId block) {
  return Get<KeyType::kBalance, BalanceKey>(address, block);
}

CodeKey GetCodeKey(const Address& address, BlockId block) {
  return Get<KeyType::kCode, CodeKey>(address, block);
}

NonceKey GetNonceKey(const Address& address, BlockId block) {
  return Get<KeyType::kNonce, NonceKey>(address, block);
}

AccountHashKey GetAccountHashKey(const Address& address, BlockId block) {
  return Get<KeyType::kAccountHash, AccountHashKey>(address, block);
}

StorageKey GetStorageKey(const Address& address,
                         ReincarnationNumber reincarnation, const Key& key,
                         BlockId block) {
  StorageKey res;
  res[0] = static_cast<char>(KeyType::kStorage);
  Write(address, subspan<1, 20>(res));
  Write(reincarnation, subspan<1 + 20, 4>(res));
  Write(key, subspan<1 + 20 + 4, 32>(res));
  Write(block, subspan<1 + 20 + 4 + 32, 4>(res));
  return res;
}

BlockId GetBlockFromKey(std::span<const char> key) {
  // The block ID is always stored in the last 4 bytes.
  assert(key.size() >= 4);
  if (key.size() < 4) return 0;
  return ReadUint32(std::span<const char, 4>(key.data() + key.size() - 4, 4));
}

std::span<const char> GetAccountPrefix(std::span<const char> key) {
  return std::span(key).subspan(0, 1 + sizeof(Address));
}

const Address& GetAddressFromKey(std::span<const char> key) {
  assert(key.size() >= 21);
  return *reinterpret_cast<const Address*>(key.data() + 1);
}

ReincarnationNumber GetReincarnationNumber(const StorageKey& key) {
  return ReadUint32(subspan<1 + 20, 4>(key));
}

const Key& GetSlotKey(const StorageKey& key) {
  return Read<Key>(subspan<1 + 20 + 4, 32>(key));
}

}  // namespace carmen::archive::leveldb
