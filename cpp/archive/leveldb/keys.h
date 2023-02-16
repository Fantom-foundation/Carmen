#pragma once

#include <array>
#include <span>

#include "absl/status/statusor.h"
#include "common/type.h"

namespace carmen::archive::leveldb {

using ReincarnationNumber = std::uint32_t;

static constexpr const std::size_t kBlockIdSize = 8;
static constexpr const std::size_t kPropertyKeySize =
    1 + sizeof(Address) + kBlockIdSize;

using PropertyKey = std::array<char, kPropertyKeySize>;

using BlockKey = std::array<char, 1 + kBlockIdSize>;
using AccountKey = PropertyKey;
using BalanceKey = PropertyKey;
using CodeKey = PropertyKey;
using NonceKey = PropertyKey;
using StorageKey =
    std::array<char, 1 + sizeof(Address) + sizeof(ReincarnationNumber) +
                         sizeof(Key) + kBlockIdSize>;

BlockKey GetBlockKey(BlockId block);

AccountKey GetAccountKey(const Address& address, BlockId block);

BalanceKey GetBalanceKey(const Address& address, BlockId block);

CodeKey GetCodeKey(const Address& address, BlockId block);

NonceKey GetNonceKey(const Address& address, BlockId block);

StorageKey GetStorageKey(const Address& address,
                         ReincarnationNumber reincarnation, const Key& key,
                         BlockId block);

BlockId GetBlockId(std::span<const char> data);

struct AccountState {
  std::array<char, 1 + 4> Encode() const;
  void SetBytes(std::span<const char>);
  bool exists;
  ReincarnationNumber reincarnation_number;
};

}  // namespace carmen::archive::leveldb
