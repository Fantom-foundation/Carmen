#pragma once

#include <array>
#include <span>

#include "absl/status/statusor.h"
#include "common/type.h"

namespace carmen::archive::leveldb {

// Prefixes for LevelDB keys to differentiated between different table spaces.
// These keys are aligned with the Go implementation of the Carmen archive, and
// should be kept aligned for compatiblity.
enum class KeyType : char {
  kBlock = '1',
  kAccount = '2',
  kBalance = '3',
  kCode = '4',
  kNonce = '5',
  kStorage = '6',
  kAccountHash = '7',
};

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
using AccountHashKey = PropertyKey;
using StorageKey =
    std::array<char, 1 + sizeof(Address) + sizeof(ReincarnationNumber) +
                         sizeof(Key) + kBlockIdSize>;

BlockKey GetBlockKey(BlockId block);

AccountKey GetAccountKey(const Address& address, BlockId block);

BalanceKey GetBalanceKey(const Address& address, BlockId block);

CodeKey GetCodeKey(const Address& address, BlockId block);

NonceKey GetNonceKey(const Address& address, BlockId block);

AccountHashKey GetAccountHashKey(const Address& address, BlockId block);

StorageKey GetStorageKey(const Address& address,
                         ReincarnationNumber reincarnation, const Key& key,
                         BlockId block);

BlockId GetBlockFromKey(std::span<const char> data);
const Address& GetAddressFromKey(std::span<const char> data);

ReincarnationNumber GetReincarnationNumber(const StorageKey& key);
Key GetSlotKey(const StorageKey& key);

std::span<const char> GetAccountPrefix(const PropertyKey& key);
std::span<const char> GetAccountPrefix(const StorageKey& key);

// TODO: move to extra file.
struct AccountState {
  static absl::StatusOr<AccountState> From(std::span<const std::byte>);
  std::array<char, 5> Encode() const;
  void SetBytes(std::span<const std::byte>);
  bool exists = false;
  ReincarnationNumber reincarnation_number = 0;
};

}  // namespace carmen::archive::leveldb
