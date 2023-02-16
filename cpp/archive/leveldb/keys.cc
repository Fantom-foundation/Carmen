#include "archive/leveldb/keys.h"

#include <array>
#include <cstring>
#include <span>

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
};

namespace {

template <std::size_t offset, std::size_t count, typename T, std::size_t Extend>
std::span<T, count> subspan(std::array<T, Extend>& array) {
  static_assert(Extend >= offset + count);
  return std::span<T, count>(&array[offset], count);
}

// Numerical values have to be encoded using big-endian such that LevelDB's
// lexicographical sorting matches the natural order of values.

void Write(std::uint32_t value, std::span<char, 4> trg) {
  for (int i = 0; i < 4; i++) {
    trg[i] = value >> (3 - i) * 8;
  }
}

// Writes a 4 byte value into an 8-byte slot (needed for encoding BlockIds).
void Write(std::uint32_t value, std::span<char, 8> trg) {
  for (int i = 0; i < 4; i++) {
    trg[i] = 0;
  }
  Write(value, std::span<char, 4>(trg.data() + 4, 4));
}

template <Trivial T>
void Write(const T& value, std::span<char, sizeof(T)> trg) {
  std::memcpy(trg.data(), &value, sizeof(T));
}

std::uint32_t ReadUint32(std::span<const char, 4> src) {
  auto byte = [&](int i) { return std::uint32_t(std::uint8_t(src[i])); };
  return byte(0) << 24 | byte(1) << 16 | byte(2) << 8 | byte(3);
}

template <KeyType type, typename Key>
Key Get(const Address& address, BlockId block) {
  Key res;
  res[0] = static_cast<char>(type);
  Write(address, subspan<1, 20>(res));
  Write(block, subspan<1 + 20, 8>(res));
  return res;
}

}  // namespace

BlockKey GetBlockKey(BlockId block) {
  BlockKey res;
  res[0] = static_cast<char>(KeyType::kBlock);
  Write(block, subspan<1, 8>(res));
  return res;
}

AccountKey GetAccountKey(const Address& address, BlockId block) {
  return Get<KeyType::kAccount, AccountKey>(address, block);
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

StorageKey GetStorageKey(const Address& address,
                         ReincarnationNumber reincarnation, const Key& key,
                         BlockId block) {
  StorageKey res;
  res[0] = static_cast<char>(KeyType::kStorage);
  Write(address, subspan<1, 20>(res));
  Write(reincarnation, subspan<1 + 20, 4>(res));
  Write(key, subspan<1 + 20 + 4, 32>(res));
  Write(block, subspan<1 + 20 + 4 + 32, 8>(res));
  return res;
}

BlockId GetBlockId(std::span<const char> data) {
  // The block ID is always stored in the last 4 bytes.
  assert(data.size() >= 4);
  if (data.size() < 4) return 0;
  return ReadUint32(std::span<const char, 4>(data.data() + data.size() - 4, 4));
}

std::array<char, 1 + 4> AccountState::Encode() const {
  std::array<char, 5> res;
  res[0] = exists ? 1 : 0;
  Write(reincarnation_number, subspan<1, 4>(res));
  return res;
}

void AccountState::SetBytes(std::span<const std::byte> span) {
  if (span.size() != 5) return;
  exists = std::uint8_t(span[0]) != 0;
  reincarnation_number = ReadUint32(std::span<const char, 4>(
      reinterpret_cast<const char*>(span.data()) + 1, 4));
}

}  // namespace carmen::archive::leveldb
