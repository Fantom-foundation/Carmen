#pragma once

#include <array>
#include <span>

#include "absl/status/statusor.h"
#include "common/type.h"

namespace carmen::archive::leveldb {

using ReincarnationId = std::uint32_t;

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
    std::array<char, 1 + sizeof(Address) + sizeof(ReincarnationId) +
                         sizeof(Key) + kBlockIdSize>;

BlockKey GetBlockKey(BlockId block);

AccountKey GetAccountKey(const Address& address, BlockId block);

BalanceKey GetBalanceKey(const Address& address, BlockId block);

CodeKey GetCodeKey(const Address& address, BlockId block);

NonceKey GetNonceKey(const Address& address, BlockId block);

StorageKey GetStorageKey(const Address& address, ReincarnationId reincarnation,
                         const Key& key, BlockId block);

class PropertyKeyView {
 public:
  static absl::StatusOr<PropertyKeyView> Parse(std::span<const char> data) {
    if (data.size() != kPropertyKeySize) {
      return absl::InvalidArgumentError("wrong size of key");
    }
    return PropertyKeyView(
        std::span<const char, kPropertyKeySize>{data.data(), kPropertyKeySize});
  }
  BlockId GetBlockId() {
    static_assert(sizeof(BlockId) == 4);
    constexpr const std::size_t offset = 1 + sizeof(Address) + 4;
    auto cast = [](char v) { return std::uint32_t(std::uint8_t(v)); };
    return cast(span_[offset + 0]) << 24 | cast(span_[offset + 1]) << 16 |
           cast(span_[offset + 2]) << 8 | cast(span_[offset + 3]);
  }

  Address GetAddress() {
    Address res;
    res.SetBytes(
        std::as_bytes(std::span<const char, 20>{span_.data() + 1, 20}));
    return res;
  }

 private:
  PropertyKeyView(std::span<const char, kPropertyKeySize> span) : span_(span) {}
  std::span<const char, kPropertyKeySize> span_;
};

}  // namespace carmen::archive::leveldb
