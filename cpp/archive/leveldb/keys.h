/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use of
 * this software will be governed by the GNU Lesser General Public License v3.
 */

#pragma once

#include <array>
#include <span>
#include <string_view>

#include "absl/status/statusor.h"
#include "common/type.h"

namespace carmen::archive::leveldb {

// Prefixes for LevelDB keys to differentiated between different table spaces.
// These keys are aligned with the Go implementation of the Carmen archive, and
// should be kept aligned for compatiblity.
enum class KeyType : char {
  kBlock = '1',
  kAccountState = '2',
  kBalance = '3',
  kCode = '4',
  kNonce = '5',
  kStorage = '6',
  kAccountHash = '7',
};

// Provides a label for each key type, or `unknown` for everything else.
std::string_view ToString(KeyType type);

// To differentiate multiple reincarnations of accounts, reincarnation numbers
// are utilized in the LevelDB archive. Each time an account is created or
// deleted, it is increased by 1, starting at 0.
using ReincarnationNumber = std::uint32_t;

// The key type used for per-block information.
using BlockKey = std::array<char, 1 + sizeof(BlockId)>;

// Most account properties share a common key format.
using PropertyKey = std::array<char, 1 + sizeof(Address) + sizeof(BlockId)>;
using AccountStateKey = PropertyKey;
using BalanceKey = PropertyKey;
using CodeKey = PropertyKey;
using NonceKey = PropertyKey;
using AccountHashKey = PropertyKey;

// The key to store storage information includes the reincarnation number to
// suppor efficient state clearing.
using StorageKey =
    std::array<char, 1 + sizeof(Address) + sizeof(ReincarnationNumber) +
                         sizeof(Key) + sizeof(BlockId)>;

// -- Factory functions for storage keys ---

BlockKey GetBlockKey(BlockId block);

AccountStateKey GetAccountStateKey(const Address& address, BlockId block);

AccountHashKey GetAccountHashKey(const Address& address, BlockId block);

BalanceKey GetBalanceKey(const Address& address, BlockId block);

CodeKey GetCodeKey(const Address& address, BlockId block);

NonceKey GetNonceKey(const Address& address, BlockId block);

StorageKey GetStorageKey(const Address& address,
                         ReincarnationNumber reincarnation, const Key& key,
                         BlockId block);

// Retrieves the block ID from any type of key. Note: for performance reasons it
// does not check that the given span encodes a valid key. It only interprets
// the portion of the provided span that is expected to contain the BlockId.
BlockId GetBlockFromKey(std::span<const char> key);

// Returns the prefix of the key covering the key space and account.
std::span<const char> GetAccountPrefix(std::span<const char> key);

// Returns the address encoded in the key. Note: for performance reasons it does
// not check that the given span encodes a valid key. It merely interprets the
// protion of the span where an address would be expected.
const Address& GetAddressFromKey(std::span<const char> data);

// Returns the reincarnation number encoded in a storage key.
ReincarnationNumber GetReincarnationNumber(const StorageKey& key);

// Returns the slot key encoded in a storage key.
const Key& GetSlotKey(const StorageKey& key);

}  // namespace carmen::archive::leveldb
