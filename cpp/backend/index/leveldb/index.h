// Copyright (c) 2024 Fantom Foundation
// 
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
// 
// Change Date: 2028-4-16
// 
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

#pragma once

#include <cassert>
#include <iostream>
#include <queue>
#include <sstream>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/common/leveldb/leveldb.h"
#include "common/hash.h"
#include "common/memory_usage.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen::backend::index::internal {
template <std::integral I>
// Parse result from leveldb.
absl::StatusOr<I> ParseDBResult(std::span<const char> value) {
  if (value.size() != sizeof(I)) {
    return absl::InternalError("Invalid value size.");
  }
  return *reinterpret_cast<const I*>(value.data());
}

// Converts given value into leveldb value.
template <std::integral I>
std::array<char, sizeof(I)> ToDBValue(const I& value) {
  std::array<char, sizeof(value)> buffer{};
  memcpy(buffer.data(), &value, sizeof(value));
  return buffer;
}

// Base levelDB index class. This class provides basic functionality for
// leveldb index. Key has to be trivially copyable and value has to be
// std::integral. KPL stands for key prefix length. It is the length of the
// prefix of the key that is used to group keys together. Grouped keys are
// used for storing data into single leveldb file to distinct data from
// different indexes.
template <Trivial K, std::integral I, std::size_t KPL>
class LevelDbIndexBase {
 public:
  using key_type = K;
  using value_type = I;

  LevelDbIndexBase(LevelDbIndexBase&&) noexcept = default;
  virtual ~LevelDbIndexBase() = default;

  // Get index for given key.
  absl::StatusOr<I> Get(const K& key) const {
    ASSIGN_OR_RETURN(auto data, GetDb().Get(ToDBKey(key)));
    return ParseDBResult<I>(data);
  }

  // Get index for given key. If key is not found, add it and return new index.
  absl::StatusOr<std::pair<I, bool>> GetOrAdd(const K& key) {
    auto result = Get(key);
    if (result.ok()) return std::pair{*result, false};

    // If key is not found, add it and return new index.
    if (result.status().code() == absl::StatusCode::kNotFound) {
      auto new_index = GenerateNewIndex(key);
      if (new_index.ok()) return std::pair{*new_index, true};
      return new_index.status();
    }

    return result.status();
  }

  // Check index for given key exists. Returns true if index exists.
  bool Contains(const K& key) { return Get(key).ok(); }

  // Computes a hash over the full content of this index.
  absl::StatusOr<Hash> GetHash() {
    RETURN_IF_ERROR(Commit());
    return GetLastHash();
  }

  // Flush unsaved index keys to disk.
  absl::Status Flush() { return GetDb().Flush(); }

  // Close this index and release resources.
  absl::Status Close() { return GetDb().Close(); }

  // Summarizes the memory usage of this instance.
  MemoryFootprint GetMemoryFootprint() const {
    MemoryFootprint res(*this);
    res.Add("unhashed_keys", SizeOf(keys_));
    res.Add("db", GetDb().GetMemoryFootprint());
    return res;
  }

 protected:
  explicit LevelDbIndexBase() = default;

 private:
  // Get hash key into leveldb.
  virtual std::string GetHashKey() const = 0;

  // Get last index key into leveldb.
  virtual std::string GetLastIndexKey() const = 0;

  // Converts given key into leveldb key.
  virtual std::array<char, sizeof(K) + KPL> ToDBKey(const K& key) const = 0;

  // Get leveldb handle.
  virtual LevelDb& GetDb() = 0;
  virtual const LevelDb& GetDb() const = 0;

  // Get last index value.
  absl::StatusOr<I> GetLastIndexFromDB() const {
    ASSIGN_OR_RETURN(auto data, GetDb().Get(GetLastIndexKey()));
    return ParseDBResult<I>(data);
  }

  // Get actual hash value.
  absl::StatusOr<Hash> GetHashFromDB() const {
    ASSIGN_OR_RETURN(auto data, GetDb().Get(GetHashKey()));
    if (data.size() != sizeof(Hash))
      return absl::InternalError("Invalid hash size.");
    return *reinterpret_cast<Hash*>(data.data());
  }

  // Add index value for given key. This method also updates last index value.
  absl::Status AddIndexAndUpdateLatestIntoDB(auto key, const I& value) {
    auto db_val = ToDBValue(value);
    auto db_key = ToDBKey(key);
    auto last_index_key = GetLastIndexKey();
    auto batch =
        std::array{LDBEntry{db_key, db_val}, LDBEntry{last_index_key, db_val}};
    return GetDb().AddBatch(batch);
  }

  // Add hash value into database.
  absl::Status AddHashIntoDB(const Hash& hash) {
    return GetDb().Add(
        {GetHashKey(), {reinterpret_cast<const char*>(&hash), sizeof(hash)}});
  }

  // Get last index value. If it is not cached, it will be fetched from
  // database.
  absl::StatusOr<I> GetLastIndex() {
    if (last_index_.has_value()) return *last_index_;
    ASSIGN_OR_RETURN(last_index_, GetLastIndexFromDB());
    return *last_index_;
  }

  // Get last hash value. If it is not cached, it will be fetched from database.
  // If there is no hash value in database, it will return empty hash.
  absl::StatusOr<Hash> GetLastHash() {
    if (hash_.has_value()) return *hash_;

    auto result = GetHashFromDB();
    switch (result.status().code()) {
      case absl::StatusCode::kNotFound:
        hash_ = Hash{};
        return *hash_;
      case absl::StatusCode::kOk:
        hash_ = *result;
        return *hash_;
      default:
        return result.status();
    }
  }

  // Generate new index for given key. This will also update last index value.
  absl::StatusOr<I> GenerateNewIndex(const K& key) {
    auto result = GetLastIndex();

    switch (result.status().code()) {
      case absl::StatusCode::kNotFound:
        last_index_ = 0;
        break;
      case absl::StatusCode::kOk:
        last_index_ = *result + 1;
        break;
      default:
        return result.status();
    }

    RETURN_IF_ERROR(AddIndexAndUpdateLatestIntoDB(key, *last_index_));

    // Append key into queue.
    keys_.push(key);

    return *last_index_;
  }

  // Commit state of the key space. This will update the hash value.
  absl::Status Commit() {
    if (keys_.empty()) return absl::OkStatus();

    ASSIGN_OR_RETURN(hash_, GetLastHash());

    // calculate new hash
    while (!keys_.empty()) {
      hash_ = carmen::GetHash(hasher_, *hash_, keys_.front());
      keys_.pop();
    }

    // add new hash
    return AddHashIntoDB(*hash_);
  }

  // Last index value. This is used to generate new index.
  std::optional<I> last_index_;
  // Current hash value.
  std::optional<Hash> hash_;
  // Cached keys to compute hash from.
  std::queue<K> keys_;
  // A SHA256 hasher instance used for hashing keys.
  Sha256Hasher hasher_;
};
}  // namespace carmen::backend::index::internal
