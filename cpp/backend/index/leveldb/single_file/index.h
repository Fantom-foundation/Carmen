#pragma once

#include <iostream>
#include <queue>
#include <sstream>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/index/leveldb/common/level_db.h"
#include "common/hash.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen::backend::index {
namespace internal {

// Converts given key space and key into leveldb key.
template <Trivial K>
std::array<char, sizeof(K) + 1> ToDBKey(char key_space, const K& key) {
  std::array<char, sizeof(key) + 1> buffer{key_space};
  memcpy(buffer.data() + 1, &key, sizeof(key));
  return buffer;
}

// Converts given value into leveldb value.
template <std::integral I>
std::array<char, sizeof(I)> ToDBValue(const I& value) {
  std::array<char, sizeof(value)> buffer{};
  memcpy(buffer.data(), &value, sizeof(value));
  return buffer;
}

// Parse result from leveldb.
template <std::integral I>
absl::StatusOr<I> ParseDBResult(std::span<const char> value) {
  if (value.size() != sizeof(I)) {
    return absl::InternalError("Invalid value size.");
  }
  return *reinterpret_cast<const I*>(value.data());
}

// LevelDBKeySpaceBase is a base class for all key spaces. It provides basic
// functionality for key space. It is not intended to be used directly. Instead
// use one of the derived classes. This class handles all the basic operations
// over leveldb like get, add, get last index, etc.
class LevelDBKeySpaceBase {
 public:
  LevelDBKeySpaceBase(std::shared_ptr<internal::LevelDB> db, char key_space)
      : ldb_(std::move(db)), key_space_(key_space) {}

  // Get result for given key.
  template <Trivial K, std::integral I>
  absl::StatusOr<I> GetFromDB(const K& key) const {
    ASSIGN_OR_RETURN(auto data, ldb_->Get(ToDBKey(key_space_, key)));
    return ParseDBResult<I>(data);
  }

  // Get last index value.
  template <std::integral I>
  absl::StatusOr<I> GetLastIndexFromDB() const {
    ASSIGN_OR_RETURN(auto data, ldb_->Get(GetLastIndexKey()));
    return ParseDBResult<I>(data);
  }

  // Get actual hash value.
  absl::StatusOr<Hash> GetHashFromDB() const;

  // Add index value for given key. This method also updates last index value.
  template <std::integral I>
  absl::Status AddIndexAndUpdateLatestIntoDB(auto key, const I& value) const {
    auto db_val = ToDBValue(value);
    auto db_key = ToDBKey(key_space_, key);
    auto last_index_key = GetLastIndexKey();
    auto batch =
        std::array{LDBEntry{db_key, db_val}, LDBEntry{last_index_key, db_val}};
    return ldb_->AddBatch(batch);
  }

  // Add hash value.
  absl::Status AddHashIntoDB(const Hash& hash) const;

 private:
  std::string GetHashKey() const;
  std::string GetLastIndexKey() const;

  std::shared_ptr<internal::LevelDB> ldb_;
  char key_space_;
};
}  // namespace internal

template <Trivial K, std::integral I>
class LevelDBKeySpace : protected internal::LevelDBKeySpaceBase {
 public:
  using LevelDBKeySpaceBase::LevelDBKeySpaceBase;

  // Get index for given key.
  absl::StatusOr<I> Get(const K& key) const { return GetFromDB<K, I>(key); }

  // Get index for given key. If key is not found, add it and return new index.
  absl::StatusOr<std::pair<I, bool>> GetOrAdd(const K& key) {
    auto result = Get(key);
    if (result.ok()) return std::make_pair(*result, false);

    // If key is not found, add it and return new index.
    if (result.status().code() == absl::StatusCode::kNotFound) {
      auto new_index = GenerateNewIndex(key);
      if (new_index.ok()) return std::make_pair(*new_index, true);
      return new_index.status();
    }

    return result.status();
  }

  // Check index for given key exists. Returns true if index exists.
  bool Contains(const K& key) { return Get(key).ok(); }

  // Computes a hash over the full content of this index.
  absl::StatusOr<Hash> GetHash() {
    auto status = Commit();
    if (!status.ok()) return status;
    return GetLastHash();
  }

 private:
  // Get last index value. If it is not cached, it will be fetched from
  // database.
  absl::StatusOr<I> GetLastIndex() {
    if (last_index_.has_value()) return *last_index_;
    ASSIGN_OR_RETURN(last_index_, GetLastIndexFromDB<I>());
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

    auto write_result = AddIndexAndUpdateLatestIntoDB(key, *last_index_);

    if (!write_result.ok()) return write_result;

    // Append key into queue.
    keys_.push(key);

    return *last_index_;
  }

  // Commit state of the key space. This will update the hash value.
  absl::Status Commit() {
    if (keys_.empty()) return absl::OkStatus();

    auto hash = GetLastHash();
    if (!hash.ok()) return hash.status();

    // calculate new hash
    while (!keys_.empty()) {
      hash_ = carmen::GetHash(hasher_, *hash, keys_.front());
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

class LevelDBIndex {
 public:
  static absl::StatusOr<LevelDBIndex> Open(const std::filesystem::path& path);

  // Returns index for given key space.
  template <Trivial K, std::integral I>
  LevelDBKeySpace<K, I> KeySpace(char key_space) {
    return {ldb_, key_space};
  }

 private:
  explicit LevelDBIndex(std::shared_ptr<internal::LevelDB> ldb);
  std::shared_ptr<internal::LevelDB> ldb_;
};

}  // namespace carmen::backend::index
