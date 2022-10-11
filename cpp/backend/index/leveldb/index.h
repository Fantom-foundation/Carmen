#pragma once

#include <iostream>
#include <queue>
#include <sstream>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/hash.h"
#include "common/type.h"

namespace carmen::backend::index {
namespace internal {
// Forward declaration of LevelDB implementation class using PIMPL pattern.
class LevelDBIndexImpl;

// Converts given key space and key into leveldb key.
template <Trivial K>
std::string ToDBKey(char key_space, const K& key) {
  std::stringstream ss;
  ss << key_space;
  ss.write(reinterpret_cast<const char*>(&key), sizeof(key));
  return ss.str();
}

// Converts given value into leveldb value.
template <std::integral I>
std::string ToDBValue(const I& value) {
  return {reinterpret_cast<const char*>(&value), sizeof(value)};
}

// Parse result from leveldb.
template <std::integral I>
absl::StatusOr<I> ParseDBResult(std::string_view value) {
  if (value.size() != sizeof(I)) {
    return absl::InvalidArgumentError("Invalid value size.");
  }
  return *reinterpret_cast<const I*>(value.data());
}

// LevelDBKeySpaceBase is a base class for all key spaces. It provides basic
// functionality for key space. It is not intended to be used directly. Instead
// use one of the derived classes. This class handles all the basic operations
// over leveldb like get, add, get last index, etc.
class LevelDBKeySpaceBase {
 public:
  LevelDBKeySpaceBase(std::shared_ptr<internal::LevelDBIndexImpl> db,
                      char key_space)
      : impl_(std::move(db)), key_space_(key_space) {}

  // Get raw result for given key without key space transformation.
  absl::StatusOr<std::string> GetFromDB(std::string_view key);

  // Get last index value.
  absl::StatusOr<std::string> GetLastIndexFromDB();

  // Get actual hash value.
  absl::StatusOr<Hash> GetHashFromDB();

  // Add last index value.
  absl::Status AddIndexAndUpdateLatestIntoDB(std::string_view key,
                                             std::string_view value);

  // Add hash value.
  absl::Status AddHashIntoDB(const Hash& hash);

 protected:
  std::shared_ptr<internal::LevelDBIndexImpl> impl_;
  char key_space_;
};
}  // namespace internal

template <Trivial K, std::integral I>
class LevelDBKeySpace : protected internal::LevelDBKeySpaceBase {
 public:
  using LevelDBKeySpaceBase::LevelDBKeySpaceBase;

  // Get index for given key.
  absl::StatusOr<I> Get(const K& key) {
    auto result = GetFromDB(internal::ToDBKey(key_space_, key));
    if (result.ok()) {
      return internal::ParseDBResult<I>(result.value());
    }
    return result.status();
  }

  // Get index for given key. If key is not found, add it and return new index.
  absl::StatusOr<I> GetOrAdd(const K& key) {
    auto result = Get(key);
    if (result.ok()) {
      return result.value();
    }

    // If key is not found, add it and return new index.
    if (result.status().code() == absl::StatusCode::kNotFound) {
      return GenerateNewIndex(key);
    }

    return result.status();
  }

  // Check index for given key exists. Returns true if index exists.
  bool Contains(const K& key) {
    return GetFromDB(internal::ToDBKey(key_space_, key)).ok();
  }

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
    if (last_index_.has_value()) {
      return last_index_.value();
    }
    auto result = GetLastIndexFromDB();
    if (result.ok()) {
      auto index = internal::ParseDBResult<I>(result.value());
      if (index.ok()) {
        last_index_ = *index;
        return last_index_.value();
      }
      return index.status();
    }
    return result.status();
  }

  // Get last hash value. If it is not cached, it will be fetched from database.
  // If there is no hash value in database, it will return empty hash.
  absl::StatusOr<Hash> GetLastHash() {
    if (hash_.has_value()) {
      return hash_.value();
    }
    auto result = GetHashFromDB();
    switch (result.status().code()) {
      case absl::StatusCode::kNotFound:
        hash_ = Hash{};
        return hash_.value();
      case absl::StatusCode::kOk:
        hash_ = result.value();
        return hash_.value();
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
        last_index_ = result.value() + 1;
        break;
      default:
        return result.status();
    }

    auto write_value = internal::ToDBValue(last_index_.value());
    auto write_result = AddIndexAndUpdateLatestIntoDB(
        internal::ToDBKey(key_space_, key), write_value);

    if (!write_result.ok()) {
      return write_result;
    }

    // Append key into queue.
    keys_.push(key);

    return last_index_.value();
  }

  // Commit state of the key space. This will update the hash value.
  absl::Status Commit() {
    if (keys_.empty()) return absl::OkStatus();

    auto hash = GetLastHash();
    if (!hash.ok()) return hash.status();

    // calculate new hash
    while (!keys_.empty()) {
      hash_ = GetSha256Hash(hash.value(), keys_.front());
      keys_.pop();
    }

    // add new hash
    return AddHashIntoDB(hash_.value());
  }

  // Last index value. This is used to generate new index.
  std::optional<I> last_index_;
  // Current hash value.
  std::optional<Hash> hash_;
  // Cached keys to compute hash from.
  std::queue<K> keys_;
};

class LevelDBIndex {
 public:
  explicit LevelDBIndex(std::string_view path);

  // Returns index for given key space.
  template <Trivial K, std::integral I>
  LevelDBKeySpace<K, I> KeySpace(char key_space) {
    return {impl_, key_space};
  }

 private:
  // PIMPL pattern. Implementation is hidden in LevelDBIndexImpl. This allows
  // us to change implementation without changing the interface.
  std::shared_ptr<internal::LevelDBIndexImpl> impl_;
};

}  // namespace carmen::backend::index
