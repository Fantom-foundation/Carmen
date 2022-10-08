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

enum class KeySpace : char {
  kBalance = 'B',
  kNonce = 'N',
  kSlot = 'S',
  kValue = 'V'
};

// Converts given key space and key into leveldb key.
template<Trivial K>
std::string ToLevelDBKey(const internal::KeySpace& key_space, const K& key) {
  std::stringstream ss;
  ss << static_cast<char>(key_space) << key;
  return ss.str();
}

// Converts given value into leveldb value.
template <Integral I>
std::string ToLevelDBValue(const I& value) {
  return {reinterpret_cast<const char*>(&value), sizeof(value)};
}

// Parse result from leveldb.
template <Integral I>
I ParseLevelDBResult(const std::string& value) {
  return *reinterpret_cast<const I*>(value.data());
}

// LevelDBKeySpaceBase is a base class for all key spaces. It provides basic
// functionality for key space. It is not intended to be used directly. Instead
// use one of the derived classes. This class handles all the basic operations
// over leveldb like get, add, get last index, etc.
class LevelDBKeySpaceBase {
 public:
  LevelDBKeySpaceBase(std::shared_ptr<internal::LevelDBIndexImpl> db, const internal::KeySpace& key_space) : impl_(std::move(db)), key_space_(key_space) {}

  // Get raw result for given key without key space transformation.
  absl::StatusOr<std::string> GetRaw(std::string_view key);

  // Get last index value.
  absl::StatusOr<std::string> GetLastIndexRaw();

  // Get actual hash value.
  absl::StatusOr<Hash> GetHashRaw();

  // Add last index value.
  absl::Status AddIndexRaw(std::string_view key, std::string_view value);

  // Add hash value.
  absl::Status AddHashRaw(const Hash& hash);

 protected:
  std::shared_ptr<internal::LevelDBIndexImpl> impl_;
  internal::KeySpace key_space_;
};
}  // namespace internal


template <Trivial K, Integral I>
class LevelDBKeySpace : protected internal::LevelDBKeySpaceBase {
 public:
  using LevelDBKeySpaceBase::LevelDBKeySpaceBase;

  // Get index for given key.
  absl::StatusOr<I> Get(const K& key) {
    auto result = GetRaw(internal::ToLevelDBKey(key_space_, key));
    if (result.ok()) {
      return internal::ParseLevelDBResult<I>(result.value());
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
    return GetRaw(internal::ToLevelDBKey(key_space_, key)).ok();
  }

  // Commit state of the key space. This will update the hash value. This is
  // required to be called after all the operations are done.
  absl::Status Commit() {
    if (keys_.empty()) return absl::OkStatus();

    auto hash = GetLastHash();
    if (!hash.ok()) return hash.status();

    // calculate new hash
    while (!keys_.empty()) {
      hash_ = GetSha256Hash(hash, keys_.front());
      keys_.pop();
    }

    // add new hash
    return AddHashRaw(hash_);
  }

  // Computes a hash over the full content of this index.
  absl::StatusOr<Hash> GetHash() const {
    auto status = Commit();
    if (!status.ok()) return status;
    return GetLastHash();
  }

 private:
  // Last index value. This is used to generate new index.
  std::optional<I> last_index_;
  // Current hash value.
  std::optional<Hash> hash_;
  // Cached keys to compute hash from.
  std::queue<K> keys_;

  // Get last index value. If it is not cached, it will be fetched from database.
  absl::StatusOr<I> GetLastIndex() {
    if (!last_index_.has_value()) {
      auto result = GetLastIndexRaw();
      if (!result.ok()) {
        return result.status();
      }
      last_index_ = internal::ParseLevelDBResult<I>(result.value());
    }
    return last_index_.value();
  }

  // Get last hash value. If it is not cached, it will be fetched from database.
  // If there is no hash value in database, it will return empty hash.
  absl::StatusOr<Hash> GetLastHash() {
    if (!hash_.has_value()) {
      auto result = GetHashRaw();
      switch (result.status().code()) {
        case absl::StatusCode::kNotFound:
          hash_ = Hash{};
          break;
        case absl::StatusCode::kOk:
          hash_ = result.value();
          break;
        default:
          return result.status();
      }
    }
    return hash_.value();
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

    auto write_value = internal::ToLevelDBValue(last_index_.value());
    auto write_result = AddIndexRaw(internal::ToLevelDBKey(key_space_, key), write_value);

    if (! write_result.ok()) {
      return write_result;
    }

    // Append key into queue.
    keys_.push(key);

    return last_index_.value();
  }
};

class LevelDBIndex {
 public:
  explicit LevelDBIndex(std::string_view path);

  // Returns Balance index.
  template <Trivial K, Integral I>
  LevelDBKeySpace<K, I> Balance() {
    return {impl_, internal::KeySpace::kBalance};
  }

  // Returns Nonce index.
  template <Trivial K, Integral I>
  LevelDBKeySpace<K, I> Nonce() {
    return {impl_, internal::KeySpace::kNonce};
  }

  // Returns Slot index.
  template <Trivial K, Integral I>
  LevelDBKeySpace<K, I> Slot() {
    return {impl_, internal::KeySpace::kSlot};
  }

  // Returns Value index.
  template <Trivial K, Integral I>
  LevelDBKeySpace<K, I> Value() {
    return {impl_, internal::KeySpace::kValue};
  }

 private:
  // PIMPL pattern. Implementation is hidden in LevelDBIndexImpl. This allows
  // us to change implementation without changing the interface.
  std::shared_ptr<internal::LevelDBIndexImpl> impl_;
};

}  // namespace carmen::backend::index
