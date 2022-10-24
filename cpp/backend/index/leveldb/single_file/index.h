#pragma once

#include <iostream>
#include <queue>
#include <sstream>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/index/leveldb/common/index.h"
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

// Converts given key space and key into leveldb key.
std::string StrToDBKey(char key_space, std::span<const char> key);
}  // namespace internal

template <Trivial K, std::integral I>
class LevelDBKeySpace : public internal::LevelDBIndexBase<K, I, 1> {
 public:
  LevelDBKeySpace(std::shared_ptr<internal::LevelDB> ldb, char key_space)
      : internal::LevelDBIndexBase<K, I, 1>(),
        ldb_(std::move(ldb)),
        key_space_(key_space) {}

 private:
  std::string GetHashKey() const override {
    return internal::StrToDBKey(key_space_, "_hash");
  };

  std::string GetLastIndexKey() const override {
    return internal::StrToDBKey(key_space_, "_last_index");
  }

  std::array<char, sizeof(K) + 1> ToDBKey(const K& key) const override {
    return internal::ToDBKey(key_space_, key);
  };

  internal::LevelDB& GetDB() override { return *ldb_; }
  const internal::LevelDB& GetDB() const override { return *ldb_; }

  std::shared_ptr<internal::LevelDB> ldb_;
  char key_space_;
};

class KeySpacedLevelDBIndex {
 public:
  static absl::StatusOr<KeySpacedLevelDBIndex> Open(
      const std::filesystem::path& path);

  // Returns index for given key space.
  template <Trivial K, std::integral I>
  LevelDBKeySpace<K, I> KeySpace(char key_space) {
    return {ldb_, key_space};
  }

 private:
  explicit KeySpacedLevelDBIndex(std::shared_ptr<internal::LevelDB> ldb);
  std::shared_ptr<internal::LevelDB> ldb_;
};

}  // namespace carmen::backend::index
