#pragma once

#include <array>
#include <string_view>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/index/leveldb/common/index.h"
#include "backend/index/leveldb/common/level_db.h"

namespace carmen::backend::index {

// KeySpacedLevelDBIndex is an index implementation over leveldb.
template <Trivial K, std::integral I>
class LevelDBIndex : public internal::LevelDBIndexBase<K, I, 0> {
 public:
  static absl::StatusOr<LevelDBIndex> Open(const std::filesystem::path& path) {
    auto db = internal::LevelDB::Open(path);
    if (!db.ok()) return db.status();
    return LevelDBIndex(std::move(*db));
  }

 private:
  explicit LevelDBIndex(internal::LevelDB ldb)
      : internal::LevelDBIndexBase<K, I, 0>(), ldb_(std::move(ldb)) {}

  std::string GetHashKey() const override { return "hash"; };

  std::string GetLastIndexKey() const override { return "last_index"; }

  std::array<char, sizeof(K)> ToDBKey(const K& key) const override {
    std::array<char, sizeof(K)> buffer;
    memcpy(buffer.data(), &key, sizeof(K));
    return buffer;
  };

  internal::LevelDB& GetDB() override { return ldb_; }
  const internal::LevelDB& GetDB() const override { return ldb_; }

  internal::LevelDB ldb_;
};

}  // namespace carmen::backend::index
