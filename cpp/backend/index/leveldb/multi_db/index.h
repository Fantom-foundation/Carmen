#pragma once

#include <array>
#include <string_view>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/common/leveldb/level_db.h"
#include "backend/index/leveldb/index.h"

namespace carmen::backend::index {

// MultiLevelDBIndex is an index implementation over leveldb. Each index
// is supposed to be stored in a separate leveldb instance. Data is stored in
// the following format: key -> value.
template <Trivial K, std::integral I>
class MultiLevelDBIndex : public internal::LevelDBIndexBase<K, I, 0> {
 public:
  static absl::StatusOr<MultiLevelDBIndex> Open(
      const std::filesystem::path& path) {
    auto db = internal::LevelDB::Open(path);
    if (!db.ok()) return db.status();
    return MultiLevelDBIndex(std::move(*db));
  }

 private:
  explicit MultiLevelDBIndex(internal::LevelDB ldb)
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
