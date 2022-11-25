#pragma once

#include <array>
#include <string_view>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/common/leveldb/leveldb.h"
#include "backend/index/leveldb/index.h"

namespace carmen::backend::index {

// MultiLevelDbIndex is an index implementation over leveldb. Each index
// is supposed to be stored in a separate leveldb instance. Data is stored in
// the following format: key -> value.
template <Trivial K, std::integral I>
class MultiLevelDbIndex : public internal::LevelDbIndexBase<K, I, 0> {
 public:
  static absl::StatusOr<MultiLevelDbIndex> Open(
      const std::filesystem::path& path) {
    ASSIGN_OR_RETURN(auto db, LevelDb::Open(path));
    return MultiLevelDbIndex(std::move(db));
  }

 private:
  explicit MultiLevelDbIndex(LevelDb ldb)
      : internal::LevelDbIndexBase<K, I, 0>(), ldb_(std::move(ldb)) {}

  std::string GetHashKey() const override { return "hash"; };

  std::string GetLastIndexKey() const override { return "last_index"; }

  std::array<char, sizeof(K)> ToDBKey(const K& key) const override {
    std::array<char, sizeof(K)> buffer;
    memcpy(buffer.data(), &key, sizeof(K));
    return buffer;
  };

  LevelDb& GetDB() override { return ldb_; }
  const LevelDb& GetDB() const override { return ldb_; }

  LevelDb ldb_;
};

}  // namespace carmen::backend::index
