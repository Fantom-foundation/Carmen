#pragma once

#include <string_view>
#include <array>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/index/leveldb/common/level_db.h"
#include "backend/index/leveldb/common/index.h"

namespace carmen::backend::index {
namespace internal {
constexpr std::string_view kHashKey = "hash";
constexpr std::string_view kLastIndexKey = "last_index";
}

// LevelDBIndex is an index implementation over leveldb.
template <Trivial K, std::integral I>
class LevelDBIndex : public internal::LevelDBIndexBase<K, I> {
 public:
  static absl::StatusOr<LevelDBIndex> Open(const std::filesystem::path& path) {
    auto db = internal::LevelDB::Open(path);
    if (!db.ok()) return db.status();
    return LevelDBIndex(std::move(*db));
  }

 private:
  explicit LevelDBIndex(internal::LevelDB ldb)
      : internal::LevelDBIndexBase<K, I>(), test(std::move(ldb)) {
                                                //this->Initialize(&test);
  }

  std::string_view GetHashKey() const override {
      return internal::kHashKey;
  };

  std::string_view GetLastIndexKey() const override {
    return internal::kLastIndexKey;
  }

  internal::LevelDB test;
};

}  // namespace carmen::backend::index
