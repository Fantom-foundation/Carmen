#include "backend/index/leveldb/single_file/index.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/index/leveldb/common/level_db.h"

namespace carmen::backend::index {
namespace internal {
std::string StrToDBKey(char key_space, std::span<const char> key) {
  std::string buffer;
  buffer.reserve(key.size() + 1);
  buffer.push_back(key_space);
  buffer.append(key.begin(), key.end());
  return buffer;
}
}  // namespace internal

absl::StatusOr<KeySpacedLevelDBIndex> KeySpacedLevelDBIndex::Open(
    const std::filesystem::path& path) {
  auto db = internal::LevelDB::Open(path);
  if (!db.ok()) return db.status();
  return KeySpacedLevelDBIndex(
      std::make_shared<internal::LevelDB>(std::move(*db)));
}

// KeySpacedLevelDBIndex constructor.
KeySpacedLevelDBIndex::KeySpacedLevelDBIndex(
    std::shared_ptr<internal::LevelDB> ldb)
    : ldb_(std::move(ldb)) {}
}  // namespace carmen::backend::index
