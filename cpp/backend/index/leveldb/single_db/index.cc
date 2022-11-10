#include "backend/index/leveldb/single_db/index.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/common/leveldb/level_db.h"

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

absl::StatusOr<SingleLevelDBIndex> SingleLevelDBIndex::Open(
    const std::filesystem::path& path) {
  ASSIGN_OR_RETURN(auto db, LevelDB::Open(path, /*create_if_missing=*/true));
  return SingleLevelDBIndex(std::make_shared<LevelDB>(std::move(db)));
}

// SingleLevelDBIndex constructor.
SingleLevelDBIndex::SingleLevelDBIndex(std::shared_ptr<LevelDB> ldb)
    : ldb_(std::move(ldb)) {}
}  // namespace carmen::backend::index
