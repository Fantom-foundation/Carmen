#include "backend/index/leveldb/single_db/index.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/common/leveldb/leveldb.h"
#include "common/status_util.h"

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

absl::StatusOr<SingleLevelDbIndex> SingleLevelDbIndex::Open(
    const std::filesystem::path& path) {
  ASSIGN_OR_RETURN(auto db, LevelDb::Open(path, /*create_if_missing=*/true));
  return SingleLevelDbIndex(std::make_shared<LevelDb>(std::move(db)));
}

// SingleLevelDbIndex constructor.
SingleLevelDbIndex::SingleLevelDbIndex(std::shared_ptr<LevelDb> ldb)
    : ldb_(std::move(ldb)) {}
}  // namespace carmen::backend::index
