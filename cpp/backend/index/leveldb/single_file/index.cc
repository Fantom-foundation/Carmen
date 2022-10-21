#include "backend/index/leveldb/single_file/index.h"

#include <span>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/index/leveldb/common/level_db.h"
#include "common/status_util.h"

namespace carmen::backend::index {
namespace {
constexpr std::string_view kHashKey = "_hash";
constexpr std::string_view kLastIndexKey = "_last";

std::string StrToDBKey(char key_space, std::span<const char> key) {
  std::string buffer;
  buffer.reserve(key.size() + 1);
  buffer.push_back(key_space);
  buffer.append(key.begin(), key.end());
  return buffer;
}
}  // namespace

namespace internal {
std::string LevelDBKeySpaceBase::GetHashKey() const {
  return StrToDBKey(key_space_, kHashKey);
}

std::string LevelDBKeySpaceBase::GetLastIndexKey() const {
  return StrToDBKey(key_space_, kLastIndexKey);
}

// Get current hash value from database for current key space.
absl::StatusOr<Hash> LevelDBKeySpaceBase::GetHashFromDB() const {
  ASSIGN_OR_RETURN(auto data, ldb_->Get(GetHashKey()));
  if (data.size() != sizeof(Hash))
    return absl::InternalError("Invalid hash size.");
  return *reinterpret_cast<Hash*>(data.data());
}

// Add hash value.
absl::Status LevelDBKeySpaceBase::AddHashIntoDB(const Hash& hash) const {
  return ldb_->Add(GetHashKey(),
                   {reinterpret_cast<const char*>(&hash), sizeof(hash)});
}
}  // namespace internal

absl::StatusOr<LevelDBIndex> LevelDBIndex::Open(
    const std::filesystem::path& path) {
  auto db = internal::LevelDB::Open(path);
  if (!db.ok()) return db.status();
  return LevelDBIndex(std::make_shared<internal::LevelDB>(std::move(*db)));
}

// LevelDBIndex constructor.
LevelDBIndex::LevelDBIndex(std::shared_ptr<internal::LevelDB> ldb)
    : ldb_(std::move(ldb)) {}
}  // namespace carmen::backend::index
