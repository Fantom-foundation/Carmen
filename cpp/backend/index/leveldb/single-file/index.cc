#include "backend/index/leveldb/single-file/index.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace carmen::backend::index {
namespace {
constexpr std::string_view kHashKey = "hash";
constexpr std::string_view kLastIndexKey = "last";
}  // namespace

namespace internal {
// Get raw result for given key without key space transformation.
absl::StatusOr<std::string> LevelDBKeySpaceBase::GetFromDB(
    std::string_view key) const {
  return ldb_->Get(key);
}

// Get latest index value from database for current key space.
absl::StatusOr<std::string> LevelDBKeySpaceBase::GetLastIndexFromDB() {
  return ldb_->Get(internal::ToDBKey(key_space_, kLastIndexKey));
}

// Get current hash value from database for current key space.
absl::StatusOr<Hash> LevelDBKeySpaceBase::GetHashFromDB() {
  auto result = ldb_->Get(internal::ToDBKey(key_space_, kHashKey));
  if (result.ok()) {
    if (result->size() != sizeof(Hash)) {
      return absl::InternalError("Invalid hash size.");
    }
    return *reinterpret_cast<Hash*>(result->data());
  }
  return result.status();
}

// Add index value for given key. This method also updates last index value.
absl::Status LevelDBKeySpaceBase::AddIndexAndUpdateLatestIntoDB(
    std::string_view key, std::string_view value) {
  auto last_index_key = internal::ToDBKey(key_space_, kLastIndexKey);

  std::array<std::pair<std::string_view, std::string_view>, 2> batch{
      std::pair<std::string_view, std::string_view>{key, value},
      std::pair<std::string_view, std::string_view>{last_index_key, value},
  };

  return ldb_->AddBatch(batch);
}

// Add hash value.
absl::Status LevelDBKeySpaceBase::AddHashIntoDB(const Hash& hash) {
  return ldb_->Add(internal::ToDBKey(key_space_, kHashKey),
                   {reinterpret_cast<const char*>(&hash), sizeof(hash)});
}
}  // namespace internal

absl::StatusOr<LevelDBIndex> LevelDBIndex::Open(std::string_view path) {
  auto db = internal::LevelDBInstance::Open(path);
  if (!db.ok()) return db.status();
  return LevelDBIndex(
      std::make_shared<internal::LevelDBInstance>(std::move(*db)));
}

// LevelDBIndex constructor.
LevelDBIndex::LevelDBIndex(std::shared_ptr<internal::LevelDBInstance> ldb)
    : ldb_(std::move(ldb)) {}
}  // namespace carmen::backend::index
