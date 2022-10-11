#include "index.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "leveldb/db.h"
#include "leveldb/slice.h"
#include "leveldb/write_batch.h"

namespace carmen::backend::index {
namespace {
constexpr std::string_view kHashKey = "hash";
constexpr std::string_view kLastIndexKey = "last";
constexpr leveldb::WriteOptions kWriteOptions = leveldb::WriteOptions();
constexpr leveldb::ReadOptions kReadOptions = leveldb::ReadOptions();
}  // namespace

namespace internal {

class LevelDBIndexImpl {
 public:
  explicit LevelDBIndexImpl(std::string_view path) {
    // open database connection and store pointer.
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status =
        leveldb::DB::Open(options, {path.data(), path.size()}, &db);
    assert(status.ok());
    db_.reset(db);
  }

  // Get value for given key.
  absl::StatusOr<std::string> Get(std::string_view key) {
    std::string value;
    leveldb::Status status =
        db_->Get(kReadOptions, {key.data(), key.size()}, &value);

    if (status.IsNotFound()) {
      return absl::NotFoundError("Key not found");
    }

    if (!status.ok()) {
      return absl::InternalError(status.ToString());
    }

    return value;
  }

  // Add single value for given key.
  absl::Status Add(std::string_view key, std::string_view value) {
    leveldb::Status status = db_->Put(kWriteOptions, {key.data(), key.size()},
                                      {value.data(), value.size()});

    if (!status.ok()) {
      return absl::InternalError(status.ToString());
    }

    return absl::OkStatus();
  }

  // Add batch of values. Input is a span of pairs of key and value.
  absl::Status AddBatch(
      std::span<std::pair<std::string_view, std::string_view>> batch) {
    leveldb::WriteBatch write_batch;

    for (const auto& [key, value] : batch) {
      write_batch.Put({key.data(), key.size()}, {value.data(), value.size()});
    }

    leveldb::Status status = db_->Write(kWriteOptions, &write_batch);

    if (!status.ok()) {
      return absl::InternalError(status.ToString());
    }

    return absl::OkStatus();
  }

 private:
  std::unique_ptr<leveldb::DB> db_;
};

// Get raw result for given key without key space transformation.
absl::StatusOr<std::string> LevelDBKeySpaceBase::GetFromDB(
    std::string_view key) {
  return impl_->Get(key);
}

// Get latest index value from database for current key space.
absl::StatusOr<std::string> LevelDBKeySpaceBase::GetLastIndexFromDB() {
  return impl_->Get(internal::ToDBKey(key_space_, kLastIndexKey));
}

// Get current hash value from database for current key space.
absl::StatusOr<Hash> LevelDBKeySpaceBase::GetHashFromDB() {
  auto result = impl_->Get(internal::ToDBKey(key_space_, kHashKey));
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

  return impl_->AddBatch(batch);
}

// Add hash value.
absl::Status LevelDBKeySpaceBase::AddHashIntoDB(const Hash& hash) {
  return impl_->Add(internal::ToDBKey(key_space_, kHashKey),
                    {reinterpret_cast<const char*>(&hash), sizeof(hash)});
}
}  // namespace internal

// LevelDBIndex constructor.
LevelDBIndex::LevelDBIndex(std::string_view path)
    : impl_(std::make_shared<internal::LevelDBIndexImpl>(path)) {}
}  // namespace carmen::backend::index
