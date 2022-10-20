#include "backend/index/leveldb/common/ldb_instance.h"

#include <span>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "leveldb/db.h"
#include "leveldb/slice.h"
#include "leveldb/write_batch.h"

namespace carmen::backend::index::internal {
namespace {
constexpr leveldb::WriteOptions kWriteOptions = leveldb::WriteOptions();
constexpr leveldb::ReadOptions kReadOptions = leveldb::ReadOptions();
}  // namespace

// LevelDB index implementation. To encapsulate leveldb dependency.
class LevelDBImpl {
 public:
  static absl::StatusOr<LevelDBImpl> Open(std::string_view path,
                                          bool create_if_missing = true) {
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = create_if_missing;
    leveldb::Status status =
        leveldb::DB::Open(options, {path.data(), path.size()}, &db);

    if (!status.ok()) return absl::InternalError(status.ToString());

    return LevelDBImpl(db);
  }

  // Get value for given key.
  absl::StatusOr<std::string> Get(std::string_view key) const {
    std::string value;
    leveldb::Status status =
        db_->Get(kReadOptions, {key.data(), key.size()}, &value);

    if (status.IsNotFound()) return absl::NotFoundError("Key not found");

    if (!status.ok()) return absl::InternalError(status.ToString());

    return value;
  }

  // Add single value for given key.
  absl::Status Add(std::string_view key, std::string_view value) {
    leveldb::Status status = db_->Put(kWriteOptions, {key.data(), key.size()},
                                      {value.data(), value.size()});

    if (!status.ok()) return absl::InternalError(status.ToString());

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

    if (!status.ok()) return absl::InternalError(status.ToString());

    return absl::OkStatus();
  }

 private:
  explicit LevelDBImpl(leveldb::DB* db) : db_(db) {}

  std::unique_ptr<leveldb::DB> db_;
};

// Open leveldb database connection.
absl::StatusOr<LevelDBInstance> LevelDBInstance::Open(std::string_view path,
                                                      bool create_if_missing) {
  auto db = LevelDBImpl::Open(path, create_if_missing);
  if (!db.ok()) return db.status();
  return LevelDBInstance(std::make_unique<LevelDBImpl>(std::move(*db)));
}

// Get value for given key.
absl::StatusOr<std::string> LevelDBInstance::Get(std::string_view key) {
  return impl_->Get(key);
}

// Add single value for given key.
absl::Status LevelDBInstance::Add(std::string_view key,
                                  std::string_view value) {
  return impl_->Add(key, value);
}

// Add batch of values. Input is a span of pairs of key and value.
absl::Status LevelDBInstance::AddBatch(
    std::span<std::pair<std::string_view, std::string_view>> batch) {
  return impl_->AddBatch(batch);
}

LevelDBInstance::LevelDBInstance(std::unique_ptr<LevelDBImpl> db)
    : impl_(std::move(db)) {}

LevelDBInstance::LevelDBInstance(LevelDBInstance&&) noexcept = default;

LevelDBInstance::~LevelDBInstance() = default;
}  // namespace carmen::backend::index::internal