#include "backend/index/leveldb/common/level_db.h"

#include <filesystem>
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
  LevelDBImpl(LevelDBImpl&&) noexcept = default;
  ~LevelDBImpl() = default;

  static absl::StatusOr<LevelDBImpl> Open(const std::filesystem::path& path,
                                          bool create_if_missing = true) {
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = create_if_missing;
    leveldb::Status status = leveldb::DB::Open(options, path.string(), &db);

    if (!status.ok()) return absl::InternalError(status.ToString());

    return LevelDBImpl(db);
  }

  // Get value for given key.
  absl::StatusOr<std::string> Get(std::span<const char> key) const {
    std::string value;
    leveldb::Status status =
        db_->Get(kReadOptions, {key.data(), key.size()}, &value);

    if (status.IsNotFound()) return absl::NotFoundError("Key not found");

    if (!status.ok()) return absl::InternalError(status.ToString());

    return value;
  }

  // Add single value for given key.
  absl::Status Add(LDBEntry entry) {
    leveldb::Status status =
        db_->Put(kWriteOptions, {entry.first.data(), entry.first.size()},
                 {entry.second.data(), entry.second.size()});

    if (!status.ok()) return absl::InternalError(status.ToString());

    return absl::OkStatus();
  }

  // Add batch of values. Input is a span of pairs of key and value.
  absl::Status AddBatch(std::span<LDBEntry> batch) {
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
absl::StatusOr<LevelDB> LevelDB::Open(const std::filesystem::path& path,
                                      bool create_if_missing) {
  auto db = LevelDBImpl::Open(path, create_if_missing);
  if (!db.ok()) return db.status();
  return LevelDB(std::make_unique<LevelDBImpl>(std::move(*db)));
}

// Get value for given key.
absl::StatusOr<std::string> LevelDB::Get(std::span<const char> key) const {
  return impl_->Get(key);
}

// Add single value for given key.
absl::Status LevelDB::Add(LDBEntry entry) { return impl_->Add(entry); }

// Add batch of values. Input is a span of pairs of key and value.
absl::Status LevelDB::AddBatch(std::span<LDBEntry> batch) {
  return impl_->AddBatch(batch);
}

LevelDB::LevelDB(std::unique_ptr<LevelDBImpl> db) : impl_(std::move(db)) {}

LevelDB::LevelDB(LevelDB&&) noexcept = default;

LevelDB::~LevelDB() {}
}  // namespace carmen::backend::index::internal
