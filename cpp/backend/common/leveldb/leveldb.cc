#include "backend/common/leveldb/leveldb.h"

#include <filesystem>
#include <span>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/memory_usage.h"
#include "common/status_util.h"
#include "leveldb/db.h"
#include "leveldb/slice.h"
#include "leveldb/write_batch.h"

namespace carmen::backend {
namespace {
constexpr leveldb::WriteOptions kWriteOptions = leveldb::WriteOptions();
constexpr leveldb::ReadOptions kReadOptions = leveldb::ReadOptions();
}  // namespace

// LevelDb index implementation. To encapsulate leveldb dependency.
class LevelDbImpl {
 public:
  LevelDbImpl(LevelDbImpl&&) noexcept = default;
  ~LevelDbImpl() = default;

  static absl::StatusOr<LevelDbImpl> Open(const std::filesystem::path& path,
                                          bool create_if_missing = true) {
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = create_if_missing;
    leveldb::Status status = leveldb::DB::Open(options, path.string(), &db);

    if (!status.ok()) return absl::InternalError(status.ToString());

    return LevelDbImpl(db);
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

  // Summarizes the memory usage of this instance.
  MemoryFootprint GetMemoryFootprint() const {
    MemoryFootprint res(*this);
    std::string usage;
    db_->GetProperty("leveldb.approximate-memory-usage", &usage);
    res.Add("db", Memory(std::stoll(usage)));
    return res;
  }

 private:
  explicit LevelDbImpl(leveldb::DB* db) : db_(db) {}

  std::unique_ptr<leveldb::DB> db_;
};

// Open leveldb database connection.
absl::StatusOr<LevelDb> LevelDb::Open(const std::filesystem::path& path,
                                      bool create_if_missing) {
  ASSIGN_OR_RETURN(auto db, LevelDbImpl::Open(path, create_if_missing));
  return LevelDb(std::make_unique<LevelDbImpl>(std::move(db)));
}

// Get value for given key.
absl::StatusOr<std::string> LevelDb::Get(std::span<const char> key) const {
  return impl_->Get(key);
}

// Add single value for given key.
absl::Status LevelDb::Add(LDBEntry entry) { return impl_->Add(entry); }

// Add batch of values. Input is a span of pairs of key and value.
absl::Status LevelDb::AddBatch(std::span<LDBEntry> batch) {
  return impl_->AddBatch(batch);
}

absl::Status LevelDb::Flush() {
  // No-op for LevelDB.
  return absl::OkStatus();
}

void LevelDb::Close() { impl_.reset(); }

bool LevelDb::IsOpen() const { return impl_ != nullptr; }

LevelDb::LevelDb(std::unique_ptr<LevelDbImpl> db) : impl_(std::move(db)) {}

LevelDb::LevelDb(LevelDb&&) noexcept = default;

LevelDb::~LevelDb() = default;

MemoryFootprint LevelDb::GetMemoryFootprint() const {
  return impl_->GetMemoryFootprint();
}

}  // namespace carmen::backend
