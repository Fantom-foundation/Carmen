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

  absl::StatusOr<LevelDbIterator> Begin() const {
    auto iter = db_->NewIterator(kReadOptions);
    iter->SeekToFirst();
    auto result = LevelDbIterator(std::unique_ptr<leveldb::Iterator>(iter));
    RETURN_IF_ERROR(result.Status());
    return result;
  }

  absl::StatusOr<LevelDbIterator> End() const {
    auto iter = db_->NewIterator(kReadOptions);
    iter->SeekToLast();
    auto result = LevelDbIterator(std::unique_ptr<leveldb::Iterator>(iter));
    RETURN_IF_ERROR(result.Status());
    RETURN_IF_ERROR(result.Next());
    return result;
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

  absl::StatusOr<LevelDbIterator> GetLowerBound(
      std::span<const char> key) const {
    auto iter = db_->NewIterator(kReadOptions);
    iter->Seek({key.data(), key.size()});
    auto result = LevelDbIterator(std::unique_ptr<leveldb::Iterator>(iter));
    RETURN_IF_ERROR(result.Status());
    return result;
  }

  // Add single value for given key.
  absl::Status Add(LDBEntry entry) {
    leveldb::Status status =
        db_->Put(kWriteOptions, {entry.first.data(), entry.first.size()},
                 {entry.second.data(), entry.second.size()});

    if (!status.ok()) return absl::InternalError(status.ToString());

    return absl::OkStatus();
  }

  absl::Status Add(LevelDbWriteBatch batch) {
    leveldb::Status status = db_->Write(kWriteOptions, batch.batch_.get());
    if (!status.ok()) return absl::InternalError(status.ToString());
    return absl::OkStatus();
  }

  // Add batch of values. Input is a span of pairs of key and value.
  absl::Status AddBatch(std::span<LDBEntry> batch) {
    LevelDbWriteBatch write_batch;
    for (const auto& [key, value] : batch) {
      write_batch.Put(key, value);
    }
    return Add(std::move(write_batch));
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

// Obtains an iterator pointing to the first element or End() if empty.
absl::StatusOr<LevelDbIterator> LevelDb::Begin() const {
  return impl_->Begin();
}

// Obtains an iterator pointing to the position after the last entry.
absl::StatusOr<LevelDbIterator> LevelDb::End() const { return impl_->End(); }

// Get value for given key.
absl::StatusOr<std::string> LevelDb::Get(std::span<const char> key) const {
  return impl_->Get(key);
}

// Returns an iterator pointing to the first element in the DB with a key
// greater or equal to the given key.
absl::StatusOr<LevelDbIterator> LevelDb::GetLowerBound(
    std::span<const char> key) const {
  return impl_->GetLowerBound(key);
}

// Add single value for given key.
absl::Status LevelDb::Add(LDBEntry entry) { return impl_->Add(entry); }

absl::Status LevelDb::Add(LevelDbWriteBatch batch) {
  return impl_->Add(std::move(batch));
}

// Add batch of values. Input is a span of pairs of key and value.
absl::Status LevelDb::AddBatch(std::span<LDBEntry> batch) {
  return impl_->AddBatch(batch);
}

absl::Status LevelDb::Flush() {
  // No-op for LevelDB.
  return absl::OkStatus();
}

absl::Status LevelDb::Close() {
  impl_.reset();
  return absl::OkStatus();
}

bool LevelDb::IsOpen() const { return impl_ != nullptr; }

LevelDb::LevelDb(std::unique_ptr<LevelDbImpl> db) : impl_(std::move(db)) {}

LevelDb::LevelDb(LevelDb&&) noexcept = default;

LevelDb::~LevelDb() = default;

MemoryFootprint LevelDb::GetMemoryFootprint() const {
  return impl_->GetMemoryFootprint();
}

LevelDbIterator::LevelDbIterator(LevelDbIterator&&) = default;
LevelDbIterator::~LevelDbIterator() = default;

LevelDbIterator::LevelDbIterator(std::unique_ptr<leveldb::Iterator> iterator)
    : state_(iterator->Valid() ? kValid : kEnd),
      iterator_(std::move(iterator)) {}

bool LevelDbIterator::IsBegin() const {
  return state_ == kBegin && Status().ok();
}

bool LevelDbIterator::IsEnd() const { return state_ == kEnd && Status().ok(); }

bool LevelDbIterator::Valid() const {
  return state_ == kValid && Status().ok();
}

absl::Status LevelDbIterator::Next() {
  if (state_ == kValid) {
    iterator_->Next();
  } else if (state_ == kBegin) {
    iterator_->SeekToFirst();
    state_ = kValid;
  } else if (state_ == kEnd) {
    // nothing
  }
  if (!iterator_->Valid()) {
    state_ = kEnd;
  }
  return Status();
}

absl::Status LevelDbIterator::Prev() {
  if (state_ == kValid) {
    iterator_->Prev();
  } else if (state_ == kBegin) {
    // nothing
  } else if (state_ == kEnd) {
    iterator_->SeekToLast();
    state_ = kValid;
  }
  if (!iterator_->Valid()) {
    state_ = kBegin;
  }
  return Status();
}

std::span<const char> LevelDbIterator::Key() const {
  auto slice = iterator_->key();
  return {slice.data(), slice.size()};
}

std::span<const char> LevelDbIterator::Value() const {
  auto slice = iterator_->value();
  return {slice.data(), slice.size()};
}

absl::Status LevelDbIterator::Status() const {
  auto status = iterator_->status();
  if (status.ok()) {
    return absl::OkStatus();
  }
  return absl::InternalError(status.ToString());
}

LevelDbWriteBatch::LevelDbWriteBatch()
    : batch_(std::make_unique<leveldb::WriteBatch>()) {}

LevelDbWriteBatch::LevelDbWriteBatch(LevelDbWriteBatch&&) = default;
LevelDbWriteBatch::~LevelDbWriteBatch() = default;

void LevelDbWriteBatch::Put(std::span<const char> key,
                            std::span<const char> value) {
  batch_->Put({key.data(), key.size()}, {value.data(), value.size()});
}

}  // namespace carmen::backend
