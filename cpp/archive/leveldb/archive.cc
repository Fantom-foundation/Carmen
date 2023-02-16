#include "archive/leveldb/archive.h"

#include <filesystem>
#include <memory>
#include <span>

#include "archive/leveldb/keys.h"
#include "backend/common/leveldb/leveldb.h"
#include "common/byte_util.h"
#include "common/status_util.h"

namespace carmen::archive::leveldb {

using ::carmen::backend::LDBEntry;
using ::carmen::backend::LevelDb;

namespace internal {

class Archive {
 public:
  static absl::StatusOr<std::unique_ptr<Archive>> Open(
      const std::filesystem::path directory) {
    ASSIGN_OR_RETURN(auto db, LevelDb::Open(directory));
    return std::unique_ptr<Archive>(new Archive(std::move(db)));
  }

  absl::Status Add(BlockId block, const Update& update) {
    // TODO: use a batch insert.

    for (const auto& [addr, balance] : update.GetBalances()) {
      RETURN_IF_ERROR(db_.Add({GetBalanceKey(addr, block), AsChars(balance)}));
    }

    return absl::OkStatus();
    // return db_.AddBatch(entries);
  }

  absl::StatusOr<Balance> GetBalance(BlockId block, const Address& address) {
    auto key = GetBalanceKey(address, block);
    ASSIGN_OR_RETURN(auto iter, db_.GetLowerBound(key));
    if (iter.IsEnd()) {
      RETURN_IF_ERROR(iter.Prev());
    } else {
      if (iter.Key().size() != key.size()) {
        return Balance{};
      }

      if (std::memcmp(iter.Key().data(), key.data(), key.size()) != 0) {
        RETURN_IF_ERROR(iter.Prev());
      }
    }

    if (!iter.Valid() || iter.Key().size() != key.size()) {
      return Balance{};
    }

    ASSIGN_OR_RETURN(auto view, PropertyKeyView::Parse(iter.Key()));
    if (view.GetBlockId() > block || view.GetAddress() != address) {
      return Balance{};
    }

    if (iter.Value().size() != sizeof(Balance)) {
      return absl::InternalError("stored balance has wrong format");
    }
    Balance result;
    result.SetBytes(std::as_bytes(iter.Value()));
    return result;
  }

  absl::Status Flush() { return db_.Flush(); }

  absl::Status Close() { return db_.Close(); }

 private:
  Archive(LevelDb db) : db_(std::move(db)) {}
  LevelDb db_;
};

}  // namespace internal

LevelDbArchive::LevelDbArchive(LevelDbArchive&&) = default;

LevelDbArchive::LevelDbArchive(std::unique_ptr<internal::Archive> archive)
    : impl_(std::move(archive)){};

LevelDbArchive& LevelDbArchive::operator=(LevelDbArchive&&) = default;

LevelDbArchive::~LevelDbArchive() { Close().IgnoreError(); };

absl::StatusOr<LevelDbArchive> LevelDbArchive::Open(
    std::filesystem::path directory) {
  // TODO: create directory if it does not exist.
  auto path = directory;
  if (std::filesystem::is_directory(directory)) {
    path = path / "archive.sqlite";
  }
  ASSIGN_OR_RETURN(auto impl, internal::Archive::Open(path));
  return LevelDbArchive(std::move(impl));
}

absl::Status LevelDbArchive::Add(BlockId block, const Update& update) {
  RETURN_IF_ERROR(CheckState());
  return impl_->Add(block, update);
}

absl::StatusOr<Balance> LevelDbArchive::GetBalance(BlockId block,
                                                   const Address& account) {
  RETURN_IF_ERROR(CheckState());
  return impl_->GetBalance(block, account);
}

absl::Status LevelDbArchive::Flush() {
  if (!impl_) return absl::OkStatus();
  return impl_->Flush();
}

absl::Status LevelDbArchive::Close() {
  if (!impl_) return absl::OkStatus();
  auto result = impl_->Close();
  impl_ = nullptr;
  return result;
}

absl::Status LevelDbArchive::CheckState() const {
  if (impl_) return absl::OkStatus();
  return absl::FailedPreconditionError("Archive not connected to DB.");
}

}  // namespace carmen::archive::leveldb
