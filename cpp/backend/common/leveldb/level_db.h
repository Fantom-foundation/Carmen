#pragma once

#include <filesystem>
#include <memory>
#include <span>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace carmen::backend {
using LDBEntry = std::pair<std::span<const char>, std::span<const char>>;

// Forward declaration. See level_db.cc for implementation.
class LevelDBImpl;

// LevelDB provides a simple interface to interact with leveldb.
class LevelDB {
 public:
  LevelDB(LevelDB&&) noexcept;
  ~LevelDB();

  // Open a LevelDB database at given path. If create_if_missing is true, then
  // create a new database if one does not exist.
  static absl::StatusOr<LevelDB> Open(const std::filesystem::path& path,
                                      bool create_if_missing = true);

  // Get value for given key.
  absl::StatusOr<std::string> Get(std::span<const char> key) const;

  // Add single value for given key.
  absl::Status Add(LDBEntry entry);

  // Add batch of values. Input is a span of pairs of key and value.
  absl::Status AddBatch(std::span<LDBEntry> batch);

 private:
  explicit LevelDB(std::unique_ptr<LevelDBImpl> db);

  // Pointer to implementation.
  std::unique_ptr<LevelDBImpl> impl_;
};
}  // namespace carmen::backend
