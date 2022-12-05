#pragma once

#include <filesystem>
#include <memory>
#include <span>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/memory_usage.h"

namespace carmen::backend {
using LDBEntry = std::pair<std::span<const char>, std::span<const char>>;

// Forward declaration. See leveldb.cc for implementation.
class LevelDbImpl;

// LevelDb provides a simple interface to interact with leveldb.
class LevelDb {
 public:
  LevelDb(LevelDb&&) noexcept;
  ~LevelDb();

  // Open a LevelDb database at given path. If create_if_missing is true, then
  // create a new database if one does not exist.
  static absl::StatusOr<LevelDb> Open(const std::filesystem::path& path,
                                      bool create_if_missing = true);

  // Get value for given key.
  absl::StatusOr<std::string> Get(std::span<const char> key) const;

  // Add single value for given key.
  absl::Status Add(LDBEntry entry);

  // Add batch of values. Input is a span of pairs of key and value.
  absl::Status AddBatch(std::span<LDBEntry> batch);

  // Flush all pending writes to database.
  absl::Status Flush();

  // Close the database.
  absl::Status Close();

  // Check if database is open.
  bool IsOpen() const;

  // Summarizes the memory usage of this instance.
  MemoryFootprint GetMemoryFootprint() const;

 private:
  explicit LevelDb(std::unique_ptr<LevelDbImpl> db);

  // Pointer to implementation.
  std::unique_ptr<LevelDbImpl> impl_;
};
}  // namespace carmen::backend
