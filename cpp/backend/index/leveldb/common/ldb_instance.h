#pragma once

#include <memory>
#include <span>
#include <string_view>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace carmen::backend::index::internal {
// Forward declaration. See ldb_instance.cc for implementation.
class LevelDBImpl;

// LevelDBInstance provides a simple interface to interact with leveldb.
class LevelDBInstance {
 public:
  LevelDBInstance(LevelDBInstance&&) noexcept;
  ~LevelDBInstance();

  // Open a LevelDB database at given path. If create_if_missing is true, then
  // create a new database if one does not exist.
  static absl::StatusOr<LevelDBInstance> Open(std::string_view path,
                                              bool create_if_missing = true);

  // Get value for given key.
  absl::StatusOr<std::string> Get(std::string_view key);

  // Add single value for given key.
  absl::Status Add(std::string_view key, std::string_view value);

  // Add batch of values. Input is a span of pairs of key and value.
  absl::Status AddBatch(
      std::span<std::pair<std::string_view, std::string_view>> batch);

 private:
  explicit LevelDBInstance(std::unique_ptr<LevelDBImpl> db);

  // Pointer to implementation.
  std::unique_ptr<LevelDBImpl> impl_;
};

}  // namespace carmen::backend::index::internal
