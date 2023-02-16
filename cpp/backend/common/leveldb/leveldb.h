#pragma once

#include <filesystem>
#include <memory>
#include <span>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/memory_usage.h"

// Forward declaration of opaque LevelDB dependencies.
namespace leveldb {
class Iterator;
}

namespace carmen::backend {
using LDBEntry = std::pair<std::span<const char>, std::span<const char>>;

// Forward declaration. See leveldb.cc for implementation.
class LevelDbImpl;
class LevelDbIterator;

// LevelDb provides a simple interface to interact with leveldb.
class LevelDb {
 public:
  LevelDb(LevelDb&&) noexcept;
  ~LevelDb();

  // Open a LevelDb database at given path. If create_if_missing is true, then
  // create a new database if one does not exist.
  static absl::StatusOr<LevelDb> Open(const std::filesystem::path& path,
                                      bool create_if_missing = true);

  // Obtains an iterator pointing to the first element or End() if empty.
  absl::StatusOr<LevelDbIterator> Begin() const;

  // Obtains an iterator pointing to the position after the last entry.
  absl::StatusOr<LevelDbIterator> End() const;

  // Get value for given key.
  absl::StatusOr<std::string> Get(std::span<const char> key) const;

  // Returns an iterator pointing to the first element in the DB with a key
  // greater or equal to the given key.
  absl::StatusOr<LevelDbIterator> GetLowerBound(
      std::span<const char> key) const;

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

// A LevelIterator allows to iterate through the key space of a LevelDB store.
class LevelDbIterator {
 public:
  LevelDbIterator(std::unique_ptr<leveldb::Iterator> iterator);
  LevelDbIterator(LevelDbIterator&&);
  ~LevelDbIterator();

  // True, if the iterator points at an invalid element before the first
  // element, false otherwise. This may be used to test for the end of an
  // iteration when iterating in reverse order.
  bool IsBegin() const;

  // True, if the iterator points at an invalid element after the last
  // element, false otherwise. This may be used to test for the end of an
  // iteration when iterating in order.
  bool IsEnd() const;

  // True, if the iterator points to a valid key/value pair, false otherwise. In
  // particular, the iterator is invalid if IsBegin() or IsEnd() is true. An
  // iterator is also invalidated by errors (see Status()).
  bool Valid() const;

  // Moves the iterator to the next key/value pair in the store. May invalidate
  // the iterator if the iterator was positioned at the last entry in the store.
  absl::Status Next();

  // Moves the iterator to the previous key/value pair. May invalidate the
  // iterator if the iterator was positioned at the first entry in the store.
  absl::Status Prev();

  // Returns a view on the key the iterator is currently pointing to. The
  // underlying storage is only valid until the next modification of the
  // iterator.
  std::span<const char> Key() const;

  // Returns a view on the value the iterator is currently pointing to. The
  // underlying storage is only valid until the next modification of the
  // iterator.
  std::span<const char> Value() const;

  // Returns the last encountered issue, OK if none has occured.
  absl::Status Status() const;

 private:
  enum State { kBegin, kValid, kEnd };
  State state_;
  std::unique_ptr<leveldb::Iterator> iterator_;
};

}  // namespace carmen::backend
