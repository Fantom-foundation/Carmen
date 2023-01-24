#pragma once

#include <cstdint>
#include <filesystem>

#include "absl/container/btree_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/type.h"

namespace carmen {

// A type alias for block numbers.
using BlockId = std::uint32_t;

class BlockUpdate;

namespace internal {
class Archive;
}

// An archive retains a history of state mutations in a block chain on a
// block-level granularity. The history is recorded by adding per-block updates.
// All updates are append-only. History written once can no longer be altered.
//
// Archive Add(..) and GetXXX(..) operations are thread safe and may thus be run
// in parallel.
class Archive {
 public:
  // Opens the archive located in the given directory. May fail if the directory
  // can not be accessed or the data format in the contained database does not
  // match requirements.
  static absl::StatusOr<Archive> Open(std::filesystem::path directory);

  Archive(Archive&&);
  ~Archive();
  Archive& operator=(Archive&&);

  // Adds the changes of the given block to this archive.
  absl::Status Add(BlockId block, const BlockUpdate& update);

  // Allows to fetch a historic value for a given slot.
  absl::StatusOr<Value> GetStorage(BlockId block, const Address& account,
                                   const Key& key);

  // Flushes all temporary changes to disk.
  absl::Status Flush();

  // Closes the archive. This disconnects the archive from the underlying DB and
  // no further member function calls will be successful.
  absl::Status Close();

 private:
  Archive(std::unique_ptr<internal::Archive> archive);

  absl::Status CheckState() const;

  // The actual archive implementation is hidden using an opaque internal type.
  std::unique_ptr<internal::Archive> impl_;
};

// A BlockUpdate summarizes all the updates produced by processing a block in
// the chain. It is the unit of data used to update archives and to synchronize
// data between archive instances.
// TODO:
//  - implement balance update support
//  - implement nonce update support
//  - implement account state update support
//  - implement cryptographic hashing of updates
//  - implement serialization and de-serialization of updates
class BlockUpdate {
 public:
  // The identifier used for slots.
  struct SlotKey {
    Address account;
    Key slot;
    auto operator<=>(const SlotKey&) const = default;
  };

  // Adds the update of a storage slot to the changes to be covered by this
  // update.
  void Set(const Address& account, const Key& key, const Value& value);

  // Provides read access to the sorted map of storage updates maintained.
  const absl::btree_map<SlotKey, Value>& GetStorage() const {
    return storage_;
  };

 private:
  // Retains storage updates in sorted order. By sorting them, a normal form for
  // updates is defined, aiding the verification of updates.
  absl::btree_map<SlotKey, Value> storage_;
};

}  // namespace carmen
