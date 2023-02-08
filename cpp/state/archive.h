#pragma once

#include <cstdint>
#include <filesystem>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/memory_usage.h"
#include "common/type.h"
#include "state/update.h"

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
  absl::Status Add(BlockId block, const Update& update);

  // Allows to test whether an account exists at the given block height.
  absl::StatusOr<bool> Exists(BlockId block, const Address& account);

  // Allows to fetch a historic balance values for a given account.
  absl::StatusOr<Balance> GetBalance(BlockId block, const Address& account);

  // Allows to fetch a historic code values for a given account.
  absl::StatusOr<Code> GetCode(BlockId block, const Address& account);

  // Allows to fetch a historic nonce values for a given account.
  absl::StatusOr<Nonce> GetNonce(BlockId block, const Address& account);

  // Allows to fetch a historic value for a given slot.
  absl::StatusOr<Value> GetStorage(BlockId block, const Address& account,
                                   const Key& key);

  // Obtains a hash on the content of the given hash at the given block height.
  absl::StatusOr<Hash> GetAccountHash(BlockId block, const Address& account);

  // Flushes all temporary changes to disk.
  absl::Status Flush();

  // Closes the archive. This disconnects the archive from the underlying DB and
  // no further member function calls will be successful.
  absl::Status Close();

  // Summarizes the memory usage of this archive.
  MemoryFootprint GetMemoryFootprint() const;

 private:
  Archive(std::unique_ptr<internal::Archive> archive);

  absl::Status CheckState() const;

  // The actual archive implementation is hidden using an opaque internal type.
  std::unique_ptr<internal::Archive> impl_;
};

}  // namespace carmen
