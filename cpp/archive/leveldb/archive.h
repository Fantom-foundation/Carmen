/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use of
 * this software will be governed by the GNU Lesser General Public Licence v3.
 */

#pragma once

#include <filesystem>
#include <memory>
#include <string_view>

#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/memory_usage.h"
#include "common/type.h"
#include "state/update.h"

namespace carmen::archive::leveldb {

namespace internal {
class Archive;
}

// A LevelDB key/value store based implementation of an Archive.
class LevelDbArchive {
 public:
  // Opens the archive located in the given directory. May fail if the directory
  // can not be accessed or the data format in the contained database does not
  // match requirements.
  static absl::StatusOr<LevelDbArchive> Open(std::filesystem::path directory);

  LevelDbArchive(LevelDbArchive&&);
  LevelDbArchive& operator=(LevelDbArchive&&);
  ~LevelDbArchive();

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

  // Obtains the last block included in this archive, 0 if empty.
  absl::StatusOr<BlockId> GetLatestBlock();

  // Computes a hash for the entire archive up until the given block.
  absl::StatusOr<Hash> GetHash(BlockId block);

  // Obtains a full list of addresses encountered up until the given block.
  absl::StatusOr<std::vector<Address>> GetAccountList(BlockId block);

  // Obtains a hash on the content of the given hash at the given block height.
  absl::StatusOr<Hash> GetAccountHash(BlockId block, const Address& account);

  // Verifies that the content of this archive up until the given block.
  absl::Status Verify(
      BlockId block, const Hash& expected_hash,
      absl::FunctionRef<void(std::string_view)> progress_callback =
          [](std::string_view) {});

  // Verifies the given account at the given block height.
  absl::Status VerifyAccount(BlockId block, const Address& account) const;

  // Flushes all temporary changes to disk.
  absl::Status Flush();

  // Closes the archive. This disconnects the archive from the underlying DB and
  // no further member function calls will be successful.
  absl::Status Close();

  // Summarizes the memory usage of this archive.
  MemoryFootprint GetMemoryFootprint() const;

 private:
  LevelDbArchive(std::unique_ptr<internal::Archive> archive);

  absl::Status CheckState() const;

  // The actual archive implementation is hidden using an opaque internal type.
  std::unique_ptr<internal::Archive> impl_;
};

}  // namespace carmen::archive::leveldb
