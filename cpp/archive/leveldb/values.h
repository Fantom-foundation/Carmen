#pragma once

#include <array>
#include <span>

#include "absl/status/statusor.h"
#include "archive/leveldb/keys.h"
#include "common/type.h"

namespace carmen::archive::leveldb {

// An AccountState summarizes the meta information maintained per account in the
// archive. For an associated block height it describes whether the account
// existed and what its reincarnation number was.
struct AccountState {
  // Parses the given byte sequence and produces an account state.
  static absl::StatusOr<AccountState> From(std::span<const char>);

  // Encodes the state into a character sequence.
  std::array<char, 5> Encode() const;

  // True, if the account exists, false if it never existed or was deleted.
  bool exists = false;

  // The reincarnation counter for the account. The counter is 0 if the account
  // was never touched, and is incremented by 1 each time the account is created
  // or deleted.
  ReincarnationNumber reincarnation_number = 0;
};

}  // namespace carmen::archive::leveldb
