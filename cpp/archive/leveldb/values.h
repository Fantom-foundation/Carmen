/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use of
 * this software will be governed by the GNU Lesser General Public License v3.
 */

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
