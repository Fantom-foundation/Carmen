/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3 
 */


#include "archive/leveldb/values.h"

#include <array>
#include <span>

#include "archive/leveldb/encoding.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen::archive::leveldb {

absl::StatusOr<AccountState> AccountState::From(std::span<const char> data) {
  if (data.size() != 5) {
    return absl::InvalidArgumentError("Invalid encoding of AccountState");
  }
  AccountState res;
  res.exists = (std::uint8_t(data[0]) != 0);
  res.reincarnation_number =
      ReadUint32(std::span<const char, 4>(data.data() + 1, 4));
  return res;
}

std::array<char, 1 + 4> AccountState::Encode() const {
  std::array<char, 5> res;
  res[0] = exists ? 1 : 0;
  Write(reincarnation_number, std::span<char, 4>(res.data() + 1, 4));
  return res;
}

}  // namespace carmen::archive::leveldb
