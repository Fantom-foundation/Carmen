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

#include <cstdint>
#include <ostream>

namespace carmen {

// An AccountState models the life-cycle of accounts.
// Note: the assigned values need to be kept in sync with the Go counterpart.
enum class AccountState : std::uint8_t {
  // An unknown or deleted account.
  kUnknown = 0,
  // An active account.
  kExists = 1,
};

std::ostream& operator<<(std::ostream&, AccountState);

}  // namespace carmen
