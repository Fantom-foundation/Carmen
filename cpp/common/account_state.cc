/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3.
 */

#include "common/account_state.h"

#include <ostream>

namespace carmen {

std::ostream& operator<<(std::ostream& out, AccountState s) {
  switch (s) {
    case AccountState::kUnknown:
      return out << "unknown";
    case AccountState::kExists:
      return out << "exists";
  }
  return out << "invalid";
}

}  // namespace carmen
