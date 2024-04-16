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

#include "state/schema.h"

#include <ostream>
#include <string_view>

namespace carmen {

std::ostream& operator<<(std::ostream& out, const Schema& s) {
  static const StateFeature features[] = {
      StateFeature::kAddressId,
      StateFeature::kKeyId,
      StateFeature::kAccountReincarnation,
  };
  static const std::string_view names[] = {
      "address_id",
      "key_id",
      "account_reincarnation",
  };

  out << '{';
  bool first = true;
  for (std::size_t i = 0; i < sizeof(features); i++) {
    if (!s.HashFeature(features[i])) continue;
    if (!first) {
      out << ',';
    }
    first = false;
    out << names[i];
  }
  return out << '}';
}

}  // namespace carmen
