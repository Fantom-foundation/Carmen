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

#include <sstream>
#include <string>

namespace carmen {

template <typename T>
std::string Print(const T& value) {
  std::stringstream out;
  out << value;
  return out.str();
}

}  // namespace carmen
